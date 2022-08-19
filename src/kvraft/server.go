package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// ClerkID and PropID are used to identify a propose
	ClerkID int
	PropID  int

	Type  string
	Key   string
	Value string
}

const (
	OpGet    = "GET"
	OpPut    = "PUT"
	OpAppend = "APPEND"
)

type OpRequest struct {
	Op
	RespCh chan Err
	Value  string
}

type ApplyResult struct {
	ClerkID int
	PropID  int
	Index   int
	// Value is set if it is a get op
	Value string
	Error Err
}

// ApplyRecord records the applied token for one clerk
type ApplyRecord map[int]struct{}
type ApplyRecords map[int]ApplyRecord

func (ar ApplyRecords) Add(clerkID, propID int) {
	if _, exist := ar[clerkID]; !exist {
		ar[clerkID] = make(ApplyRecord)
	}
	ar[clerkID][propID] = struct{}{}
}

func (ar ApplyRecords) Exist(clerkID, propID int) bool {
	if record, ok := ar[clerkID]; ok {
		_, ok := record[propID]
		return ok
	}
	return false
}

const rpcReqQueueSize = 1000

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// the state machine
	database     map[string]string
	applyRecords ApplyRecords

	rpcReqQueue chan *OpRequest

	cond            *sync.Cond
	applyResultsBuf []*ApplyResult
	applyResults    chan *ApplyResult
}

func encodeOp(op Op) []byte {
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(op.ClerkID)
	enc.Encode(op.PropID)
	enc.Encode(op.Type)
	enc.Encode(op.Key)
	enc.Encode(op.Value)
	return w.Bytes()
}

func decodeOp(data []byte) (Op, error) {
	var op Op
	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)
	if err := dec.Decode(&op.ClerkID); err != nil {
		return Op{}, err
	}
	if err := dec.Decode(&op.PropID); err != nil {
		return Op{}, err
	}
	if err := dec.Decode(&op.Type); err != nil {
		return Op{}, err
	}
	if err := dec.Decode(&op.Key); err != nil {
		return Op{}, err
	}
	if err := dec.Decode(&op.Value); err != nil {
		return Op{}, err
	}
	return op, nil
}

func (kv *KVServer) starter() {
nextreq:
	for req := range kv.rpcReqQueue {
		// consume remain apply results
		//for {
		//	select {
		//	case <-kv.applyResults:
		//	default:
		//		goto Work
		//	}
		//}

		//	Work:
		DPrintf("[server %d] starter process req %v", kv.me, req.Op)
		data := encodeOp(req.Op)
		index, _, isLeader := kv.rf.Start(data)
		DPrintf("[server %d] starter start req %v index %d isLeader %v", kv.me, req, index, isLeader)
		for isLeader {
			select {
			case applyResult := <-kv.applyResults:
				if applyResult.Index > index {
					req.RespCh <- ErrWrongLeader
					goto nextreq
				}
				if applyResult.Index == index {
					if applyResult.ClerkID != req.ClerkID || applyResult.PropID != req.PropID {
						req.RespCh <- ErrWrongLeader
					} else {
						if req.Op.Type == OpGet {
							if applyResult.Error == ErrNoKey {
								req.RespCh <- ErrNoKey
							} else {
								req.Value = applyResult.Value
								req.RespCh <- OK
							}
						} else {
							req.RespCh <- OK
						}
					}
					goto nextreq
				}
			default:
				_, isLeader = kv.rf.GetState()
			}
		}
		// iam not leader
		req.RespCh <- ErrWrongLeader
	}
}

func (kv *KVServer) addApplyResultsToBuffer(res *ApplyResult) {
	kv.cond.L.Lock()
	kv.applyResultsBuf = append(kv.applyResultsBuf, res)
	kv.cond.Signal()
	kv.cond.L.Unlock()
}

func (kv *KVServer) applyResultsBufConsumer() {
	for {
		var results []*ApplyResult

		kv.cond.L.Lock()
		for len(kv.applyResultsBuf) == 0 {
			kv.cond.Wait()
		}
		results = kv.applyResultsBuf
		kv.applyResultsBuf = []*ApplyResult{}
		kv.cond.L.Unlock()

		if results != nil {
			for _, res := range results {
				kv.applyResults <- res
			}
		}
	}
}

func (kv *KVServer) applier() {
	for applyMsg := range kv.applyCh {
		data, ok := applyMsg.Command.([]byte)
		if !ok {
			continue
		}
		op, err := decodeOp(data)
		if err != nil {
			continue
		}
		res := &ApplyResult{
			ClerkID: op.ClerkID,
			PropID:  op.PropID,
			Index:   applyMsg.CommandIndex,
		}
		if kv.applyRecords.Exist(op.ClerkID, op.PropID) == false {
			switch op.Type {
			case OpGet:
				if value, ok := kv.database[op.Key]; ok {
					res.Value = value
					res.Error = OK
				} else {
					res.Error = ErrNoKey
				}
				// we don't add OpGet to apply records
			case OpPut:
				kv.database[op.Key] = op.Value
				kv.applyRecords.Add(op.ClerkID, op.PropID)
			case OpAppend:
				if ori, found := kv.database[op.Key]; found {
					kv.database[op.Key] = ori + op.Value
				} else {
					kv.database[op.Key] = op.Value
				}
				kv.applyRecords.Add(op.ClerkID, op.PropID)
			}
		}
		kv.addApplyResultsToBuffer(res)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[server %d] receive get %v", kv.me, args)
	op := &OpRequest{
		Op: Op{
			Type: OpGet,
			Key:  args.Key,
		},
		RespCh: make(chan Err),
	}
	kv.rpcReqQueue <- op
	for kv.killed() == false {
		select {
		case reply.Err = <-op.RespCh:
			reply.Value = op.Value
			return
		default:
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("[server %d] receive put append %v", kv.me, args)
	op := &OpRequest{
		Op: Op{
			ClerkID: args.ClerkID,
			PropID:  args.PropID,
			Type:    args.Op,
			Key:     args.Key,
			Value:   args.Value,
		},
		RespCh: make(chan Err),
	}
	kv.rpcReqQueue <- op
	for kv.killed() == false {
		select {
		case reply.Err = <-op.RespCh:
			return
		default:
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.applyRecords = make(ApplyRecords)

	kv.rpcReqQueue = make(chan *OpRequest, 1000)
	kv.applyResults = make(chan *ApplyResult, 1000)

	kv.cond = sync.NewCond(&kv.mu)

	go kv.starter()
	go kv.applier()
	go kv.applyResultsBufConsumer()

	return kv
}
