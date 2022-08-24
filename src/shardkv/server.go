package shardkv

import (
	"6.824/labrpc"
	"bytes"
	"log"
	"math"
	"sync/atomic"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

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

type OpRequest struct {
	Op
	RespCh chan Err
	Value  string
}

type Bitmap struct {
	// invariant: Start % 8 == 0
	Start   int
	Content []uint8
}

func NewBitmap() *Bitmap {
	return &Bitmap{
		Start:   0,
		Content: []uint8{},
	}
}

func (b *Bitmap) Set(i int) {
	if i < b.Start {
		return
	}
	i = i - b.Start
	blk, off := i/8, i%8
	for i := blk - len(b.Content); i >= 0; i-- {
		b.Content = append(b.Content, 0)
	}
	b.Content[blk] |= 1 << off
}

func (b *Bitmap) IsSet(i int) bool {
	if i < b.Start {
		return true
	}
	i = i - b.Start
	blk, off := i/8, i%8
	if blk >= len(b.Content) {
		return false
	}
	return (b.Content[blk] & (1 << off)) > 0
}

const uint8Max = uint8(math.MaxUint8)

// Shrink will find continuous set bits from beginning, remove them and
// update the Start index.
func (b *Bitmap) Shrink() {
	toShrink := 0
	for i := range b.Content {
		if b.Content[i]&uint8Max != uint8Max {
			break
		}
		b.Start += 8
		toShrink++
	}
	if toShrink == len(b.Content) {
		b.Content = []uint8{}
	} else {
		b.Content = b.Content[toShrink:]
	}
	// fmt.Printf("after shrink %d\n", b.Start)
}

type ApplyRecord struct {
	Clerk   int
	Applied *Bitmap
}

type ApplyHistory struct {
	Records []ApplyRecord
}

func (ar *ApplyHistory) Add(clerkID, propID int) {
	var record *ApplyRecord
	for i := range ar.Records {
		if ar.Records[i].Clerk == clerkID {
			record = &ar.Records[i]
			break
		}
	}
	if record == nil {
		ar.Records = append(ar.Records, ApplyRecord{
			Clerk:   clerkID,
			Applied: NewBitmap(),
		})
		record = &ar.Records[len(ar.Records)-1]
	}
	record.Applied.Set(propID)
}

func (ar *ApplyHistory) Exist(clerkID, propID int) bool {
	var record *ApplyRecord
	for i := range ar.Records {
		if ar.Records[i].Clerk == clerkID {
			record = &ar.Records[i]
			break
		}
	}
	if record == nil {
		return false
	}
	return record.Applied.IsSet(propID)
}

type State struct {
	Database map[string]string
	History  ApplyHistory
}

func encodeState(state State) []byte {
	for i := range state.History.Records {
		state.History.Records[i].Applied.Shrink()
	}
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(state.Database)
	enc.Encode(state.History)
	return w.Bytes()
}

func decodeState(data []byte) State {
	var state State
	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)
	if dec.Decode(&state.Database) != nil ||
		dec.Decode(&state.History) != nil {
		log.Fatalf("decodeState error")
	}
	return state
}

type ApplyResult struct {
	ClerkID int
	PropID  int
	Index   int
	// Value is set if it is a get op
	Value string
	Error Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	dead         int32
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister

	// the state machine
	state State

	rpcReqQueue chan *OpRequest

	cond            *sync.Cond
	applyResultsBuf []*ApplyResult
	applyResults    chan *ApplyResult
}

func (kv *ShardKV) starter() {
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

func (kv *ShardKV) addApplyResultsToBuffer(res *ApplyResult) {
	kv.cond.L.Lock()
	kv.applyResultsBuf = append(kv.applyResultsBuf, res)
	kv.cond.Signal()
	kv.cond.L.Unlock()
}

func (kv *ShardKV) applyResultsBufConsumer() {
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

func (kv *ShardKV) applier() {
	for applyMsg := range kv.applyCh {
		// DPrintf("[server %d] applier %v", kv.me, applyMsg)
		if applyMsg.SnapshotValid {
			kv.state = decodeState(applyMsg.Snapshot)
			kv.addApplyResultsToBuffer(&ApplyResult{
				ClerkID: -1,
				PropID:  -1,
				Index:   applyMsg.SnapshotIndex,
			})
			continue
		}
		// command valid
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
		if kv.state.History.Exist(op.ClerkID, op.PropID) == false {
			switch op.Type {
			case OpGet:
				if value, ok := kv.state.Database[op.Key]; ok {
					res.Value = value
					res.Error = OK
				} else {
					res.Error = ErrNoKey
				}
				// we don't add OpGet to apply Records
			case OpPut:
				kv.state.Database[op.Key] = op.Value
				kv.state.History.Add(op.ClerkID, op.PropID)
			case OpAppend:
				if ori, found := kv.state.Database[op.Key]; found {
					kv.state.Database[op.Key] = ori + op.Value
				} else {
					kv.state.Database[op.Key] = op.Value
				}
				kv.state.History.Add(op.ClerkID, op.PropID)
			}
		}
		kv.addApplyResultsToBuffer(res)
		if kv.maxraftstate != -1 {
			if kv.persister.RaftStateSize() > kv.maxraftstate/2 {
				data := encodeState(kv.state)
				kv.rf.Snapshot(applyMsg.CommandIndex, data)
			}
		}
	}
}
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.state.Database = make(map[string]string)
	kv.state.History = ApplyHistory{Records: []ApplyRecord{}}

	kv.rpcReqQueue = make(chan *OpRequest, 1000)
	kv.applyResults = make(chan *ApplyResult, 1000)

	kv.cond = sync.NewCond(&kv.mu)

	go kv.starter()
	go kv.applier()
	go kv.applyResultsBufConsumer()

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	// kv.applyCh = make(chan raft.ApplyMsg)
	// kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
