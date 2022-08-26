package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

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

	// OpGetShard and OpPutShard
	Shard int
	Data  map[string]string

	// OpPutShard
	Num int

	// OpConfig
	Config shardctrler.Config
}

const (
	OpGet        = "GET"
	OpPut        = "PUT"
	OpAppend     = "APPEND"
	OpGetShard   = "GETSHARD"
	OpPutShard   = "PUTSHARD"
	OpCloseShard = "CLOSESHARD"
	OpConfig     = "CONFIG"
)

const KeyConfig = "__config__"
const KeyConfigNum = "__config_num__"
const KeyOutgoingShards = "__outgoing_shards__"

func encodeOp(op Op) []byte {
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(op.ClerkID)
	enc.Encode(op.PropID)
	enc.Encode(op.Type)
	enc.Encode(op.Key)
	enc.Encode(op.Value)
	enc.Encode(op.Shard)
	enc.Encode(op.Data)
	enc.Encode(op.Num)
	enc.Encode(op.Config)
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
	if err := dec.Decode(&op.Shard); err != nil {
		return Op{}, err
	}
	if err := dec.Decode(&op.Data); err != nil {
		return Op{}, err
	}
	if err := dec.Decode(&op.Num); err != nil {
		return Op{}, err
	}
	if err := dec.Decode(&op.Config); err != nil {
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

type Shard struct {
	Status int
	Data   map[string]string
}

func (s *Shard) String() string {
	return strconv.Itoa(s.Status) + " " + fmt.Sprintf("%s", s.Data)
}

const (
	Incoming = iota
	Outgoing
	Working
)

type ConfigStatus struct {
	Config              shardctrler.Config
	Rebalancing         bool
	PendingOutgoing     map[int]int // outgoing shard --> target gid
	PendingOutgoingData map[int]map[string]string
}

func encodeConfigStatus(status ConfigStatus) string {
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(status)
	return w.String()
}

func decodeConfigStatus(status string) (ret ConfigStatus) {
	r := bytes.NewReader([]byte(status))
	dec := labgob.NewDecoder(r)
	dec.Decode(&ret)
	return
}

type State struct {
	Shards          map[int]*Shard
	Config          shardctrler.Config
	Rebalancing     bool
	PendingOutgoing map[int]int // outgoing shard --> target gid
	History         ApplyHistory
}

func NewState() State {
	return State{
		Shards: map[int]*Shard{},
		Config: shardctrler.Config{
			Num:    0,
			Shards: [10]int{},
			Groups: make(map[int][]string),
		},
		Rebalancing:     false,
		PendingOutgoing: map[int]int{},
		History:         ApplyHistory{Records: make([]ApplyRecord, 0)},
	}
}

func encodeConfig(config shardctrler.Config) string {
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(config)
	return w.String()
}

func decodeConfig(config string) (ret shardctrler.Config) {
	r := bytes.NewReader([]byte(config))
	dec := labgob.NewDecoder(r)
	dec.Decode(&ret)
	return
}

func encodePendingOutgoing(outgoing map[int]int) string {
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(outgoing)
	return w.String()
}

func decodePendingOutgoing(outgoing string) (ret map[int]int) {
	r := bytes.NewReader([]byte(outgoing))
	dec := labgob.NewDecoder(r)
	dec.Decode(&ret)
	return
}

func (s *State) Add(key, value string) Err {
	shardNum := key2shard(key)
	shard, exist := s.Shards[shardNum]
	if exist == false || shard.Status != Working {
		return ErrWrongGroup
	}
	shard.Data[key] = value
	return OK
}

func (s *State) AddShard(shardNum int, data map[string]string) {
	shard, exist := s.Shards[shardNum]
	if !exist || shard.Status != Incoming {
		panic("AddShard: shard does not exist or is not incoming")
	}
	shard.Data = data
}

func (s *State) Get(key string) (string, Err) {
	shardNum := key2shard(key)
	if shard, exist := s.Shards[shardNum]; exist && shard.Status == Working {
		if v, ok := shard.Data[key]; ok {
			return v, OK
		}
		return "", ErrNoKey
	}
	return "", ErrWrongGroup
}

func (s *State) maybeFinishRebalancing() {
	allShardsWorking := true
	for _, shard := range s.Shards {
		if shard.Status != Working {
			allShardsWorking = false
			break
		}
	}
	if allShardsWorking {
		s.Rebalancing = false
	}
}

func (s *State) Apply(op Op, index int, gid int) ApplyResult {
	res := ApplyResult{
		ClerkID: op.ClerkID,
		PropID:  op.PropID,
		Index:   index,
		Error:   OK,
	}

	if s.History.Exist(op.ClerkID, op.PropID) == false {
		switch op.Type {
		case OpGet:
			switch op.Key {
			case KeyConfigNum:
				res.Value = strconv.Itoa(s.Config.Num)
			case KeyConfig:
				status := ConfigStatus{
					Config:              s.Config,
					Rebalancing:         s.Rebalancing,
					PendingOutgoing:     s.PendingOutgoing,
					PendingOutgoingData: make(map[int]map[string]string),
				}
				for shardNum, _ := range s.PendingOutgoing {
					status.PendingOutgoingData[shardNum] = s.Shards[shardNum].Data
				}
				res.Value = encodeConfigStatus(status)
			case KeyOutgoingShards:
				res.Value = encodePendingOutgoing(s.PendingOutgoing)
			default:
				if value, err := s.Get(op.Key); err != OK {
					res.Error = err
				} else {
					res.Value = value
				}
			}

		case OpGetShard:
			if s, exist := s.Shards[op.Shard]; exist {
				res.Data = s.Data
			} else {
				res.Error = ErrWrongGroup
			}

		case OpPutShard:
			if op.Num > s.Config.Num {
				res.Error = ErrWrongGroup
				// panic("op.Num > kv.state.Config.Num")
			} else if op.Num < s.Config.Num {
				// skip
			} else {
				shard, exist := s.Shards[op.Shard]
				if !exist {
					res.Error = ErrWrongGroup
				}
				if shard.Status == Incoming {
					s.AddShard(op.Shard, op.Data)
				}
				shard.Status = Working
				s.maybeFinishRebalancing()
			}

		case OpCloseShard:
			delete(s.PendingOutgoing, op.Shard)
			delete(s.Shards, op.Shard)
			s.maybeFinishRebalancing()

		case OpPut:
			res.Error = s.Add(op.Key, op.Value)
			if res.Error == OK {
				s.History.Add(op.ClerkID, op.PropID)
			}

		case OpAppend:
			ori, err := s.Get(op.Key)
			if err == ErrWrongGroup {
				res.Error = ErrWrongGroup
			} else if err == ErrNoKey {
				res.Error = s.Add(op.Key, op.Value)
			} else {
				res.Error = s.Add(op.Key, ori+op.Value)
			}
			if res.Error == OK {
				s.History.Add(op.ClerkID, op.PropID)
			}

		case OpConfig:
			config := op.Config
			incomingShards := make([]int, 0)
			outgoingShards := make(map[int]int, 0)
			if config.Num != s.Config.Num+1 {
				// skip?
				return res
			}
			for i := range config.Shards {
				_, haveShard := s.Shards[i]
				if config.Shards[i] == gid && haveShard == false {
					incomingShards = append(incomingShards, i)
				} else if config.Shards[i] != gid && haveShard == true {
					outgoingShards[i] = config.Shards[i]
				}
			}
			for _, shard := range incomingShards {
				s.Shards[shard] = &Shard{
					Status: Incoming,
					Data:   make(map[string]string),
				}
			}
			for shard := range outgoingShards {
				if s, exist := s.Shards[shard]; exist {
					s.Status = Outgoing
				}
			}
			s.Config = config
			s.Rebalancing = true
			s.PendingOutgoing = outgoingShards
			// in initial config, we set all shards to be working
			if config.Num == 1 {
				for _, shard := range s.Shards {
					shard.Status = Working
				}
			}
			s.maybeFinishRebalancing()
		}
	}
	return res
}

func encodeState(state State) []byte {
	for i := range state.History.Records {
		state.History.Records[i].Applied.Shrink()
	}
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(state.Shards)
	enc.Encode(state.Config)
	enc.Encode(state.Rebalancing)
	enc.Encode(state.PendingOutgoing)
	enc.Encode(state.History)
	return w.Bytes()
}

func decodeState(data []byte) State {
	var state State
	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)
	if dec.Decode(&state.Shards) != nil ||
		dec.Decode(&state.Config) != nil ||
		dec.Decode(&state.Rebalancing) != nil ||
		dec.Decode(&state.PendingOutgoing) != nil ||
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
	// Data is set if it is a get shard op
	Data  map[string]string
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

	ctrlerck    *shardctrler.Clerk
	config      shardctrler.Config
	rebalancing bool
}

func (kv *ShardKV) starter() {
nextreq:
	for req := range kv.rpcReqQueue {
		DPrintf("[server %d-%d] starter process req %v", kv.gid, kv.me, req.Op)
		data := encodeOp(req.Op)
		index, _, isLeader := kv.rf.Start(data)
		DPrintf("[server %d-%d] starter start req %v index %d isLeader %v", kv.gid, kv.me, req, index, isLeader)
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
							req.Value = applyResult.Value
						}
						if req.Op.Type == OpGetShard {
							req.Data = applyResult.Data
						}
						req.RespCh <- applyResult.Error
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
		if op.Type == OpConfig {
			DPrintf("[server %d-%d] before OpConfig %v rebalancing %v", kv.gid, kv.me, kv.state.Config, kv.state.Rebalancing)
		}
		old := kv.state.Rebalancing
		res := kv.state.Apply(op, applyMsg.CommandIndex, kv.gid)
		if op.Type == OpCloseShard || op.Type == OpPutShard {
			DPrintf("[server %d-%d] after close shard ot put shard rebalancing %v shards:", kv.gid, kv.me, kv.state.Rebalancing)
			for i, shard := range kv.state.Shards {
				DPrintf("[server %d-%d] num %d shard %s", kv.gid, kv.me, i, shard)
			}
		}
		if op.Type == OpConfig {
			DPrintf("[server %d-%d] after OpConfig pending outgoing %v config %v rebalancing %v", kv.gid, kv.me, kv.state.PendingOutgoing, kv.state.Config, kv.state.Rebalancing)
		}
		if old == true && kv.state.Rebalancing == false {
			DPrintf("[server %d-%d] finish rebalancing in config %d", kv.gid, kv.me, kv.state.Config.Num)
		}
		kv.addApplyResultsToBuffer(&res)
		if kv.maxraftstate != -1 {
			if kv.persister.RaftStateSize() > kv.maxraftstate/2 {
				data := encodeState(kv.state)
				kv.rf.Snapshot(applyMsg.CommandIndex, data)
			}
		}
	}
}

func (kv *ShardKV) rebalancer() {
	for kv.killed() == false {
		var (
			err             Err
			configStatus    ConfigStatus
			config          shardctrler.Config
			pendingOutgoing map[int]int
			newConfig       shardctrler.Config
		)

		if _, isLeader := kv.rf.GetState(); isLeader == false {
			goto idle
		}

		configStatus, err = kv.GetConfig()
		if err != OK {
			goto idle
		}
		DPrintf("[server %d-%d] got config %v", kv.gid, kv.me, configStatus)
		config = configStatus.Config
		if configStatus.Rebalancing == false {
			newConfig = kv.ctrlerck.Query(config.Num + 1)
			if newConfig.Num == config.Num+1 {
				op := &OpRequest{
					Op: Op{
						Type:   OpConfig,
						Config: newConfig,
					},
					RespCh: make(chan Err),
				}
				kv.rpcReqQueue <- op
				<-op.RespCh
			}
		} else {
			pendingOutgoing = configStatus.PendingOutgoing
			if len(pendingOutgoing) > 0 {
				for shard, target := range pendingOutgoing {
					data := configStatus.PendingOutgoingData[shard]
					args := PutShardArgs{
						Num:   config.Num,
						Shard: shard,
						Data:  data,
					}
					reply := PutShardReply{}

					servers := make([]string, 0, 5)
					for _, server := range config.Groups[target] {
						servers = append(servers, server)
					}

				tryPutShard:
					for {
						for si := 0; si < len(servers); si++ {
							srv := kv.make_end(servers[si])
							ok := srv.Call("ShardKV.PutShard", &args, &reply)
							if ok && reply.Err == OK {
								break tryPutShard
							}
							if ok && reply.Err == ErrWrongGroup {
								break
							}
							// ... not ok, or ErrWrongLeader
						}
						time.Sleep(100 * time.Millisecond)
					}

					DPrintf("[server %d-%d] hand shard %d to %d", kv.gid, kv.me, shard, target)

					if err := kv.closeShard(shard, config.Num); err != OK {
						goto idle
					}
				}
			}
		}

	idle:
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) GetMeta(metaKey string) (string, Err) {
	args := &GetArgs{
		Key: metaKey,
	}
	reply := &GetReply{}
	kv.Get(args, reply)
	return reply.Value, reply.Err
}

func (kv *ShardKV) GetConfig() (ConfigStatus, Err) {
	got, err := kv.GetMeta(KeyConfig)
	if err != OK {
		return ConfigStatus{}, err
	}
	return decodeConfigStatus(got), OK
}

func (kv *ShardKV) closeShard(shard, num int) Err {
	op := &OpRequest{
		Op: Op{
			Type:  OpCloseShard,
			Shard: shard,
			Num:   num,
		},
		RespCh: make(chan Err),
	}
	kv.rpcReqQueue <- op
	for kv.killed() == false {
		select {
		case err := <-op.RespCh:
			return err
		default:
		}
	}
	return ErrWrongLeader
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[server %d-%d] receive get args %v", kv.gid, kv.me, args)
	// defer DPrintf("[server %d-%d] receive get reply %v", kv.gid, kv.me, reply)
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

func (kv *ShardKV) GetShardInternal(shard int) (map[string]string, Err) {
	op := &OpRequest{
		Op: Op{
			Type:  OpGetShard,
			Shard: shard,
		},
		RespCh: make(chan Err),
	}
	kv.rpcReqQueue <- op
	for kv.killed() == false {
		select {
		case err := <-op.RespCh:
			if err != OK {
				return nil, err
			}
			return op.Data, OK
		default:
		}
	}
	return nil, ErrWrongLeader
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	op := &OpRequest{
		Op: Op{
			Type:  OpGetShard,
			Shard: args.Shard,
		},
		RespCh: make(chan Err),
	}
	kv.rpcReqQueue <- op
	for kv.killed() == false {
		select {
		case reply.Err = <-op.RespCh:
			reply.Data = op.Data
			return
		default:
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("[server %d-%d] receive put append args %v", kv.gid, kv.me, args)
	defer DPrintf("[server %d-%d] receive put append reply %v", kv.gid, kv.me, reply)
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

func (kv *ShardKV) PutShard(args *PutShardArgs, reply *PutShardReply) {
	DPrintf("[server %d-%d] receive put shard %v", kv.gid, kv.me, args)
	defer DPrintf("[server %d-%d] reply put shard %v", kv.gid, kv.me, reply)
	op := &OpRequest{
		Op: Op{
			Type:  OpPutShard,
			Num:   args.Num,
			Shard: args.Shard,
			Data:  args.Data,
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
	DPrintf("[server %d-%d] got killed", kv.gid, kv.me)
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
	kv.state = NewState()

	kv.rpcReqQueue = make(chan *OpRequest, 1000)
	kv.applyResults = make(chan *ApplyResult, 1000)

	kv.cond = sync.NewCond(&kv.mu)

	// Use something like this to talk to the shardctrler:
	kv.ctrlerck = shardctrler.MakeClerk(kv.ctrlers)

	go kv.starter()
	go kv.applier()
	go kv.applyResultsBufConsumer()
	go kv.rebalancer()

	return kv
}
