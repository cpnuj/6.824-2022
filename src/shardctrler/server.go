package shardctrler

import (
	"6.824/raft"
	"log"
	"sort"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	startMu sync.Mutex
	applied int

	configs []Config // indexed by config num
}

func rebalance(groups map[int][]string) [NShards]int {
	if len(groups) == 0 {
		return [NShards]int{}
	}
	targets := make([]int, 0, len(groups))
	for gid := range groups {
		targets = append(targets, gid)
	}
	sort.Ints(targets)
	var shards [NShards]int
	for i := 0; i < NShards; i++ {
		shards[i] = targets[i%len(targets)]
	}
	return shards
}

type Op struct {
	// Your data here.
	Type string
	// GIDs is not null when it is OpLeave
	GIDs []int
	// Servers is not null when it is OpJoin
	Servers map[int][]string
	// Shard and GID are set when it is OpMove
	Shard int
	GID   int
}

const (
	OpJoin  = "JOIN"
	OpLeave = "LEAVE"
	OpQuery = "QUERY"
	OpMove  = "MOVE"
)

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.startMu.Lock()
	defer sc.startMu.Unlock()
	err := sc.startAndWaitApply(Op{
		Type:    OpJoin,
		GIDs:    nil,
		Servers: args.Servers,
	})
	reply.WrongLeader = err == ErrWrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.startMu.Lock()
	defer sc.startMu.Unlock()
	err := sc.startAndWaitApply(Op{
		Type:    OpLeave,
		GIDs:    args.GIDs,
		Servers: nil,
	})
	reply.WrongLeader = err == ErrWrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.startMu.Lock()
	defer sc.startMu.Unlock()
	err := sc.startAndWaitApply(Op{
		Type:  OpMove,
		GID:   args.GID,
		Shard: args.Shard,
	})
	reply.WrongLeader = err == ErrWrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	defer DPrintf("[ctrler %d] query args %v reply %v", sc.me, args, reply)
	sc.startMu.Lock()
	defer sc.startMu.Unlock()
	err := sc.startAndWaitApply(Op{
		Type: OpQuery,
	})
	if err == OK {
		if args.Num < 0 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
	} else {
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) Applied() int {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.applied
}

func (sc *ShardCtrler) IncApplied() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.applied++
}

func (sc *ShardCtrler) CurrentConfig() Config {
	return sc.configs[len(sc.configs)-1]
}

func (sc *ShardCtrler) startAndWaitApply(op Op) Err {
	index, term, isLeader := sc.Raft().Start(op)
	DPrintf("[ctrler %d] start op %v", sc.me, op)
	if isLeader == false {
		return ErrWrongLeader
	}
	for {
		curTerm, isLeader := sc.Raft().GetState()
		if isLeader == false || curTerm != term {
			return ErrWrongLeader
		}
		if sc.Applied() >= index {
			return OK
		}
	}
}

// newConfig create append new config which is a deep copy of last one
func (sc *ShardCtrler) newConfig() *Config {
	nextGroups := make(map[int][]string)
	for gid, servers := range sc.CurrentConfig().Groups {
		nextGroups[gid] = servers
	}
	var nextShards [NShards]int
	for i, gid := range sc.CurrentConfig().Shards {
		nextShards[i] = gid
	}
	nextConfig := Config{
		Num:    len(sc.configs),
		Shards: nextShards,
		Groups: nextGroups,
	}
	sc.configs = append(sc.configs, nextConfig)
	return &sc.configs[len(sc.configs)-1]
}

func (sc *ShardCtrler) applier() {
	for applyMsg := range sc.applyCh {
		sc.mu.Lock()
		if op, isOp := applyMsg.Command.(Op); isOp {
			if op.Type == OpQuery {
				goto done
			}
			config := sc.newConfig()
			switch op.Type {
			case OpJoin:
				for gid, servers := range op.Servers {
					config.Groups[gid] = servers
				}
				config.Shards = rebalance(config.Groups)
			case OpLeave:
				for _, gid := range op.GIDs {
					delete(config.Groups, gid)
				}
				config.Shards = rebalance(config.Groups)
			case OpMove:
				config.Shards[op.Shard] = op.GID
			}
		}
	done:
		sc.applied++
		sc.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	go sc.applier()

	return sc
}
