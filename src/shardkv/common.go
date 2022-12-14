package shardkv

import (
	"log"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID int
	PropID  int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkID int
	PropID  int
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardArgs struct {
	ClerkID int
	PropID  int
	Shard   int
}

type GetShardReply struct {
	Err  Err
	Data map[string]string
}

type PutShardArgs struct {
	// Num is the corresponding config of put shard
	Num     int
	Shard   int
	Data    map[string]string
	History ApplyHistory
}

type PutShardReply struct {
	Err Err
}
