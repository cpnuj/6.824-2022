package kvraft

import (
	"6.824/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu           sync.Mutex
	clerkID      int
	nextPropID   int
	cachedLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkID = int(nrand())
	ck.nextPropID = 0
	ck.cachedLeader = 0
	return ck
}

func (ck *Clerk) getPropID() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ret := ck.nextPropID
	ck.nextPropID++
	return ret
}

func callWithTimeout(fn func() chan struct{}, timeout time.Duration) {
	doneCh := fn()
	select {
	case <-doneCh:
	case <-time.After(timeout):
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := &GetArgs{Key: key, ClerkID: ck.clerkID, PropID: ck.getPropID()}
	reply := &GetReply{}
	if ok := ck.servers[ck.cachedLeader].Call("KVServer.Get", args, reply); ok {
		if reply.Err != ErrWrongLeader {
			if reply.Err == ErrNoKey {
				return ""
			} else {
				return reply.Value
			}
		}
	}
	for {
		for i := range ck.servers {
			if ok := ck.servers[i].Call("KVServer.Get", args, reply); ok {
				DPrintf("%v", reply)
				if reply.Err == ErrWrongLeader {
					continue
				}
				ck.cachedLeader = i
				if reply.Err == ErrNoKey {
					return ""
				} else {
					return reply.Value
				}
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:     key,
		Value:   value,
		ClerkID: ck.clerkID,
		PropID:  ck.getPropID(),
	}
	if op == "Append" {
		args.Op = OpAppend
	} else {
		args.Op = OpPut
	}
	reply := &PutAppendReply{}
	if ok := ck.servers[ck.cachedLeader].Call("KVServer.PutAppend", args, reply); ok {
		if reply.Err == OK {
			return
		}
	}
	for {
		for i := range ck.servers {
			if ok := ck.servers[i].Call("KVServer.PutAppend", args, reply); ok {
				DPrintf("%v", reply)
				if reply.Err == ErrWrongLeader {
					continue
				}
				ck.cachedLeader = i
				if reply.Err == OK {
					return
				}
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
