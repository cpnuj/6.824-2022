package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	ck.cachedLeader = 0
	return ck
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
	args := &GetArgs{key}
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
	if op == "Append" {
		ori := ck.Get(key)
		ck.PutAppend(key, ori+value, "Put")
		return
	}
	args := &PutAppendArgs{
		Op:    OpPut,
		Key:   key,
		Value: value,
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
