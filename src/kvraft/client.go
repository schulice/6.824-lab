package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	cid      int64
	windex   int64
}

const (
	ELECTION_TIMEOUT = 1000 * time.Millisecond
)

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
	ck.cid = nrand()%int64((^uint64(0)>>1)-1) + 1 // leave 0 for nop command
	ck.leaderId = int(nrand()) % len(servers)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.windex += 1
	args := GetArgs{
		Key:    key,
		CId:    ck.cid,
		WIndex: ck.windex,
	}
	reply := GetReply{}

	sidc := ck.leaderId
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		DPrintf("CLIE\tGet\targs:%v", args)
		if !ok {
			DPrintf("CLIE\tGet\tsid:%d\tTimeout", ck.leaderId)
			ck.updateLeader(sidc)
			continue
		}
		switch reply.Err {
		case ErrWrongLeader:
			DPrintf("CLIE\tGet\tWrongLeader")
			ck.updateLeader(sidc)
			continue
		case ErrNoKey:
			return ""
		case OK:
			return reply.Value
		}
		time.Sleep(ELECTION_TIMEOUT / 10)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	if op != "Put" && op != "Append" {
		return
	}

	ck.windex += 1
	args := PutAppendArgs{
		Key:    key,
		Value:  value,
		CId:    ck.cid,
		WIndex: ck.windex,
	}
	reply := PutAppendReply{}

	sidc := ck.leaderId
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer."+op, &args, &reply)
		DPrintf("CLIE\t%s\targs:%v", op, args)
		if !ok {
			DPrintf("CLIE\tPUT\tsid:%d\tTimeout", ck.leaderId)
			ck.updateLeader(sidc)
			continue
		}
		switch reply.Err {
		case ErrWrongLeader:
			DPrintf("CLIE\tPut\tWrongLeader")
			ck.updateLeader(sidc)
			continue
		case ErrNoKey:
			DPrintf("CLIE\tPut\tNoKey")
		case OK:
			return
		}
		time.Sleep(ELECTION_TIMEOUT / 10)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) updateLeader(sidc int) {
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	if ck.leaderId == sidc {
		time.Sleep(ELECTION_TIMEOUT)
	}
}
