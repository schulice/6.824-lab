package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	m  map[string]string
	// Your definitions here.
	clientWOrder       map[int64]int64
	clientAppendReturn map[int64]string
}

// need be done in lock
func (kv *KVServer) checkOrder(client int64, order int64) bool {
	v, ok := kv.clientWOrder[client]
	if !ok {
		kv.clientWOrder[client] = 0
		v = 0
	}
	if order == v+1 {
		kv.clientWOrder[client] = order
		delete(kv.clientAppendReturn, client)
		return true
	} else {
		return false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	v, ok := kv.m[args.Key]
	kv.mu.Unlock()
	if ok {
		reply.Value = v
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	ook := kv.checkOrder(args.Uid, args.WOrder)
	if !ook {
		kv.mu.Unlock()
		return
	}
	kv.m[args.Key] = args.Value
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	ook := kv.checkOrder(args.Uid, args.WOrder)
	if !ook {
		reply.Value = kv.clientAppendReturn[args.Uid]
		kv.mu.Unlock()
		return
	}
	v, ok := kv.m[args.Key]
	if ok {
		kv.m[args.Key] += args.Value
		reply.Value = v
	} else {
		kv.m[args.Key] = args.Value
		reply.Value = ""
	}
	kv.clientAppendReturn[args.Uid] = reply.Value
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.m = make(map[string]string)
	kv.clientWOrder = make(map[int64]int64)
	kv.clientAppendReturn = make(map[int64]string)

	return kv
}
