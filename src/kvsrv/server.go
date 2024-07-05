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
	m map[string]string
	// Your definitions here.
	clientOrder map[int64]int64
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
	kv.m[args.Key] = args.Value
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	v, ok := kv.m[args.Key]
	if ok {
		kv.m[args.Key] += args.Value
		kv.mu.Unlock()
		reply.Value = v
	} else {
		kv.m[args.Key] = args.Value
		kv.mu.Unlock()
		reply.Value = ""
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.m = make(map[string]string)

	return kv
}
