package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	APPLY_TIMEOUT = 1000 * time.Millisecond
)

type clerkInfo struct {
	CId    int64
	WIndex int64
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command command
	Key     string
	Value   string
	Info    clerkInfo
}

type command uint8

func (c command) String() string {
	switch c {
	case commandNop:
		return "NOP"
	case commandGet:
		return "GET"
	case commandPut:
		return "PUT"
	case commandAppend:
		return "APPEND"
	default:
		return "UNKOWN"
	}
}

const (
	commandNop    = 0o0
	commandGet    = 0o1
	commandPut    = 0o2
	commandAppend = 0o3
)

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// persist
	state map[string]string

	// leader state
	isLeader bool

	// apply
	appliedCond     sync.Cond
	appliedIndex    int
	lastAppliedTime time.Time
	commitInfo      map[int]clerkInfo
	clerkWIndex     map[int64]int64
}

// MUST mu.Lock()
func (kv *KVServer) windexChecker(cid int64, windex int64) bool {
	if _, ok := kv.clerkWIndex[cid]; !ok {
		kv.clerkWIndex[cid] = 0
	}
	if windex != kv.clerkWIndex[cid]+1 {
		DPrintf("INDEX\tUnmatch\tcurrent:%d\ttarget:%d", windex, kv.clerkWIndex[cid]+1)
		return false
	}
	return true
}

// MUST mu.Lock()
func (kv *KVServer) rpcCommiter(op *Op) Err {
	for {
		var index int
		index, _, kv.isLeader = kv.rf.Start(*op)
		if !kv.isLeader {
			return Err(ErrWrongLeader)
		}
		kv.commitInfo[index] = op.Info

		DPrintf("%v\tsid:%d\tindex:%d\t%v", op.Command, kv.me, index, op.Info)

		for !(kv.appliedIndex >= index ||
			kv.clerkWIndex[op.Info.CId]+1 != op.Info.WIndex /*WIndex changed*/) {
			kv.appliedCond.Wait()
		}

		if kv.clerkWIndex[op.Info.CId] < op.Info.WIndex {
			DPrintf("RPC\tERROR\tno linearizable windex")
		}

		info, ok := kv.commitInfo[index]
		if ok && info == op.Info {
			delete(kv.commitInfo, index)
			break
		}
	}
	return Err(OK)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if ok := kv.windexChecker(args.CId, args.WIndex); !ok {
		if kv.clerkWIndex[args.CId] >= args.WIndex {
			reply.Err = Err(OK)
			reply.Value = kv.state[args.Key]
		} else {
			reply.Err = Err("LargerWIndex")
		}
		return
	}

	clerkinfo := clerkInfo{args.CId, args.WIndex}
	op := Op{
		Command: commandGet,
		Key:     args.Key,
		Value:   "",
		Info:    clerkinfo,
	}

	reply.Err = kv.rpcCommiter(&op)

	if reply.Err != Err(OK) {
		return
	}

	reply.Value = kv.state[args.Key]
	DPrintf("GET\tFinish\t%v", args)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if ok := kv.windexChecker(args.CId, args.WIndex); !ok {
		if kv.clerkWIndex[args.CId] >= args.WIndex {
			reply.Err = Err(OK)
		} else {
			reply.Err = Err("LargerWIndex")
		}
		return
	}

	clerkinfo := clerkInfo{args.CId, args.WIndex}
	op := Op{
		Command: commandPut,
		Key:     args.Key,
		Value:   args.Value,
		Info:    clerkinfo,
	}

	reply.Err = kv.rpcCommiter(&op)

	if reply.Err != Err(OK) {
		return
	}
	DPrintf("PUT\tFinish\t%v", args)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if ok := kv.windexChecker(args.CId, args.WIndex); !ok {
		if kv.clerkWIndex[args.CId] >= args.WIndex {
			reply.Err = Err(OK)
		} else {
			reply.Err = Err("LargerWIndex")
		}
		return
	}

	clerkinfo := clerkInfo{args.CId, args.WIndex}
	op := Op{
		Command: commandAppend,
		Key:     args.Key,
		Value:   args.Value,
		Info:    clerkinfo,
	}

	reply.Err = kv.rpcCommiter(&op)

	if reply.Err != Err(OK) {
		return
	}
	DPrintf("APP\tFinish\t%+v", args)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyHandle() {
	for {
		msg := <-kv.applyCh
		// DPrintf("APPL\tmsg:%v", msg)
		if msg.CommandValid {
			kv.mu.Lock()
			go kv.commandHandle(msg.CommandIndex, msg.Command.(Op))
		} else if msg.SnapshotValid {

		}

	}
}

// handle lock
func (kv *KVServer) commandHandle(index int, op Op) {
	defer kv.mu.Unlock()

	DPrintf("APPL\tid:%d\top:%+v", kv.me, op)

	if index <= kv.appliedIndex {
		return
	}
	if index != kv.appliedIndex+1 {
		DPrintf("APPL\tErrorIndex\tindex:%d\tcurrent:%d", index, kv.appliedIndex)
		return
	}

	// ACCEPT op index, all return goto EXIT

	if op.Info.CId != 0 {
		if ok := kv.windexChecker(op.Info.CId, op.Info.WIndex); !ok {
			DPrintf("APPL\tErrorWIndex\tTarget:%d\tCurrent:%d", op.Info.WIndex, kv.clerkWIndex[op.Info.CId])
			goto EXIT
		}
	}

	if v, ok := kv.commitInfo[index]; ok && v != op.Info {
		for k := range kv.commitInfo {
			if k >= index {
				delete(kv.commitInfo, k)
			}
		}
	}

	switch op.Command {
	case commandNop:
	case commandGet:
	case commandPut:
		kv.state[op.Key] = op.Value
	case commandAppend:
		kv.state[op.Key] += op.Value
	}
	kv.clerkWIndex[op.Info.CId] = op.Info.WIndex

EXIT:
	kv.appliedIndex = index
	DPrintf("SERVER\tClerkWIndex:%v", kv.clerkWIndex)
	kv.lastAppliedTime = time.Now()
	kv.appliedCond.Broadcast()
}

func (kv *KVServer) applyTimeoutHandler() {
	for !kv.killed() {
		kv.mu.Lock()
		if !time.Now().After(kv.lastAppliedTime.Add(APPLY_TIMEOUT)) {
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		DPrintf("APPL\tTIMOUT\tstart")
		op := Op{
			Command: commandNop,
			Key:     "",
			Value:   "",
			Info: clerkInfo{
				CId:    0,
				WIndex: 0,
			},
		}
		_, _, kv.isLeader = kv.rf.Start(op)
		if kv.isLeader {
			DPrintf("APPL\tTIMEOUT\tfind leader %d", kv.me)
		}
		kv.mu.Unlock()
		DPrintf("APPL\tTIMOUT\tfinish")
		time.Sleep(10 * time.Millisecond)
	}
}

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
	kv.state = make(map[string]string)
	kv.appliedCond = *sync.NewCond(&kv.mu)
	kv.isLeader = false
	kv.commitInfo = make(map[int]clerkInfo)
	kv.clerkWIndex = make(map[int64]int64)

	kv.lastAppliedTime = time.Now()
	go kv.applyHandle()
	go kv.applyTimeoutHandler()

	return kv
}
