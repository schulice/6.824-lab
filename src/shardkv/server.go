package shardkv

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command command
	Info    clerkInfo
	Param   interface{}
}

type command uint8

const (
	commandNop = iota
	commandGet
	commandPut
	commandAppend
	commandConfigUpdate
	commandShardRecieve
	commandShardSuccess
)

func (c command) String() string {
	switch c {
	case commandNop:
		return "NOP"
	case commandGet:
		return "GET"
	case commandPut:
		return "PUT"
	case commandAppend:
		return "APD"
	case commandConfigUpdate:
		return "CUP"
	case commandShardRecieve:
		return "SRV"
	case commandShardSuccess:
		return "SSC"
	default:
		return "UNKOWN"
	}
}

type clerkInfo struct {
	Shard int
	Cid   int64
	Index int64
}

type paramGet struct {
	Key string
}

type paramPutAppend struct {
	Key   string
	Value string
}

type paramPut = paramPutAppend
type paramAppend = paramPutAppend

type paramConfigUpdate struct {
	Config shardctrler.Config
}

type paramShardRecieve struct {
	Shard      int
	CNum       int
	ClerkIndex map[int64]int64
	KV         map[string]string
}

type paramShardSuccess struct {
	Shard int
	CNum  int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// persist
	appliedIndex  int
	kvstate       []map[string]string
	sci           []map[int64]int64 // (shard, cid) -> index
	shardcnum     []int
	currentConfig shardctrler.Config
	shardtarget   map[int][]string

	// helper

	// apply
	appliedCond     sync.Cond
	lastAppliedTime time.Time
	committedInfo   map[int]clerkInfo

	// shard
	mck shardctrler.Clerk
}

func (kv *ShardKV) isShardValid(shard int) bool {
	if kv.currentConfig.Shards[shard] != kv.gid ||
		kv.shardcnum[shard] != kv.currentConfig.Num {
		return false
	}
	return true
}

func (kv *ShardKV) isNextIndex(shard int, cid int64, index int64) bool {
	if _, ok := kv.sci[shard][cid]; !ok {
		kv.sci[shard][cid] = 0
	}
	if index != kv.sci[shard][cid]+1 {
		return false
	}
	return true
}

func (kv *ShardKV) commitRPC(op *Op) Err {
	for !kv.killed() {
		aindex, _, isLeader := kv.rf.Start(*op)
		if !isLeader {
			return ErrWrongLeader
		}
		// DPrintf("COMM\tstart\tgid:%d\tindex:%d\top:%v", kv.gid, aindex, op)
		kv.committedInfo[aindex] = op.Info

		for !(kv.committedInfo[aindex] != op.Info) {
			kv.appliedCond.Wait()
		}

		if kv.isShardValid(op.Info.Shard) {
			return ErrWrongGroup
		}
		if op.Info.Index <= kv.sci[op.Info.Shard][op.Info.Cid] {
			// DPrintf("COMM\tfinish\tgid:%d\tindex:%d\top:%v", kv.gid, aindex, op)
			return OK
		}
		if op.Info.Index > kv.sci[op.Info.Shard][op.Info.Cid] {
			DPrintf("COMM\t---ERROR---\tnolinear\tgid:%d\tindex:%d\top:%v", kv.gid, aindex, op)
			// something not linearizable happened
		}
	}
	return ErrWrongLeader
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("RPCR\tGet\tstart\targs:%v", args)
	if !kv.isShardValid(int(args.Shard)) {
		reply.Err = ErrWrongGroup
		return
	}
	DPrintf("RPCR\tGet\tshard\targs:%v", args)
	if !kv.isNextIndex(int(args.Shard), args.Cid, args.Index) {
		if kv.sci[args.Shard][args.Cid] == args.Index {
			reply.Err = OK
			reply.Value = kv.kvstate[args.Shard][args.Key]
		} else {
			reply.Err = ErrWrongLeader
		}
		return
	}
	DPrintf("RPCR\tGet\tindex\targs:%v", args)

	clerkinfo := &clerkInfo{
		Shard: int(args.Shard),
		Cid:   args.Cid,
		Index: args.Index,
	}
	op := Op{
		Command: commandGet,
		Info:    *clerkinfo,
		Param:   nil,
	}

	reply.Err = kv.commitRPC(&op)

	if reply.Err == OK {
		reply.Value = kv.kvstate[args.Shard][args.Key]
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("RPCR\t%s\tstart\targs:%v", args.Op, args)
	if !kv.isShardValid(int(args.Shard)) {
		reply.Err = ErrWrongGroup
		return
	}
	DPrintf("RPCR\t%s\tshard\targs:%v", args.Op, args)
	if !kv.isNextIndex(int(args.Shard), args.Cid, args.Index) {
		if kv.sci[args.Shard][args.Cid] == args.Index {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
		return
	}
	DPrintf("RPCR\t%s\tindex\targs:%v", args.Op, args)

	clerkinfo := &clerkInfo{
		Shard: int(args.Shard),
		Cid:   args.Cid,
		Index: args.Index,
	}
	op := Op{
		Command: commandNop,
		Info:    *clerkinfo,
		Param: paramPutAppend{
			Key:   args.Key,
			Value: args.Value,
		},
	}
	switch args.Op {
	case "Put":
		op.Command = commandPut
	case "Append":
		op.Command = commandAppend
	}

	reply.Err = kv.commitRPC(&op)
}

func (kv *ShardKV) RecieveShard(args *RecieveShardArgs, reply *RecieveShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.CNum <= int64(kv.shardcnum[args.Shard]) {
		reply.Err = OK
		return
	}
	if args.CNum > int64(kv.currentConfig.Num) {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Command: commandShardRecieve,
		Info:    clerkInfo{-1, -1, -1},
		Param: paramShardRecieve{
			CNum:       int(args.CNum),
			Shard:      int(args.Shard),
			ClerkIndex: args.ClerkIndex,
			KV:         args.KV,
		},
	}

	for !kv.killed() {
		index, _, isleader := kv.rf.Start(op)
		if !isleader {
			reply.Err = ErrWrongLeader
			break
		}
		kv.committedInfo[index] = op.Info
		for kv.committedInfo[index] == op.Info {
			kv.appliedCond.Wait()
		}
		if kv.shardcnum[op.Param.(paramShardRecieve).Shard] >= op.Param.(paramShardRecieve).CNum {
			DPrintf("RPCS\tRecieve\tfinish\tgid:%d\tsid:%d\targs:%v", kv.gid, args.Shard, args)
			reply.Err = OK
			break
		}
	}
}

func (kv *ShardKV) configUpdate(param *paramConfigUpdate) {
	if param.Config.Num <= kv.currentConfig.Num {
		return
	}
	for s, g := range param.Config.Shards {
		if kv.shardcnum[s]+1 == param.Config.Num {
			if g == kv.gid {
				kv.shardcnum[s] = param.Config.Num
			} else if kv.shardcnum[s] != 0 {
				kv.shardtarget[s] = param.Config.Groups[g]
			}
		}
		// if kv.shardcnum is not linearable and kv.shardtarget i != 0, this shard is sent
	}
	kv.currentConfig = param.Config
	DPrintf("CUPD\tfinish\tgid:%d\trid:%d\tconfig:%v", kv.gid, kv.me, kv.currentConfig)
}

func (kv *ShardKV) shardRecieve(param *paramShardRecieve) {
	if param.CNum <= kv.shardcnum[param.Shard] {
		return
	}

	kv.kvstate[param.Shard] = make(map[string]string)
	for i := range param.KV {
		kv.kvstate[param.Shard][i] = param.KV[i]
	}
	kv.sci[param.Shard] = make(map[int64]int64)
	for i := range param.ClerkIndex {
		kv.sci[param.Shard][i] = param.ClerkIndex[i]
	}

	if kv.currentConfig.Shards[param.Shard] == kv.gid {
		kv.shardcnum[param.Shard] = kv.currentConfig.Num
	} else {
		kv.shardcnum[param.Shard] = kv.currentConfig.Num - 1
		kv.shardtarget[param.Shard] = kv.currentConfig.Groups[kv.currentConfig.Shards[param.Shard]]
	}
}

func (kv *ShardKV) shardSuccess(param *paramShardSuccess) {
	if param.CNum != kv.shardcnum[param.Shard] {
		return
	}
	if _, ok := kv.shardtarget[param.Shard]; !ok {
		return
	}
	delete(kv.shardtarget, param.Shard)
	kv.sci[param.Shard] = make(map[int64]int64)
	kv.kvstate[param.Shard] = make(map[string]string)
}

func (kv *ShardKV) configUpdateDetector() {
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)
		kv.mu.Lock()
		_, isleader := kv.rf.GetState()
		if !isleader {
			kv.mu.Unlock()
			continue
		}

		// DPrintf("CUPD\tstart\tgid:%d\tcnum:%d", kv.gid, kv.currentConfig.Num+1)
		nxt := kv.mck.Query(kv.currentConfig.Num + 1)
		if nxt.Num != kv.currentConfig.Num+1 {
			kv.mu.Unlock()
			continue
		}

		// DPrintf("CUPD\tcommit\tgid:%d\tcnum:%d", kv.gid, kv.currentConfig.Num+1)
		op := Op{
			Command: commandConfigUpdate,
			Info:    clerkInfo{-1, -1, -1},
			Param:   paramConfigUpdate{nxt},
		}
		kv.rf.Start(op)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) shardSender() {
	for !kv.killed() {
		kv.mu.Lock()
		if len(kv.shardtarget) == 0 {
			kv.mu.Unlock()
			time.Sleep(_SMALL_INTERVEL)
			continue
		}
		for s, targets := range kv.shardtarget {
			go kv.shardSendHandle(s, targets)
		}
		kv.mu.Unlock()
		time.Sleep(5 * _QUARY_DURATION)
	}
}

func (kv *ShardKV) shardSendHandle(s int, targets []string) {
	kv.mu.Lock()
	if _, ok := kv.shardtarget[s]; !ok {
		kv.mu.Unlock()
		return
	}
	cnum := kv.shardcnum[s]
	ci := make(map[int64]int64)
	for i := range kv.sci[s] {
		ci[i] = kv.sci[s][i]
	}
	kvs := make(map[string]string)
	for i := range kv.kvstate[s] {
		kvs[i] = kv.kvstate[s][i]
	}
	args := RecieveShardArgs{
		CNum:       int64(cnum),
		Shard:      int64(s),
		ClerkIndex: ci,
		KV:         kvs,
	}
	kv.mu.Unlock()

	var sendi int
	var target string
	for sendi, target = range targets {
		end := kv.make_end(target)
		reply := RecieveShardReply{}
		ok := end.Call("ShardKV.RecieveShard", &args, &reply)
		if ok && reply.Err == OK {
			break
		}
	}
	if sendi == len(target) {
		return
	}

	kv.mu.Lock()
	if _, ok := kv.shardtarget[s]; !ok {
		kv.mu.Unlock()
		return
	}
	if kv.shardcnum[s] != cnum {
		kv.mu.Unlock()
		return
	}
	op := Op{
		Command: commandShardSuccess,
		Info:    clerkInfo{-1, -1, -1},
		Param:   paramShardSuccess{s, cnum},
	}
	kv.rf.Start(op)
	kv.mu.Unlock()
}

func (kv *ShardKV) appliedChReciever() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.commandHandle(msg.CommandIndex, msg.Command.(Op))
		} else if msg.SnapshotValid {
			kv.installSnapshotHandle(msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot)
		}
	}
}

const (
	_APPLY_TIMEOUT  = 1000 * time.Millisecond
	_QUARY_DURATION = 100 * time.Millisecond
	_SMALL_INTERVEL = 10 * time.Millisecond
)

func (kv *ShardKV) applyTimeoutChecker() {
	for !kv.killed() {
		kv.mu.Lock()
		if !time.Now().After(kv.lastAppliedTime.Add(_APPLY_TIMEOUT)) {
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		nop := Op{
			Command: commandNop,
			Info:    clerkInfo{-1, -1, -1},
			Param:   nil,
		}
		kv.rf.Start(nop)
		kv.mu.Unlock()
		time.Sleep(_APPLY_TIMEOUT)
	}

}

func (kv *ShardKV) commandHandle(index int, op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if index != kv.appliedIndex+1 {
		return
	}
	kv.appliedIndex = index
	kv.lastAppliedTime = time.Now()
	if v, ok := kv.committedInfo[index]; ok && v != op.Info {
		kv.committedInfo = make(map[int]clerkInfo)
	} else {
		delete(kv.committedInfo, index)
	}
	// not from server self, need update clerk index table
	if op.Info.Cid != -1 {
		if !kv.isShardValid(op.Info.Shard) ||
			!kv.isNextIndex(op.Info.Shard, op.Info.Cid, op.Info.Index) {
			// In this case, command is invalid
			// Should not increase index and have side effect
			op.Command = commandNop
		} else {
			kv.sci[op.Info.Shard][op.Info.Cid] = op.Info.Index
		}
	}

	// TODO
	switch op.Command {
	// Do nothing
	case commandNop:

	// Clerk Op (lazy to put them in each function)
	case commandGet:
		// no write behaviour
	case commandPut:
		kv.kvstate[op.Info.Shard][op.Param.(paramPut).Key] = op.Param.(paramPut).Value
	case commandAppend:
		kv.kvstate[op.Info.Shard][op.Param.(paramAppend).Key] += op.Param.(paramAppend).Value

	// Peer Op
	case commandConfigUpdate:
		z := op.Param.(paramConfigUpdate)
		kv.configUpdate(&z)
	case commandShardRecieve:
		z := op.Param.(paramShardRecieve)
		kv.shardRecieve(&z)
	case commandShardSuccess:
		z := op.Param.(paramShardSuccess)
		kv.shardSuccess(&z)
	}

	kv.appliedCond.Broadcast()
}

func (kv *ShardKV) snapshoter(persister *raft.Persister) {
	if kv.maxraftstate == -1 {
		return
	}

	for !kv.killed() {
		kv.mu.Lock()
		if persister.RaftStateSize() >= kv.maxraftstate*15/16 {
			kv.snapshotHandle()
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) snapshotHandle() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.appliedIndex)
	e.Encode(kv.kvstate)
	e.Encode(kv.sci)
	e.Encode(kv.currentConfig)
	e.Encode(kv.shardcnum)
	e.Encode(kv.shardtarget)
	snap := w.Bytes()
	kv.rf.Snapshot(kv.appliedIndex, snap)
}
func (kv *ShardKV) readSnapshot(snap []byte) bool {
	makepersist := func() {
		kv.appliedIndex = 0
		kv.kvstate = make([]map[string]string, shardctrler.NShards)
		for i := range kv.kvstate {
			kv.kvstate[i] = make(map[string]string)
		}
		kv.sci = make([]map[int64]int64, shardctrler.NShards)
		for i := range kv.sci {
			kv.sci[i] = make(map[int64]int64)
		}
		kv.currentConfig = shardctrler.Config{}
		kv.currentConfig.Groups = make(map[int][]string)
		kv.shardcnum = make([]int, shardctrler.NShards)
		for i := range kv.shardcnum {
			kv.shardcnum[i] = 0
		}
		kv.shardtarget = make(map[int][]string)
	}

	if snap == nil || len(snap) < 1 {
		makepersist()
		return true
	}
	r := bytes.NewBuffer(snap)
	d := labgob.NewDecoder(r)
	var appliedIndex int
	var kvstate []map[string]string
	var sci []map[int64]int64
	var currentConfig shardctrler.Config
	var shardcnum []int
	var shardtarget map[int][]string
	if d.Decode(&appliedIndex) != nil ||
		d.Decode(&kvstate) != nil ||
		d.Decode(&sci) != nil ||
		d.Decode(&currentConfig) != nil ||
		d.Decode(&shardcnum) != nil ||
		d.Decode(&shardtarget) != nil {
		makepersist()
		return false
	}

	kv.appliedIndex = appliedIndex
	kv.kvstate = kvstate
	kv.sci = sci
	kv.currentConfig = currentConfig
	kv.shardcnum = shardcnum
	kv.shardtarget = shardtarget
	return true
}

func (kv *ShardKV) installSnapshotHandle(index int, term int, snap []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.appliedIndex >= index {
		return
	}
	ok := kv.readSnapshot(snap)
	if !ok {
		// something wrong
	}
	for k := range kv.committedInfo {
		if k <= index {
			delete(kv.committedInfo, k)
		}
	}
	kv.appliedCond.Broadcast()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(paramGet{})
	labgob.Register(paramPut{})
	labgob.Register(paramAppend{})
	labgob.Register(paramConfigUpdate{})
	labgob.Register(paramShardRecieve{})
	labgob.Register(paramShardSuccess{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	// should not query config on making

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.readSnapshot(persister.ReadSnapshot())
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mck = *shardctrler.MakeClerk(kv.ctrlers)
	kv.appliedCond = *sync.NewCond(&kv.mu)
	kv.committedInfo = make(map[int]clerkInfo)
	kv.lastAppliedTime = time.Now()

	go kv.appliedChReciever()
	go kv.applyTimeoutChecker()
	go kv.snapshoter(persister)
	go kv.configUpdateDetector()
	go kv.shardSender()

	DPrintf("MAKE\tgid:%d", kv.gid)

	return kv
}
