package shardkv

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
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
	shardtarget   []int
	currentConfig shardctrler.Config

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
	for {
		aindex, _, isLeader := kv.rf.Start(*op)
		if !isLeader {
			return ErrWrongLeader
		}
		kv.committedInfo[aindex] = op.Info

		for !(kv.committedInfo[aindex] != op.Info) {
			kv.appliedCond.Wait()
		}

		if kv.isShardValid(op.Info.Shard) {
			return ErrWrongGroup
		}
		if op.Info.Index <= kv.sci[op.Info.Shard][op.Info.Cid] {
			return OK
		}
		if op.Info.Index > kv.sci[op.Info.Shard][op.Info.Cid] {
			// something not linearizable happened
		}
	}
	return ErrWrongLeader
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.isShardValid(int(args.Shard)) {
		reply.Err = ErrWrongGroup
		return
	}
	if !kv.isNextIndex(int(args.Shard), args.Cid, args.Index) {
		if kv.sci[args.Shard][args.Cid] == args.Index {
			reply.Err = OK
			reply.Value = kv.kvstate[args.Shard][args.Key]
		} else {
			reply.Err = ErrWrongLeader
		}
		return
	}

	clerkinfo := &clerkInfo{
		Shard: int(args.Shard),
		Cid:   args.Cid,
		Index: args.Index,
	}
	op := Op{
		Command: commandGet,
		Info:    *clerkinfo,
		Param: paramGet{
			Key: args.Key,
		},
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
	if !kv.isShardValid(int(args.Shard)) {
		reply.Err = ErrWrongGroup
		return
	}
	if !kv.isNextIndex(int(args.Shard), args.Cid, args.Index) {
		if kv.sci[args.Shard][args.Cid] == args.Index {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
		return
	}

	clerkinfo := &clerkInfo{
		Shard: int(args.Shard),
		Cid:   args.Cid,
		Index: args.Index,
	}
	op := Op{
		Command: commandGet,
		Info:    *clerkinfo,
		Param: paramGet{
			Key: args.Key,
		},
	}

	reply.Err = kv.commitRPC(&op)

	// not need to read reply
}

func (kv *ShardKV) configUpdate(param *paramConfigUpdate) {
	if param.Config.Num <= kv.currentConfig.Num {
		return
	}
	for i, v := range param.Config.Shards {
		if v == kv.currentConfig.Shards[i] {
			kv.shardcnum[i] = param.Config.Num
		} else if kv.shardtarget[i] == 0 {
			kv.shardtarget[i] = v
		}
		// if kv.shardtarget i != 0, this shard is sent
	}
	kv.currentConfig = param.Config
}

func (kv *ShardKV) appliedChHandler() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.commandHandle(msg.CommandIndex, msg.Command.(Op))
		} else if msg.SnapshotValid {

		}
	}
}

const _APPLY_TIMEOUT = 1000 * time.Millisecond

func (kv *ShardKV) applyTimeoutChecker() {
	for {
		kv.mu.Lock()
		if !time.Now().After(kv.lastAppliedTime.Add(_APPLY_TIMEOUT)) {
			kv.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		nop := Op{
			Command: commandNop,
			Info:    clerkInfo{0, 0, 0},
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
	if op.Info.Cid != 0 {
		if !kv.isShardValid(op.Info.Shard) ||
			!kv.isNextIndex(op.Info.Shard, op.Info.Cid, op.Info.Index) {
			// in this case command is invalid
			// not need to increase index and should no side effect
			op.Command = commandNop
		} else {
			kv.sci[op.Info.Shard][op.Info.Cid] = op.Info.Index
		}
	}

	// TODO
	switch op.Command {
	case commandNop:
		// do nothing

	// Clerk Op (lazy to put them in a single function)
	case commandGet:
		// not need to do in this
	case commandPut:
		kv.kvstate[op.Info.Shard][op.Param.(paramPut).Key] = op.Param.(paramPut).Value
	case commandAppend:
		kv.kvstate[op.Info.Shard][op.Param.(paramAppend).Key] += op.Param.(paramAppend).Value

	// Peer Op
	case commandConfigUpdate:
	case commandShardRecieve:
	case commandShardSuccess:

	}

	kv.appliedCond.Broadcast()
}

func (kv *ShardKV) configUpdateDetector() {
	for {
		kv.mu.Lock()
		nxt := kv.mck.Query(kv.currentConfig.Num + 1)
		if nxt.Num != kv.currentConfig.Num+1 {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		op := Op{
			Command: commandConfigUpdate,
			Info:    clerkInfo{0, 0, 0},
			Param:   nxt,
		}
		kv.rf.Start(op)
		time.Sleep(100 * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.mck = *shardctrler.MakeClerk(kv.ctrlers)
	// should not query config on making

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
