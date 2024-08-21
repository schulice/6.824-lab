package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.
	configs []Config // indexed by config num

	appliedIndex int
	clerkIndex   map[int64]int64

	// apply
	appliedCond     sync.Cond
	lastAppliedTime time.Time
	commitInfo      map[int]clerkInfo
}

const (
	_APPLY_TIMEOUT = 1000 * time.Millisecond
)

type clerkInfo struct {
	CID   int64
	Index int64
}

type Op struct {
	// Your data here.
	Command command
	Info    clerkInfo
	Param   interface{}
}

type command uint8

const (
	commandNop = iota
	commandJoin
	commandLeave
	commandMove
	commandQuery
)

type paramJoin struct {
	Servers map[int][]string
}

type paramLeave struct {
	GIDs []int
}

type paramMove struct {
	Shard int
	GID   int
}

type paramQuery struct {
	Num int
}

// MUST mu.Lock()
func (sc *ShardCtrler) isNextCIndex(cid int64, windex int64) bool {
	if _, ok := sc.clerkIndex[cid]; !ok {
		sc.clerkIndex[cid] = 0
	}
	if windex != sc.clerkIndex[cid]+1 {
		return false
	}
	return true
}

// MUST mu.Lock()
func (sc *ShardCtrler) commitRPC(op *Op) Err {
	for !sc.killed() {
		index, _, isLeader := sc.rf.Start(*op)
		if !isLeader {
			return Err("ErrWrongLeader")
		}
		sc.commitInfo[index] = op.Info

		for !(sc.commitInfo[index] != op.Info) {
			sc.appliedCond.Wait()
		}

		if sc.clerkIndex[op.Info.CID] >= op.Info.Index {
			return Err(OK)
		}
	}
	return Err("ErrWrongLeader")
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if !sc.isNextCIndex(args.CID, args.Index) {
		if sc.clerkIndex[args.CID] == args.Index {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
		return
	}
	clerkinfo := clerkInfo{args.CID, args.Index}
	op := Op{
		Command: commandJoin,
		Info:    clerkinfo,
		Param: paramJoin{
			Servers: args.Servers,
		},
	}

	reply.Err = sc.commitRPC(&op)

	if reply.Err == Err("ErrWrongLeader") {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	reply.WrongLeader = true

	if !sc.isNextCIndex(args.CID, args.Index) {
		if sc.clerkIndex[args.CID] == args.Index {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
		return
	}

	clerkinfo := clerkInfo{args.CID, args.Index}
	op := Op{
		Command: commandLeave,
		Info:    clerkinfo,
		Param: paramLeave{
			GIDs: args.GIDs,
		},
	}

	reply.Err = sc.commitRPC(&op)

	if reply.Err == Err("ErrWrongLeader") {
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	reply.WrongLeader = true

	if !sc.isNextCIndex(args.CID, args.Index) {
		if sc.clerkIndex[args.CID] == args.Index {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
		return
	}

	clerkinfo := clerkInfo{args.CID, args.Index}
	op := Op{
		Command: commandMove,
		Info:    clerkinfo,
		Param: paramMove{
			Shard: args.Shard,
			GID:   args.GID,
		},
	}

	reply.Err = sc.commitRPC(&op)

	if reply.Err == Err("ErrWrongLeader") {
		reply.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if !sc.isNextCIndex(args.CID, args.Index) {
		if sc.clerkIndex[args.CID] == args.Index {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
		return
	}

	clerkinfo := clerkInfo{args.CID, args.Index}
	op := Op{
		Command: commandQuery,
		Info:    clerkinfo,
		Param:   nil,
	}

	reply.Err = sc.commitRPC(&op)

	if reply.Err == Err("ErrWrongLeader") {
		reply.WrongLeader = true
		return
	}

	reply.Config = *sc.query(paramQuery{Num: args.Num})
}

// MUST lock
func (sc *ShardCtrler) readNextConfigCopy() *Config {
	config := new(Config)
	config.Num = len(sc.configs)
	current := &sc.configs[config.Num-1]
	config.Groups = make(map[int][]string)
	for k := range current.Groups {
		config.Groups[k] = current.Groups[k]
	}
	copy(config.Shards[:], current.Shards[:])
	return config
}

func (sc *ShardCtrler) rebalence(config *Config) {
	shards := len(config.Shards)
	groups := len(config.Groups)
	if groups == 0 {
		for i := range config.Shards {
			config.Shards[i] = 0
		}
		return
	}

	x := shards / groups
	a := shards % groups
	counter := make(map[int]int)
	for k := range config.Groups {
		counter[k] = 0
	}
	queuei := 0
	pop := func() int {
		for config.Shards[queuei] != 0 {
			queuei += 1
		}
		return queuei
	}
	push := func(i int) {
		config.Shards[i] = 0
	}

	for i, g := range config.Shards {
		_, ok := config.Groups[g]
		if !ok {
			push(i)
		} else if a != 0 && counter[g] == x {
			counter[g] += 1
			a -= 1
		} else if counter[g] >= x {
			push(i)
		} else {
			counter[g] += 1
		}
	}

	sortedKey := make([]int, 0)
	for g := range counter {
		sortedKey = append(sortedKey, g)
	}
	sort.Ints(sortedKey)
	var c, appendn int
	for _, g := range sortedKey {
		c = counter[g]
		appendn = 0
		if a != 0 && c < x+1 {
			a -= 1
			appendn = x + 1 - c
		} else if c < x {
			appendn = x - c
		}
		for appendn != 0 {
			appendn -= 1
			config.Shards[pop()] = g
		}
	}
}

func (sc *ShardCtrler) join(param paramJoin) {
	config := sc.readNextConfigCopy()
	for k := range param.Servers {
		config.Groups[k] = param.Servers[k]
	}
	sc.rebalence(config)
	sc.configs = append(sc.configs, *config)
}

func (sc *ShardCtrler) leave(param paramLeave) {
	config := sc.readNextConfigCopy()
	for _, g := range param.GIDs {
		delete(config.Groups, g)
	}
	sc.rebalence(config)
	sc.configs = append(sc.configs, *config)
}

func (sc *ShardCtrler) move(param paramMove) {
	config := sc.readNextConfigCopy()
	if _, ok := sc.configs[len(sc.configs)-1].Groups[param.GID]; !ok {
		return
	}
	config.Shards[param.Shard] = param.GID
	sc.configs = append(sc.configs, *config)
}

func (sc *ShardCtrler) query(param paramQuery) *Config {
	current := len(sc.configs) - 1
	if param.Num < 0 || param.Num > current {
		return &sc.configs[current]
	}
	return &sc.configs[param.Num]
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applyChHandler() {
	for !sc.killed() {
		msg := <-sc.applyCh
		if msg.CommandValid {
			sc.commandHandle(msg.CommandIndex, msg.Command.(Op))
		}
	}
}

// handle lock
func (sc *ShardCtrler) commandHandle(index int, op Op) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if index != sc.appliedIndex+1 {
		return
	}
	if v, ok := sc.commitInfo[index]; ok && v != op.Info {
		sc.commitInfo = make(map[int]clerkInfo)
	} else {
		delete(sc.commitInfo, index)
	}
	// not Nop committed due to timeout
	if op.Info.CID != 0 {
		if ok := sc.isNextCIndex(op.Info.CID, op.Info.Index); !ok {
			// no side effect for repeated command
			op.Command = commandNop
		} else {
			sc.clerkIndex[op.Info.CID] = op.Info.Index
		}
	}

	switch op.Command {
	case commandNop:
		// Nop
	case commandJoin:
		sc.join(op.Param.(paramJoin))
	case commandLeave:
		sc.leave(op.Param.(paramLeave))
	case commandMove:
		sc.move(op.Param.(paramMove))
	case commandQuery:
		// do nothing write
	}

	sc.appliedIndex = index
	sc.lastAppliedTime = time.Now()
	sc.appliedCond.Broadcast()
}

func (sc *ShardCtrler) applyTimeoutChecker() {
	for !sc.killed() {
		sc.mu.Lock()
		if !time.Now().After(sc.lastAppliedTime.Add(_APPLY_TIMEOUT)) {
			sc.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}

		op := Op{
			Command: commandNop,
			Info: clerkInfo{
				CID:   0,
				Index: 0,
			},
			Param: nil,
		}
		sc.rf.Start(op)
		sc.mu.Unlock()

		time.Sleep(_APPLY_TIMEOUT)
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(paramJoin{})
	labgob.Register(paramLeave{})
	labgob.Register(paramMove{})
	labgob.Register(paramQuery{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.clerkIndex = make(map[int64]int64)
	sc.appliedCond = *sync.NewCond(&sc.mu)
	sc.lastAppliedTime = time.Now()
	sc.commitInfo = make(map[int]clerkInfo)

	go sc.applyChHandler()
	go sc.applyTimeoutChecker()

	return sc
}
