package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	SMALL_INTERVAL     = 10 * time.Millisecond
	HEARTBEAT_INTERVAL = 100 * time.Millisecond
	ELECTION_TIMEOUT   = 1000 * time.Millisecond // fixed, and the ticker will random sleep
)

// go1.15 no min
func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}

type role int8

const (
	_ROLE_FOLLOWER     role = 0
	_ROLE_CANDIDATE    role = 1
	_ROLE_LEADER       role = 2
)

// debugger
func (st *role) toString() string {
	switch *st {
	case _ROLE_FOLLOWER:
		return "F"
	case _ROLE_CANDIDATE:
		return "C"
	case _ROLE_LEADER:
		return "L"
	}
	return "N"
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent !!lock
	currentTerm int
	votedFor    int
	log         []entry // log[0] use for bound check

	// snapshot info, may use log[0] to save term?
	lastIncludedIndex int
	currentSnapshot   []byte

	// all server volatile state
	commitIndex int
	lastApplied int

	// leader volatile
	nextIndex  []int
	matchIndex []int

	// helper
	applyCh      chan ApplyMsg
	role         role
	lastHearbeat time.Time
	commitCond   sync.Cond
	applyCond    sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.role == _ROLE_LEADER

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.log)
	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.currentSnapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// normal init
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastIncludedIndex = 0
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.log = make([]entry, 1)
	rf.log[0] = entry{0, 0}

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var lastIncludedIndex int
	var log []entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&log) != nil {
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastIncludedIndex = lastIncludedIndex
	rf.log = log

	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex < index {
		fmt.Printf("SNAP\tERROR\tlarger Index")
	}
	if index <= rf.lastIncludedIndex {
		DPrintf("SNAP\tOutdate Index\t%d\t%d", rf.lastIncludedIndex, index)
		return
	}

	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	rf.currentSnapshot = clone(snapshot)
	rf.log = append(make([]entry, 0), rf.log[rf.toLogIndex(index):]...)
	rf.log[0].Command = nil
	rf.lastIncludedIndex = index
	rf.persist()
	DPrintf("SNAP\t%d\tindex:%d\t%v", rf.me, index, rf.log)

	// send snapshot logic in append RPC
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term > reply.Term {
		rf.meetLargerTerm(args.Term)
	} else if args.Term < reply.Term {
		return
	}

	leftBound := rf.toAbsIndex(0) >= args.LastIncludedIndex
	if leftBound {
		return
	}

	DPrintf("BEIS\t%d\t{%d, %d}\t%d %v", rf.me, args.LastIncludedIndex, args.Term, rf.lastIncludedIndex, rf.log)
	rightBound := rf.toAbsIndex(len(rf.log)-1) < args.LastIncludedIndex
	if rightBound || rf.log[rf.toLogIndex(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
		rf.log = make([]entry, 1)
		rf.log[0].Term = args.LastIncludedTerm
	} else {
		rf.log = append(make([]entry, 0), rf.log[rf.toLogIndex(args.LastIncludedIndex):]...)
		rf.log[0].Command = nil
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.currentSnapshot = args.Data
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		// need lock to prevent random applymsg
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		}
	}
	rf.persist()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type entry struct {
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Entries      []entry
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

// MUST rf.mu.Lock()
func (rf *Raft) makeEmptyAppendEntriesArgs(server int) AppendEntriesArgs {
	newArgs := AppendEntriesArgs{}
	newArgs.Term = rf.currentTerm
	newArgs.LeaderId = rf.me
	newArgs.LeaderCommit = rf.commitIndex
	newArgs.PrevLogIndex = rf.nextIndex[server] - 1
	if newArgs.PrevLogIndex < rf.lastIncludedIndex {
		newArgs.PrevLogTerm = 0
	} else {
		newArgs.PrevLogTerm = rf.log[rf.toLogIndex(newArgs.PrevLogIndex)].Term
	}
	newArgs.Entries = make([]entry, 0)
	return newArgs
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Term = 0
		reply.Success = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if Debug && len(args.Entries) >= 0 {
		DPrintf("RAPP\tS%d\t\nlog: %d\t%v\nentry: {%d, %d}\t%v",
			rf.me, rf.lastIncludedIndex, rf.log,
			args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XTerm = 0
	reply.XIndex = args.PrevLogIndex
	reply.XLen = rf.toAbsIndex(len(rf.log))
	// all server rule
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.meetLargerTerm(args.Term)
	}
	// refuse outdate term update heartbeat timer
	rf.lastHearbeat = time.Now()
	// candidate rule
	if rf.role == _ROLE_CANDIDATE {
		rf.role = _ROLE_FOLLOWER
	}
	// refuse outbound index
	if args.PrevLogIndex < rf.toAbsIndex(0) ||
		args.PrevLogIndex > rf.toAbsIndex(len(rf.log)-1) {
		return
	}
	if rf.log[rf.toLogIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[rf.toLogIndex(args.PrevLogIndex)].Term
		// binary search to find xxx(y)yyzzz
		reply.XIndex = func() int {
			l := 0
			r := rf.toLogIndex(args.PrevLogIndex)
			for l < r {
				mid := (l + r) / 2
				if rf.log[mid].Term == reply.XTerm {
					r = mid
				} else {
					l = mid + 1
				}
			}
			return rf.toAbsIndex(l)
		}()
		return
	}
	// update follower log
	// heartbeat will skip this
	for i := range args.Entries {
		// tolog(PrevLogIndex) in [0, log.sz - 1]
		li := rf.toLogIndex(args.PrevLogIndex) + i + 1
		if li == len(rf.log) ||
			rf.log[li].Term != args.Entries[i].Term {
			rf.log = rf.log[:li]
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		lastNewIdx := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, lastNewIdx)
		rf.applyCond.Broadcast()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidtateId int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if rf.killed() {
		reply.Term = 0
		reply.VoteGranted = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// all server rule
  if args.Term < rf.currentTerm {
    return
  }
  if args.Term > rf.currentTerm {
    rf.meetLargerTerm(args.Term)
  }
  // rev has voted for other
	if rf.votedFor != -1 && rf.votedFor != args.CandidtateId {
		return
	}
  // req is not up-to-date than rev
	lastLogTerm := rf.log[len(rf.log)-1].Term
	if lastLogTerm > args.LastLogTerm ||
		lastLogTerm == args.LastLogTerm && rf.toAbsIndex(len(rf.log)-1) > args.LastLogIndex {
		return
	}
	// reset timer when voting
	rf.lastHearbeat = time.Now()
	if rf.votedFor == -1 {
		rf.votedFor = args.CandidtateId
		rf.persist()
	}
	reply.VoteGranted = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	// Your code here (3B).
	if rf.killed() {
		return index, term, isLeader
	}
	rf.mu.Lock()
	isLeader = rf.role == _ROLE_LEADER
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	term = rf.currentTerm
	e := entry{command, term}
	rf.log = append(rf.log, e)
	index = rf.toAbsIndex(len(rf.log) - 1)
	// no usage
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	// no usage
	rf.persist()
	DPrintf("CLIE\tP%d\tI%d\tCommand: %v", rf.me, index, command)
	DPrintf("NEXT\t%v", rf.nextIndex)
	rf.mu.Unlock()
	rf.commitCond.Broadcast()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.commitCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// MUST mu.lock
func (rf *Raft) toLogIndex(i int) int {
	return i - rf.lastIncludedIndex
}

// MUST mu.lock
func (rf *Raft) toAbsIndex(i int) int {
	return i + rf.lastIncludedIndex
}

// MUST mu.Lock() before call
func (rf *Raft) follower() {
}

// MUST mu.lock() to enter
// To follower
func (rf *Raft) meetLargerTerm(term int) {
	// exit leader
	rf.nextIndex = nil
	rf.matchIndex = nil

	rf.currentTerm = term
	rf.votedFor = -1
	rf.role = _ROLE_FOLLOWER

	rf.persist()
	DPrintf("STAT\tFollower\tP%d\tT%d", rf.me, rf.currentTerm)
}

// mu.Unlock() handler
// MUST mu.Lock() before call
func (rf *Raft) candidate(beforeTerm int) {
	rf.mu.Lock()
	if rf.currentTerm != beforeTerm ||
		(rf.role != _ROLE_FOLLOWER && rf.role != _ROLE_CANDIDATE) {
		rf.mu.Unlock()
		return
	}
	rf.role = _ROLE_CANDIDATE
	rf.lastHearbeat = time.Now()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	electionTerm := rf.currentTerm
	DPrintf("STAT\tCandidate\tP%d\tT%d", rf.me, rf.currentTerm)
	args := func() (ret RequestVoteArgs) {
		ret.Term = rf.currentTerm
		ret.CandidtateId = rf.me
		ret.LastLogIndex = rf.toAbsIndex(len(rf.log) - 1)
		ret.LastLogTerm = rf.log[rf.toLogIndex(ret.LastLogIndex)].Term
		return
	}()
	rf.mu.Unlock()

	grantedCount := 1
	peerNum := len(rf.peers)
	getMajority := func() bool {
		return 2*grantedCount > peerNum
	}
	type ticket struct {
		int
		bool
	}
	rev := make(chan ticket, peerNum-1)
	wg := sync.WaitGroup{}
	sendRV := func(server int) {
		defer wg.Done()
		reply := RequestVoteReply{}
		ok := rf.sendRequestVote(server, &args, &reply)
		if !ok {
			DPrintf("VOTE\t%d\t%d\tfail send", rf.me, server)
			return
		}
		rev <- ticket{electionTerm, reply.VoteGranted}
	}

	for i := (rf.me + 1) % peerNum; i != rf.me; i = (i + 1) % peerNum {
		wg.Add(1)
		go sendRV(i)
	}
	go func() {
		wg.Wait()
		close(rev)
	}()
	for i := 0; i < peerNum-1; i++ {
		v, ok := <-rev
		if !ok {
			break
		}
		rf.mu.Lock()
		if rf.currentTerm != beforeTerm+1 || rf.role != _ROLE_CANDIDATE {
			rf.mu.Unlock()
			break
		}
		if v.int > rf.currentTerm {
			rf.meetLargerTerm(v.int)
			rf.mu.Unlock()
			break
		}
		if v.bool {
			grantedCount += 1
			if getMajority() {
				DPrintf("VOTE\tP%d\tT%d\tget major", rf.me, rf.currentTerm)
				go rf.leader(rf.currentTerm)
				rf.mu.Unlock()
				break
			}
		}
		rf.mu.Unlock()
	}
}

// mu.Unlock() handler
// MUST mu.Lock() before call and immediatly EXIT the prev handler
func (rf *Raft) leader(beforeTerm int) {
	rf.mu.Lock()
	if rf.currentTerm != beforeTerm || rf.role != _ROLE_CANDIDATE {
		rf.mu.Unlock()
		return
	}
	rf.role = _ROLE_LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.toAbsIndex(len(rf.log))
	}
	leaderTerm := rf.currentTerm
	DPrintf("STAT\tLeader\t\tP%d\tT%d", rf.me, rf.currentTerm)
	rf.mu.Unlock()

	go rf.heartbeatAllWithoutMe(leaderTerm)
	go rf.heartbeatController(leaderTerm)
	servers := len(rf.peers)
	for i := (rf.me + 1) % servers; i != rf.me; i = (i + 1) % servers {
		go rf.commitController(leaderTerm, i)
	}
	go rf.applyChecker(leaderTerm)
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and 350
		// milliseconds.

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		shouldElection := time.Now().After(rf.lastHearbeat.Add(ELECTION_TIMEOUT))
		if shouldElection && rf.role != _ROLE_LEADER {
			go rf.candidate(rf.currentTerm)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for !(rf.lastApplied < rf.commitIndex) {
			rf.applyCond.Wait()
		}
		bufferHead := rf.lastApplied + 1
		newLastApplied := rf.commitIndex
		buffer := make([]entry, newLastApplied+1-bufferHead)
		copy(buffer, rf.log[rf.toLogIndex(bufferHead):rf.toLogIndex(newLastApplied+1)]) //[head, commit]
		rf.mu.Unlock()

		for i := range buffer {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      buffer[i].Command,
				CommandIndex: bufferHead + i,
			}
			rf.applyCh <- msg
		}

		rf.mu.Lock()
		if rf.lastApplied < newLastApplied {
			rf.lastApplied = newLastApplied
		}
		DPrintf("APPL\t%d\t[%d, %d)\t%v", rf.me, bufferHead, bufferHead+len(buffer), buffer)
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeatAllWithoutMe(leaderTerm int) {
	servers := len(rf.peers)
	rev := make(chan int)
	wg := sync.WaitGroup{}
	hearbeat := func(server int, args AppendEntriesArgs) {
		defer wg.Done()
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &args, &reply)
		if !ok {
			return
		}
		rev <- reply.Term
	}
	rf.mu.Lock()
	if rf.currentTerm != leaderTerm || rf.role != _ROLE_LEADER {
		rf.mu.Unlock()
		return
	}
	for i := (rf.me + 1) % servers; i != rf.me; i = (i + 1) % servers {
		args := rf.makeEmptyAppendEntriesArgs(i)
		go hearbeat(i, args)
	}
	wg.Add(servers - 1)
	rf.mu.Unlock()
	go func() {
		wg.Wait()
		close(rev)
	}()
	for {
		term, ok := <-rev
		if !ok {
			break
		}
		if rf.killed() {
			continue
		}
		rf.mu.Lock()
		if rf.currentTerm != leaderTerm || rf.role != _ROLE_LEADER {
			rf.mu.Unlock()
			break
		}
		if term > rf.currentTerm {
			rf.meetLargerTerm(term)
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeatController(leaderTerm int) {
	servers := len(rf.peers)
	rev := make(chan int)
	wg := sync.WaitGroup{}
	produce := sync.WaitGroup{}
	hearbeat := func(server int, args AppendEntriesArgs) {
		defer wg.Done()
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &args, &reply)
		if !ok {
			return
		}
		rev <- reply.Term
	}
	produce.Add(1)
	go func() {
		defer produce.Done()
		i := rf.me
		for !rf.killed() {
			i = (i + 1) % servers
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			if rf.currentTerm != leaderTerm || rf.role != _ROLE_LEADER {
				rf.mu.Unlock()
				return
			}
			args := rf.makeEmptyAppendEntriesArgs(i)
			rf.mu.Unlock()
			wg.Add(1)
			go hearbeat(i, args)
			time.Sleep(HEARTBEAT_INTERVAL)
		}
	}()
	go func() {
		produce.Wait()
		wg.Wait()
		close(rev)
	}()
	for {
		term, ok := <-rev
		if !ok {
			break
		}
		if rf.killed() {
			continue
		}
		rf.mu.Lock()
		if rf.currentTerm != leaderTerm || rf.role != _ROLE_LEADER {
			rf.mu.Unlock()
			break
		}
		if term > rf.currentTerm {
			rf.meetLargerTerm(term)
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
}

// MUST mu.Lock
func (rf *Raft) snapshotHandle(leaderTerm int, server int) {
	rf.mu.Lock()
	if rf.currentTerm != leaderTerm || rf.role != _ROLE_LEADER {
		return
	}
	args := func() (ret InstallSnapshotArgs) {
		ret.Term = leaderTerm
		ret.LeaderId = rf.me
		ret.LastIncludedIndex = rf.lastIncludedIndex
		ret.LastIncludedTerm = rf.log[0].Term
		ret.Data = rf.currentSnapshot
		return
	}()
	newNextIndex := rf.lastIncludedIndex + 1
	DPrintf("INSP\t%d\t%d", rf.me, server)
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if !ok {
		DPrintf("SNAP\t%d\t%d\tFail send", rf.me, server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != leaderTerm || rf.role != _ROLE_LEADER {
		return
	}
	if rf.currentTerm < reply.Term {
		rf.meetLargerTerm(reply.Term)
		return
	}
	// succeed update snapshot then change nextindex
	if rf.nextIndex[server] < newNextIndex {
		DPrintf("INSP\t%d\t%d\tSucceed", rf.me, server)
		rf.nextIndex[server] = newNextIndex
	}
}

// Lock handler
func (rf *Raft) appendHandle(leaderTerm int, server int) {
	// DPrintf("COMM\t%d\t%d\tinit", rf.me, server)
	rf.mu.Lock()
	if rf.currentTerm != leaderTerm || rf.role != _ROLE_LEADER {
		return
	}
	currentNextIdx := rf.nextIndex[server]
	currentLastIdx := rf.toAbsIndex(len(rf.log) - 1)
	args := func() (ret AppendEntriesArgs) {
		ret = rf.makeEmptyAppendEntriesArgs(server)
		// MUST copy, due to the race from RPC to change error log
		begin := rf.toLogIndex(currentNextIdx)
		end := rf.toLogIndex(currentLastIdx) + 1
		ret.Entries = make([]entry, end-begin)
		copy(ret.Entries[:], rf.log[begin:end]) // [) when same
		return
	}()
	DPrintf("COMM\tSR%d\tTA%d\tSZ%d\tprev:{%d,%d}\tlog:%v", rf.me, server,
		len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		DPrintf("COMM\tSR%d\tTA%d\tfail send", rf.me, server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// leader restrain check
	if rf.currentTerm != leaderTerm || rf.role != _ROLE_LEADER {
		return
	}
	// is commited by other thread
	if currentNextIdx != rf.nextIndex[server] {
		return
	}
	// all server
	if reply.Term > rf.currentTerm {
		rf.meetLargerTerm(reply.Term)
		return
	}
	if !reply.Success {
		rf.quickBackup(server, reply.XTerm, reply.XIndex, reply.XLen)
	} else {
		rf.nextIndex[server] = currentLastIdx + 1
		rf.matchIndex[server] = currentLastIdx
		DPrintf("COMM\t%d\t%d\tsucceed with index:%d", rf.me, server, currentLastIdx)
	}
}

func (rf *Raft) commitController(leaderTerm int, server int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != _ROLE_LEADER || rf.currentTerm != leaderTerm {
			rf.mu.Unlock()
			return
		}
		for !(rf.toAbsIndex(len(rf.log)-1) >= rf.nextIndex[server]) {
			rf.commitCond.Wait()
			if rf.role != _ROLE_LEADER || rf.currentTerm != leaderTerm || rf.killed() {
				rf.mu.Unlock()
				return
			}
		}
		if rf.nextIndex[server] <= rf.lastIncludedIndex {
			go rf.snapshotHandle(leaderTerm, server)
		} else {
			go rf.appendHandle(leaderTerm, server)
		}
		rf.mu.Unlock()
		// just send, do not doubt
		time.Sleep(HEARTBEAT_INTERVAL)
	}
}

func (rf *Raft) quickBackup(server int, XTerm, XIndex, XLen int) {
	beforeNextIdx := rf.nextIndex[server]
	// Update new next index
	if rf.nextIndex[server] > XLen {
		// CASE XLen too short
		rf.nextIndex[server] = XLen // refuse 0 XLen
	} else {
		// binsearch to find oooyyyxx(x)zzz
		lastXIndex := func() int {
			l := 0
			r := len(rf.log) - 1
			for l < r {
				mid := (l + r + 1) / 2
				if rf.log[mid].Term <= XTerm {
					l = mid
				} else {
					r = mid - 1
				}
			}
			return rf.toAbsIndex(l)
		}()
		if lastXIndex == rf.lastIncludedIndex || rf.log[rf.toLogIndex(lastXIndex)].Term != XTerm {
			// CASE leader does not have xterm
			rf.nextIndex[server] = XIndex
		} else {
			// CASE leader has xterm
			rf.nextIndex[server] = lastXIndex
		}
	}
	DPrintf("COMM\tP%d\tquickBack peer%d logIdx from %d to %d", rf.me, server, beforeNextIdx, rf.nextIndex[server])
}

func (rf *Raft) applyChecker(leaderTerm int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.currentTerm != leaderTerm || rf.role != _ROLE_LEADER {
			rf.mu.Unlock()
			return
		}
		indexCount := make([]int, len(rf.log))
		for serveri, idx := range rf.matchIndex {
			if serveri == rf.me {
				continue
			}
			if idx > rf.lastIncludedIndex {
				indexCount[rf.toLogIndex(idx)] += 1
			}
		}
		// sum init with leader self
		servers := len(rf.peers)
		nextCommitIdx := rf.toAbsIndex(len(indexCount) - 1)
		for sum := 1; nextCommitIdx > rf.commitIndex; nextCommitIdx -= 1 {
			sum += indexCount[rf.toLogIndex(nextCommitIdx)]
			if 2*sum > servers {
				break
			}
		}
		// not allow update commit index for previous term
		if rf.log[rf.toLogIndex(nextCommitIdx)].Term == rf.currentTerm &&
			nextCommitIdx > rf.commitIndex {
			rf.commitIndex = nextCommitIdx
			rf.applyCond.Broadcast()
			// !!HOT POINT hearbeat to update commitIndex is slow
			go rf.heartbeatAllWithoutMe(leaderTerm)
			DPrintf("COMM\t%d\tupdate", rf.me)
		}
		rf.mu.Unlock()

		time.Sleep(SMALL_INTERVAL)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.currentSnapshot = rf.persister.ReadSnapshot()
	// channel init
	rf.applyCh = applyCh
	// cond init
	rf.commitCond = *sync.NewCond(&rf.mu)
	rf.applyCond = *sync.NewCond(&rf.mu)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}
