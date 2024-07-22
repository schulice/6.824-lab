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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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
	log         []entry

	// all server volatile state
	commitIndex int
	lastApplied int

	// leader volatile
	nextIndex  []int
	matchIndex []int

	// helper
	applyCh         chan ApplyMsg
	currentState    state
	lastHearbeat    time.Time
	electionTimeout time.Duration
	commitCond      sync.Cond
	applyCond       sync.Cond
}

type state int8

const (
	stateFollower     state = 0o0
	stateWaitElection state = 0o1
	stateCandidate    state = 0o2
	stateLeader       state = 0o3
)

func (st *state) toString() string {
	switch *st {
	case stateFollower:
		return "F"
	case stateWaitElection:
		return "W"
	case stateCandidate:
		return "C"
	case stateLeader:
		return "L"
	}
	return "N"
}

const (
	HEARTBEAT_INTER   = 100 * time.Millisecond
	COMMIT_SEND_INTER = 75 * time.Millisecond
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.currentState == stateLeader

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
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
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
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type entry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		reply.Term = 0
		reply.Success = false
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("AE\t%d", rf.me)
	rf.lastHearbeat = time.Now()

	reply.Term = rf.currentTerm
	reply.Success = false

	// all server rule
	if args.Term > rf.currentTerm {
		rf.currentState = stateFollower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		DPrintf("STAT\tFollower\tP%d\tT%d\tAE", rf.me, rf.currentTerm)
	} else if args.Term < rf.currentTerm {
		return
	}

	if rf.currentState == stateCandidate {
		rf.currentState = stateFollower
	}

	prevIdxInside := args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log)
	if !prevIdxInside || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	// update follower log
	// heartbeat will skip this
	for i := range args.Entries {
		logIdx := args.PrevLogIndex + i + 1
		if logIdx >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		} else if rf.log[logIdx].Term != args.Entries[i].Term {
			// before
			rf.log = rf.log[:logIdx]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		// min(lastNewIdx, leardercommit)
		lastNewIdx := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit > lastNewIdx {
			rf.commitIndex = lastNewIdx
		} else {
			rf.commitIndex = args.LeaderCommit
		}
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

	// all server
	if args.Term > rf.currentTerm {
		rf.currentState = stateFollower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		DPrintf("STAT\tFollower\tP%d\tT%d\tRV", rf.me, rf.currentTerm)
	} else if args.Term < rf.currentTerm {
		return
	}

	// currentTerm == args.Term

	if rf.votedFor != -1 {
		return
	}

	lastLog := rf.log[len(rf.log)-1]
	// 1. lastTerm < args.lastTerm
	// 2. lastT == alastT, len(log) <= len(alog)
	if lastLog.Term > args.LastLogTerm || lastLog.Term == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex {
		return
	}

	rf.lastHearbeat = time.Now()

	rf.votedFor = args.CandidtateId
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

	isLeader = rf.currentState == stateLeader
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	DPrintf("CLIE\t%d\t%v", rf.me, command)
	index = len(rf.log)
	term = rf.currentTerm
	e := entry{term, command}
	rf.log = append(rf.log, e)
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// MUST mu.Lock() before call
func (rf *Raft) follower() {
}

// mu.Unlock() handler
// MUST mu.Lock() before call
func (rf *Raft) candidate() {
	rf.currentState = stateCandidate
	rf.lastHearbeat = time.Now()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	electionTerm := rf.currentTerm
	DPrintf("STAT\tCandidate\tP%d\tT%d", rf.me, rf.currentTerm)
	args := func() (ret RequestVoteArgs) {
		ret.Term = rf.currentTerm
		ret.CandidtateId = rf.me
		ret.LastLogIndex = len(rf.log) - 1
		ret.LastLogTerm = rf.log[ret.LastLogIndex].Term
		return
	}()
	rf.mu.Unlock()

	// protected by rf.mu
	grantedCount := 1
	peerNum := len(rf.peers)
	getMajority := func() bool {
		return 2*grantedCount > peerNum
	}
	sendRV := func(server int) {
		reply := RequestVoteReply{}

		ok := rf.sendRequestVote(server, &args, &reply)
		if !ok {
			DPrintf("VOTE\t%d\t%d\tfail sent", rf.me, server)
			return
		}

		rf.mu.Lock()
		if rf.currentTerm != electionTerm || rf.currentState != stateCandidate {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentState = stateFollower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			DPrintf("STAT\tFollower\tP%d\tT%d", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		if reply.VoteGranted {
			grantedCount += 1
			if getMajority() {
				go rf.leader()
				return
			}
		}
		rf.mu.Unlock()
		return
	}

	for i := (rf.me + 1) % peerNum; i != rf.me; i = (i + 1) % peerNum {
		go sendRV(i)
	}
}

func (rf *Raft) heartbeater(leaderTerm int) {
	peerNum := len(rf.peers)
	// lock handler
	perH := func(i int) {
		args := func() (ret AppendEntriesArgs) {
			ret.Term = rf.currentTerm
			ret.LeaderId = rf.me
			ret.LeaderCommit = rf.commitIndex
			ret.PrevLogIndex = rf.nextIndex[i] - 1
			ret.PrevLogTerm = rf.log[ret.PrevLogIndex].Term
			ret.Entries = make([]entry, 0)
			return
		}()
		reply := AppendEntriesReply{}
		// DPrintf("HEAR\t%d\t%d", rf.me, i)
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(i, &args, &reply)
		if !ok {
			DPrintf("HEAR\t%d\t%d\tfail send", rf.me, i)
		}

		rf.mu.Lock()
		if rf.currentTerm != leaderTerm || rf.currentState != stateLeader {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentState = stateFollower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			DPrintf("STAT\tFollower\t%d\t%d\tHEART", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}

	for i := rf.me; !rf.killed(); i = (i + 1) % peerNum {
		rf.mu.Lock()
		if rf.currentTerm != leaderTerm || rf.currentState != stateLeader {
			rf.mu.Unlock()
			return
		}
		go perH(i) // unlock
		time.Sleep(HEARTBEAT_INTER)
	}
}

func (rf *Raft) commitHandler(leaderTerm int, server int) {
	DPrintf("COMM\t%d\t%d\tinit", rf.me, server)
	args := func() (ret AppendEntriesArgs) {
		ret.Term = rf.currentTerm
		ret.LeaderId = rf.me
		ret.LeaderCommit = rf.commitIndex
		ret.PrevLogIndex = rf.nextIndex[server] - 1
		ret.PrevLogTerm = rf.log[ret.PrevLogIndex].Term
		ret.Entries = rf.log[rf.nextIndex[server]:len(rf.log)] // [) when same
		return
	}()
	reply := AppendEntriesReply{}
	commitedIdx := len(rf.log) - 1
	currentNextIdx := rf.nextIndex[server]
	DPrintf("COMM\t%d\t%d", rf.me, server)
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		DPrintf("COMM\t%d\t%d\tfail send", rf.me, server)
		return
	}

	rf.mu.Lock()
	// leader restrain check
	if rf.currentTerm != leaderTerm || rf.currentState != stateLeader {
		rf.mu.Unlock()
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentState = stateFollower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		DPrintf("STAT\tFollower\t%d\t%dCOMM", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		return
	}
	// is commited by some thread
	if currentNextIdx != rf.nextIndex[server] {
		rf.mu.Unlock()
		return
	}
	if reply.Success {
		rf.nextIndex[server] = commitedIdx + 1
		rf.matchIndex[server] = commitedIdx
		DPrintf("COMM\t%d\t%d\tsucceed", rf.me, server)
		rf.mu.Unlock()
		return
	} else {
		DPrintf("COMM\t%d\t%d\tdecrease", rf.me, server)
		rf.nextIndex[server] -= 1
		if rf.nextIndex[server] == 0 {
			DPrintf("COMM\t%d\t%d\tERROR negtive nexidx", rf.me, server)
		}
		go rf.commitHandler(leaderTerm, server)
	}
}

func (rf *Raft) commitProducer(leaderTerm int, server int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.currentState != stateLeader || rf.currentTerm != leaderTerm {
			rf.mu.Unlock()
			return
		}
		for !(len(rf.log) >= rf.nextIndex[server]) {
			rf.commitCond.Wait()
			if rf.currentState != stateLeader || rf.currentTerm != leaderTerm {
				rf.mu.Unlock()
				return
			}
		}

		go rf.commitHandler(leaderTerm, server)

		time.Sleep(COMMIT_SEND_INTER)
	}
}

func (rf *Raft) commitUpdater(leaderTerm int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.currentTerm != leaderTerm || rf.currentState != stateLeader {
			rf.mu.Unlock()
			return
		}
		checker := make([]int, len(rf.log))
		for id, i := range rf.matchIndex {
			if id == rf.me {
				continue
			}
			checker[i] += 1
		}
		nextCommitIdx := len(checker) - 1
		peerNum := len(rf.peers)
		// sum init with leader
		for sum := 1; nextCommitIdx > 0; nextCommitIdx -= 1 {
			sum += checker[nextCommitIdx]
			if 2*sum > peerNum {
				break
			}
		}
		if nextCommitIdx > rf.commitIndex {
			rf.commitIndex = nextCommitIdx
			rf.applyCond.Broadcast()
			DPrintf("COMM\t%d\tupdate", rf.me)
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

// mu.Unlock() handler
// MUST mu.Lock() before call and immediatly EXIT the prev handler
func (rf *Raft) leader() {
	rf.currentState = stateLeader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	leaderTerm := rf.currentTerm
	DPrintf("STAT\tLeader\t\tP%d\tT%d", rf.me, rf.currentTerm)
	rf.mu.Unlock()

	go rf.heartbeater(leaderTerm)

	peerNum := len(rf.peers)
	for i := (rf.me + 1) % peerNum; i != rf.me; i = (i + 1) % peerNum {
		go rf.commitProducer(leaderTerm, i)
	}

	go rf.commitUpdater(leaderTerm)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and 350
		// milliseconds.

		rf.mu.Lock()
		shouldElection := time.Now().After(rf.lastHearbeat.Add(rf.electionTimeout))

		if shouldElection {
			go rf.candidate()
		} else {
			rf.mu.Unlock()
		}

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyer() {
	for !rf.killed() {
		rf.mu.Lock()
		for !(rf.lastApplied < rf.commitIndex) {
			rf.applyCond.Wait()
		}
		bufferHead := rf.lastApplied + 1
		buffer := rf.log[bufferHead : rf.commitIndex+1]
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		for i := range buffer {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      buffer[i].Command,
				CommandIndex: bufferHead + i,
			}
			rf.applyCh <- msg
		}
		DPrintf("APPL\t%d\t[%d, %d)", rf.me, bufferHead, bufferHead+len(buffer))
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// channel init
	rf.applyCh = applyCh
	rf.commitCond = *sync.NewCond(&rf.mu)
	rf.applyCond = *sync.NewCond(&rf.mu)
	ms := 500 + (rand.Int63() % 500)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond // 1-2s

	// 3A no persistent so init
	rf.log = make([]entry, 1)

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyer()

	return rf
}
