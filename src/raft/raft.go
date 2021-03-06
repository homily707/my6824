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
	"log"
	"math/rand"
	"sort"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index int
	Term  int
	Data  interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        *sync.Mutex         // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyChan chan ApplyMsg
	//logChan   chan LogEntry

	// Your data here (2A, 2B, 2C).
	// A
	role          Role
	currentTerm   int
	votedFor      int
	leader        int
	voteChan      chan *RequestVoteReply
	beatsInterval int
	beatsCheck    time.Time
	beatsLast     time.Time

	// B
	logs         []LogEntry
	commitIndex  int
	lastApplied  int
	nextLogIndex int
	nextIndexs   []int
	matchIndexs  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//rf.Logf("get state %v", rf.role)
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// A
	Term        int
	CandidateId int
	// B
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	VotedFor    int
}

type AppendEntriesArgs struct {
	// 2A
	Term     int
	LeaderId int
	// B
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) sendBeats() {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		term := rf.currentTerm
		rf.mu.Unlock()
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		rf.LogKey("re", "beats on")
		rf.sendAppendEntries(i, "Raft.Beats", args, reply)
		if !reply.Success && reply.Term > term {
			rf.mu.Lock()
			rf.LogfKey("beats", "leader out of time %v", reply.Term)
			rf.role = Follower
			rf.currentTerm = reply.Term
			rf.mu.Unlock()
			break
		}
	}
	//rf.Log("beats off")

}

func (rf *Raft) sendElection(args *RequestVoteArgs, index int) {
	reply := &RequestVoteReply{}
	rf.sendRequestVote(index, args, reply)
	rf.voteChan <- reply
}

func (rf *Raft) election() {
	rf.mu.Lock()
	rf.role = Candidate
	rf.leader = -1
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.voteChan = make(chan *RequestVoteReply, len(rf.peers)-1)
	rf.LogfKey("re", " candidate for term %v", rf.currentTerm)
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i != rf.me {
			go rf.sendElection(args, i)
		}
	}

	go rf.reElection()

	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	replys := 0
	myvotes := 1
	for replys < len(rf.peers)-1 && myvotes*2 < len(rf.peers)+1 {
		reply := <-rf.voteChan
		rf.LogfKey("re", " reply vote granted %v, for %v, term %v", reply.VoteGranted, reply.VotedFor, reply.Term)
		replys++
		if reply.VoteGranted && reply.VotedFor == rf.me {
			myvotes++
		}
	}

	rf.LogfKey("re", " get reply %v, votes %v", replys, myvotes)
	// self select
	rf.mu.Lock()
	checkterm := rf.currentTerm
	rf.mu.Unlock()
	if myvotes*2 >= len(rf.peers)+1 && term == checkterm {
		rf.mu.Lock()
		rf.role = Leader
		rf.leader = rf.me
		rf.votedFor = -1
		rf.mu.Unlock()
		rf.LogKey("re", " i'm leader ")
	}
}

func (rf *Raft) reElection() {

	dur := time.Duration(500+rand.Intn(200)) * time.Millisecond
	time.Sleep(dur)
	if rf.leader < 0 {
		rf.LogfKey("re", "having wait %v ,now reElection", dur)
		rf.election()
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// A
	// stale candidate
	rf.LogfKey("re", " receive requestVote: candidate %v, term %v", args.CandidateId, args.Term)
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	if args.Term < term {
		reply.Term = term
		reply.VoteGranted = false
		return
	} else if args.Term > term {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.role = Follower
		rf.mu.Unlock()
		reply.Term = args.Term
		reply.VoteGranted = true
		reply.VotedFor = args.CandidateId
	} else {
		reply.Term = term
		reply.VoteGranted = true
		reply.VotedFor = rf.votedFor
	}
	rf.LogfKey("re", " vote to :  %v, term %v", reply.VotedFor, reply.Term)
}

func (rf *Raft) Beats(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// A
	rf.mu.Lock()
	term := rf.currentTerm
	reply.Term = term
	rf.mu.Unlock()
	rf.LogfKey("re", "hear beats from %v, term %v", args.LeaderId, args.Term)
	if args.Term < term {
		reply.Term = term
		reply.Success = false
		return
	}
	rf.mu.Lock()
	rf.beatsLast = time.Now()
	rf.role = Follower
	rf.leader = args.LeaderId
	rf.votedFor = -1
	rf.currentTerm = args.Term
	DmePrintf(rf.me, "hear beats")
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs))
		DmePrintf(rf.me, "hear beats update commit ")
		go rf.Apply()
	}
	rf.mu.Unlock()
	reply.Success = true
	return
}

func (rf *Raft) LogEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	reply.Term = term
	//rule 1
	if args.Term < term {
		DPrintf("fail for term")
		reply.Success = false
		return
	}
	//rule 2
	if rf.logs[len(rf.logs)-1].Index < args.PrevLogIndex {
		DPrintf("index not found,my last is %v", rf.logs[len(rf.logs)-1].Index)
		reply.Success = false
		return
	}
	//rule 3
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DmePrintf(rf.me, "index %v ok term %v not", args.PrevLogIndex, args.PrevLogTerm)
		rf.logs = rf.logs[0:args.PrevLogIndex]
		reply.Success = false
		return
	}
	if args.Entries == nil {
		DmePrintf(rf.me, "check confirm index %v term %v ", args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = true
		return
	}

	//rule 4
	//Dprintf( "reach rule 4")
	rf.logs = append(rf.logs[0:args.PrevLogIndex+1], args.Entries...)
	//Dprintf( "reach rule 5")
	//rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs))
	}
	go rf.Apply()
	//DmePrintf(rf.me, "accept log,my commit is %v", rf.commitIndex)
	//if len(rf.logs)%5 == 0 {
	//	logs := rf.logs
	//	builder := strings.Builder{}
	//	builder.WriteString("{ ")
	//	for _, v := range logs {
	//		builder.WriteString(fmt.Sprintf("%v/%v ", v.Term, v.Index))
	//	}
	//	builder.WriteString(" }")
	//	DmePrintf(rf.me, "log is %v", builder.String())
	//}
	reply.Success = true
	// rf.LogKey("log", "ok ok")
	return
}

func min(x int, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, method string, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call(method, args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	rf.mu.Lock()
	isLeader := rf.role == Leader
	rf.mu.Unlock()
	// Your code here (2B).
	if !isLeader {
		return index, term, isLeader
	}

	DmePrintf(rf.me, "receive a command")
	index, term = rf.handleCommand(command)

	return index, term, isLeader
}

func (rf *Raft) handleCommand(command interface{}) (int, int) {
	rf.mu.Lock()
	entry := LogEntry{
		Index: rf.nextLogIndex,
		Term:  rf.currentTerm,
		Data:  command,
	}
	DmePrintf(rf.me, "new log term/log %v/%v", rf.currentTerm, rf.nextLogIndex)
	rf.logs = append(rf.logs, entry)
	rf.nextLogIndex++
	//rf.LogKey("log", "reach here")
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendLog(i)
	}
	return entry.Index, entry.Term
}

func (rf *Raft) sendLog(i int) {
	// sync nextIndex first
	for {
		rf.mu.Lock()
		nextIndex := rf.nextIndexs[i]
		entry := rf.logs[nextIndex-1]
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogTerm:  entry.Term,
			PrevLogIndex: entry.Index,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		reply := &AppendEntriesReply{}
		//DmePrintf(rf.me, "sending entry from [%v, %v) to %v.(prev term/index %v/%v) ",
		//	nextIndex, rf.nextLogIndex, i, args.PrevLogTerm, args.PrevLogIndex)
		rf.sendAppendEntries(i, "Raft.LogEntries", args, reply)
		if reply.Success {
			DmePrintf(rf.me, "check index %v success", nextIndex)
			break
		} else {
			// TODO bisect to accelerate
			DmePrintf(rf.me, "check index %v failed", nextIndex)
			rf.nextIndexs[i]--
		}
	}

	for {
		rf.mu.Lock()
		nextIndex := rf.nextIndexs[i]
		entries := rf.logs[nextIndex:rf.nextLogIndex]
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogTerm:  0,
			PrevLogIndex: 0,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		if nextIndex > 0 {
			args.PrevLogTerm = rf.logs[nextIndex-1].Term
			args.PrevLogIndex = rf.logs[nextIndex-1].Index
		}
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		DmePrintf(rf.me, "sending entry from [%v, %v) to %v.(prev term/index %v/%v) ",
			nextIndex, rf.nextLogIndex, i, args.PrevLogTerm, args.PrevLogIndex)
		rf.sendAppendEntries(i, "Raft.LogEntries", args, reply)
		if reply.Success {
			rf.mu.Lock()
			rf.nextIndexs[i] = rf.nextLogIndex
			rf.matchIndexs[i] = entries[len(entries)-1].Index
			DmePrintf(rf.me, "success,update %v next to %v, match to %v", i, rf.nextIndexs[i], rf.matchIndexs[i])
			rf.mu.Unlock()
			go rf.Commit()
			break
		} else {
			// TODO bisect to accelerate
			rf.nextIndexs[i]--
		}
	}
}

func (rf *Raft) Commit() {
	matchIndexs := make([]int, len(rf.peers))
	rf.mu.Lock()
	//DmePrintf(rf.me, "check commit")
	copy(matchIndexs, rf.matchIndexs)
	sort.Ints(matchIndexs)
	halfMatch := matchIndexs[len(rf.peers)/2]

	if halfMatch > rf.commitIndex {
		//DmePrintf(rf.me, "half match %v beyond commit %v", halfMatch, rf.commitIndex)
		rf.commitIndex = halfMatch
		rf.Apply()
	}
	rf.mu.Unlock()
}

func (rf *Raft) Apply() {
	commitIndex := rf.commitIndex
	//DmePrintf(rf.me, "commit is %v,apply is %v", commitIndex, rf.lastApplied)
	for i := rf.lastApplied + 1; i <= commitIndex; i++ {
		log := rf.logs[i]
		rf.applyChan <- ApplyMsg{
			CommandValid:  true,
			Command:       log.Data,
			CommandIndex:  log.Index,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		DmePrintf(rf.me, "applymsg %v", i)
	}
	rf.lastApplied = commitIndex
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()
		switch role {
		case Leader:
			go rf.sendBeats()
			//go rf.Commit()
			dur := time.Millisecond * time.Duration(rf.beatsInterval)
			rf.LogfKey("beats", "leader sleep %v", dur)
			time.Sleep(dur)
		case Follower:
			rf.mu.Lock()
			if rf.votedFor != -1 {
				continue
			}
			rf.mu.Unlock()
			dur := time.Millisecond * time.Duration(250+rand.Intn(500))
			rf.Logf("follower sleep %v", dur)
			time.Sleep(dur)
			// rf.Logf("lastbeats before check %v", rf.beatsLast.Before(rf.beatsCheck))
			if rf.leader == -1 || rf.beatsLast.Before(rf.beatsCheck) {
				// rf.Log("no beats, go election")
				rf.election()
			}
			rf.beatsCheck = time.Now()
		}

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	log.SetFlags(log.Lmicroseconds)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = &sync.Mutex{}
	rf.applyChan = applyCh

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.role = Follower
	rf.beatsInterval = 200
	rf.leader = -1
	rf.votedFor = -1

	// 2B
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndexs = make([]int, len(rf.peers))
	rf.matchIndexs = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndexs[i] = 1
	}
	rf.nextLogIndex = 1

	rf.logs = []LogEntry{{
		Index: 0,
		Term:  0,
		Data:  nil,
	}}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf Raft) Logf(format string, args ...interface{}) {
	//RaftPrintf(rf.me, rf.role, rf.currentTerm, rf.leader, format, args...)
}

func (rf Raft) Log(format string) {
	//RaftPrint(rf.me, rf.role, rf.currentTerm, rf.leader, format)
}

func (rf Raft) LogfKey(key string, format string, args ...interface{}) {
	//RaftPrintfWithKey(rf, key, format, args...)
}

func (rf Raft) LogKey(key string, str string) {
	//RaftPrintWithKey(rf, key, str)
}
