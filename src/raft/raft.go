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
	"sync"
	"labrpc"
	"time"
	"math/rand"
	"bytes"
	"encoding/gob"
	"runtime"
	"fmt"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command     interface{}
	Term int
	Index int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	log []LogEntry

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

	applyCh chan ApplyMsg
	quitChannel chan struct{}

	state string

	heartbeatChannel chan bool
	votingChannel chan bool
	becomeLeaderChannel chan bool
	commitChannel chan bool
	

	votesGranted int

	leaderId int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	NextIndex int
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data[] byte
}

type InstallSnapshotReply struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == "leader"
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) getLastIndex() int{
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastTerm() int{
	return rf.log[len(rf.log)-1].Term
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.quitChannel)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == "leader"
	if isLeader {
		index = rf.getLastIndex() + 1
		var entry LogEntry
		entry.Term = term;
		entry.Command = command;
		entry.Index = index;

		rf.log = append(rf.log, entry)

		rf.persist()
	}

	return index, term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok{
		if rf.state != "candidate" || args.Term != rf.currentTerm{
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.state = "follower"
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.leaderId = -1
			rf.votesGranted = 1

			rf.persist()

		}

		if rf.state == "candidate"{
			if reply.VoteGranted{
				rf.votesGranted += 1
			}

			if rf.votesGranted > (len(rf.peers)/2){
				rf.becomeLeaderChannel <- true
			}
		}
	}

	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = "follower"
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = -1
		rf.votesGranted = 1

		rf.persist()

	}
	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId{
		if args.LastLogTerm > rf.getLastTerm() || (args.LastLogTerm == rf.getLastTerm() && args.LastLogIndex >= rf.getLastIndex()){
			rf.votingChannel <- true

			rf.state = "follower"
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.leaderId = -1
			rf.votesGranted = 1

			rf.persist()

			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.state != "leader" || args.Term != rf.currentTerm{
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.state = "follower"
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.leaderId = -1
			rf.votesGranted = 1

			rf.persist()
			return ok
		}

		if reply.Success {
			if len(args.Entries) > 0{
				rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = reply.NextIndex - 1
			}
		}else{
			rf.nextIndex[server] = reply.NextIndex
		}
	}

	return ok
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.NextIndex = 0

	if args.Term < rf.currentTerm {
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	rf.heartbeatChannel <- true

	if args.Term > rf.currentTerm {
		rf.state = "follower"
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = args.LeaderId
		rf.votesGranted = 1

		rf.persist()
	}
	reply.Term = rf.currentTerm

	// Reply false if log doesn’t contain an entry at prevLogIndex
	if args.PrevLogIndex > rf.getLastIndex() {
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}
	baseIndex := rf.log[0].Index
	if args.PrevLogIndex > baseIndex && rf.log[args.PrevLogIndex-baseIndex].Term != args.PrevLogTerm{
		reply.NextIndex = args.PrevLogIndex
		for i := len(rf.log)-1; i>=0; i--{
			if rf.log[args.PrevLogIndex-baseIndex].Term == rf.log[i].Term{
				reply.NextIndex = rf.log[i].Index
			}else{
				break
			}
		}
		return
	}

	if args.PrevLogIndex >= baseIndex{
		tmp := rf.log[:args.PrevLogIndex+1-baseIndex]
		tmp = append(tmp, args.Entries...)
		rf.log = tmp

		rf.state = "follower"
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = args.LeaderId
		rf.votesGranted = 1

		rf.persist()

		reply.Success = true
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
	}else {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.log[0].Index + 1
	}

	if args.LeaderCommit > rf.commitIndex {
		last := rf.getLastIndex()
		if args.LeaderCommit > last{
			rf.commitIndex = last
		}else{
			rf.commitIndex = args.LeaderCommit
		}

		rf.commitChannel <- true
	}
}

func (rf *Raft) CreateSnapshot(data []byte, appliedIndex int){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Println("createsnapshot called on server: ", rf.me)

	baseIndex := rf.log[0].Index
	if appliedIndex <= baseIndex || appliedIndex > rf.getLastIndex(){
		if appliedIndex > rf.log[len(rf.log)-1].Index {
			//fmt.Println("\tcreatesnapshot failed on server: ", rf.me, " reason: appliedIndex (", appliedIndex, ") > rf.log[len(rf.log)-1].Index(", rf.log[len(rf.log)-1].Index, ")")
		} else {
			//fmt.Println("\tcreatesnapshot failed on server: ", rf.me, " reason: appliedIndex (", appliedIndex, ") <= initialIndex (", baseIndex, ")")
		}

		return
	}

	var retainLog []LogEntry
	var logEntry LogEntry
	logEntry.Index = appliedIndex
	logEntry.Term = rf.log[appliedIndex-baseIndex].Term
	retainLog = append(retainLog, logEntry)
	
	//fmt.Println("\tcommitsnapshot server ", rf.me, " appliedIndex: ", appliedIndex)
	//fmt.Println("\tcommitsnapshot server ", rf.me, " rf.log: ", rf.log)

	for i:=appliedIndex+1; i<=rf.getLastIndex(); i++{
		retainLog = append(retainLog,rf.log[i-baseIndex])
	}

	//fmt.Println("\tcommitsnapshot server ", rf.me, " thisLog: ", retainLog)

	//fmt.Println("\tcommitsnapshot server ", rf.me, " rf.log size before: ", len(rf.log))

	rf.log = retainLog

	//fmt.Println("\tcommitsnapshot server ", rf.me, " rf.log size after: ", len(rf.log))

	// persist log up to appliedIndex
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.log[0].Index)
	e.Encode(rf.log[0].Term)

	// do not e.Encode(snapshot), because snapshot already encoded
	snapshot := w.Bytes()
	snapshot = append(snapshot, data...)
	rf.persister.SaveSnapshot(snapshot)

	rf.persist()
}

func (rf *Raft) readSnapshot(data []byte) {
	if len(data) > 0 {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)

		var lastCommitIndex int
		var lastCommitTerm int
		d.Decode(&lastCommitIndex)
		d.Decode(&lastCommitTerm)

		rf.commitIndex = lastCommitIndex
		rf.lastApplied = lastCommitIndex

		var retainLog []LogEntry
		var logEntry LogEntry
		logEntry.Index = lastCommitIndex
		logEntry.Term = lastCommitTerm
		retainLog = append(retainLog, logEntry)
		for i := 0; i < len(rf.log); i++ {
			if rf.log[i].Index == lastCommitIndex && rf.log[i].Term == lastCommitTerm {
				retainLog = append(retainLog, rf.log[i+1:]...)
				break
			}
		}

		rf.log = retainLog

		go func() {
			var applyMsg ApplyMsg
			applyMsg.UseSnapshot = true
			applyMsg.Snapshot = data
			rf.applyCh <- applyMsg
		}()
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	//fmt.Println("sendInstallSnapshot called on server: ", server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.currentTerm < reply.Term {
			rf.state = "follower"
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.leaderId = -1
			rf.votesGranted = 1

			rf.persist()

			return ok
		}

		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}

	return ok
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Println("InstallSnapshot called on server: ", rf.me)

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	rf.heartbeatChannel <- true

	if args.Term > rf.currentTerm {
		rf.state = "follower"
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = args.LeaderId
		rf.votesGranted = 1

		rf.persist()
	}

	reply.Term = rf.currentTerm

	//fmt.Println("installsnapshot, server: ", rf.me, " args.data: ", args.Data)

	rf.persister.SaveSnapshot(args.Data)

	//fmt.Println("installsnapshot server ", rf.me, " rf.log before: ", rf.log)

	//fmt.Println("installsnapshot server ", rf.me, "  size before: ", len(rf.log))

	var retainLog []LogEntry
	var logEntry LogEntry
	logEntry.Index = args.LastIncludedIndex
	logEntry.Term = args.LastIncludedTerm
	retainLog = append(retainLog, logEntry)
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Index == args.LastIncludedIndex && rf.log[i].Term == args.LastIncludedTerm {
			retainLog = append(retainLog, rf.log[i+1:]...)
			break
		}
	}

	rf.log = retainLog

	//fmt.Println("installsnapshot server ", rf.me, " this log: ", rf.log)
	//fmt.Println("installsnapshot server ", rf.me, "  size after: ", len(rf.log))

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.persist()

	//fmt.Println("installsnapshot applymsg: ", ApplyMsg{UseSnapshot:true,Snapshot:args.Data})

	var applyMsg ApplyMsg
	applyMsg.UseSnapshot = true
	applyMsg.Snapshot = args.Data

	rf.applyCh <- applyMsg

}

func (rf *Raft) followerStateChecker(){
	for {
		select {
		case <- rf.quitChannel:
			return
		default:
			rf.mu.Lock()
			curState := rf.state
			rf.mu.Unlock()
			if curState == "follower"{
				select {
				case <- rf.heartbeatChannel:
				case <- rf.votingChannel:
				case <- time.After(time.Duration(rand.Int63()%333+550)*time.Millisecond):
					rf.mu.Lock()
					rf.state = "candidate"
					rf.mu.Unlock()
				}
			}else{
				runtime.Gosched()
			}
		}
	}
}

func (rf *Raft) candidateStateChecker(){
	for {
		select {
		case <- rf.quitChannel:
			return
		default:
			rf.mu.Lock()
			curState := rf.state
			rf.mu.Unlock()
			if curState == "candidate"{
				rf.mu.Lock()
				
				rf.state = "candidate"
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.votesGranted = 1
				rf.leaderId = -1

				rf.persist()

				var args RequestVoteArgs

				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				args.LastLogIndex = rf.getLastIndex()
				args.LastLogTerm = rf.getLastTerm()

				for i :=0; i<len(rf.peers); i++{
					if i != rf.me && rf.state == "candidate"{
						go func(i int){
							var reply RequestVoteReply
							rf.sendRequestVote(i, args, &reply)
						}(i)
					}
				}

				rf.mu.Unlock()

				select {
				case <- rf.heartbeatChannel:
					rf.mu.Lock()
					rf.state = "follower"
					rf.mu.Unlock()
				case <- rf.becomeLeaderChannel:
					rf.mu.Lock()
					
					rf.state = "leader"
					rf.leaderId = rf.me

					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					index := rf.getLastIndex() + 1
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = index
						rf.matchIndex[i] = rf.log[0].Index
					}

					rf.mu.Unlock()
				case <- time.After(time.Duration(rand.Int63()%333+550)*time.Millisecond):
				}
			}else{
				runtime.Gosched()
			}
		}
	}
}

func (rf *Raft) leaderStateChecker(){
	for {
		select {
		case <- rf.quitChannel:
			return
		default:
			rf.mu.Lock()
			curState := rf.state
			rf.mu.Unlock()
			if curState == "leader"{
				
				rf.mu.Lock()

				if rf.state == "leader" {
					N := rf.commitIndex
					last := rf.getLastIndex()
					baseIndex := rf.log[0].Index
					for i := rf.commitIndex + 1; i <= last; i++ {
						num := 1
						for j := range rf.peers {
							if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-baseIndex].Term == rf.currentTerm {
								num++
							}
						}
						if 2*num > len(rf.peers) {
							N = i
						}
					}
					if N != rf.commitIndex {
						rf.commitIndex = N
						rf.commitChannel <- true
					}
				}

				for i := range rf.peers {
					if i != rf.me && rf.state == "leader"{
						baseIndex := rf.log[0].Index
						if rf.nextIndex[i] <= baseIndex {

							var args InstallSnapshotArgs
							args.Term = rf.currentTerm
							args.LeaderId = rf.me
							args.Data = rf.persister.snapshot
							args.LastIncludedIndex = rf.log[0].Index
							args.LastIncludedTerm = rf.log[0].Term

							go func(i int){
								var reply InstallSnapshotReply
								rf.sendInstallSnapshot(i, args, &reply)
							}(i)
						}else{
							var args AppendEntriesArgs
							args.LeaderCommit = rf.commitIndex
							args.LeaderId = rf.me
							args.PrevLogIndex = rf.nextIndex[i]-1
							args.Term = rf.currentTerm
							args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
							args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1-baseIndex:]))
							copy(args.Entries, rf.log[args.PrevLogIndex+1-baseIndex:])

							go func(i int){
								var reply AppendEntriesReply
								rf.sendAppendEntries(i, args, &reply)
							}(i)
						}
					}
				}

				rf.mu.Unlock()

				time.Sleep(50*time.Millisecond)
			} else{
				runtime.Gosched()
			}
		}
	}
}

func (rf *Raft) commitChecker(){
	for {
		select {
		case <- rf.quitChannel:
			return
		case <- rf.commitChannel:
			rf.mu.Lock()
			baseIndex := rf.log[0].Index
			for i := rf.lastApplied+1; i<=rf.commitIndex && i <= rf.getLastIndex(); i++{
				var applyMsg ApplyMsg
				applyMsg.Index = i
				applyMsg.Command = rf.log[i-baseIndex].Command

				rf.applyCh <- applyMsg
				rf.lastApplied = i
			}
			rf.mu.Unlock()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	if false {
		fmt.Println("filler")
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votesGranted = 1
	rf.state = "follower"
	rf.leaderId = -1
	var dummyLog LogEntry
	dummyLog.Index = 0
	dummyLog.Term = 0
	rf.log = append(rf.log, dummyLog) //use null first entry to align with paper's 1 indexing

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.quitChannel = make(chan struct{})
	rf.heartbeatChannel = make(chan bool,100)
	rf.votingChannel = make(chan bool,100)
	rf.becomeLeaderChannel = make(chan bool,100)
	rf.commitChannel = make(chan bool,100)

	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	go rf.followerStateChecker()
	go rf.candidateStateChecker()
	go rf.leaderStateChecker()

	go rf.commitChecker()

	return rf
}