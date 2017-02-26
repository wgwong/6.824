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
	//"fmt"
)

// import "bytes"
// import "encoding/gob"



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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).

	state string;
	currentTerm int
	votedFor int;
	voteCount int;
	//log [] LogEntry;
	timeoutTime int;

	heartbeatChannel chan bool
	//votingChannel chan bool
	electedChannel chan bool

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock();
	term = (*rf).currentTerm;
	isleader = (*rf).state == "leader";
	rf.mu.Unlock();
	return term, isleader;
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
	Entries[] string
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	////fmt.Println(args.CandidateId, " requestvote called on raft id: ", rf.me);
	////fmt.Println("\tstate: ", rf.state);
	////fmt.Println("\tvotedFor: ", rf.votedFor);
	////fmt.Println("\tcurrentTerm: ", rf.currentTerm);

	rf.mu.Lock()
	rfState := rf.state;
	rfTerm := rf.currentTerm;
	rfVotedFor := rf.votedFor;
	rf.mu.Unlock();

	// Your code here (2A, 2B).
	if rfState == "leader" {
		reply.Term = rfTerm;
		reply.VoteGranted = false;
		////fmt.Println("\t", rf.me, " already leader [reject]");
		return;
	}

	if rfTerm > args.Term {
		reply.Term = rfTerm;
		reply.VoteGranted = false;
		////fmt.Println("\t", rf.me, " term higher than candidate [reject]");
		return;
	}

	if rfTerm == args.Term && rfVotedFor!= -1 {
		//&&rf.votedFor != args.CandidateId
		reply.Term = rfTerm;
		reply.VoteGranted = false;

		////fmt.Println("\t", rf.me, " already voted for ", rf.votedFor ," [reject]");
		return;
	}

	if rfTerm < args.Term { //self is not up to date, convert to follower if not already follower
		////fmt.Println("\t", rf.me, " term out of date, convert to follower, previous: ", rf.state);
		rf.mu.Lock();
		rf.state = "follower";
		rf.currentTerm = args.Term;
		rf.mu.Unlock();
	}

	rf.mu.Lock();
	rf.votedFor = args.CandidateId;
	rf.state = "follower";
	rf.currentTerm = args.Term;
	rfTerm = rf.currentTerm;
	rf.mu.Unlock();
	reply.Term = rfTerm;
	reply.VoteGranted = true;
	////fmt.Println("\t", rf.me, " votes for ", rf.votedFor, ". currentTerm: ", rf.currentTerm);
	return;

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock();
	rf.heartbeatChannel <- true;
	rfTerm := rf.currentTerm;
	rf.mu.Unlock();
	if rfTerm != args.Term {
		if rfTerm > args.Term {
			reply.Term = rfTerm;
			reply.Success = false;
			return;
		} else { // leader has higher term, squash any potential candidates/leaders
			rf.mu.Lock();
			rf.currentTerm = args.Term;
			rf.state = "follower";
			rf.votedFor = -1;
			rf.mu.Unlock();
		}
	}
	//otherwise, args.Term == rf.currentTerm
	reply.Term = rfTerm;
	rf.mu.Lock();
	//TODO, append logs
	rf.mu.Unlock();
	reply.Success = true;
	return;
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply);

	////fmt.Println(rf.me, " sendRequestVote: called ", server);
	////fmt.Println("\targs| term: ", args.Term, ", CandidateId: ", args.CandidateId);
	////fmt.Println("\treply| term:  ", reply.Term, ", votedGranted: ", reply.VoteGranted);

	if ok {
		rf.mu.Lock();
		rfState := rf.state;
		rfTerm := rf.currentTerm;
		rf.mu.Unlock();

		if rfState != "candidate" {
			return ok;
		}
		if args.Term != rfTerm {
			return ok;
		}
		if reply.Term > rfTerm { //leader out of date, demoted to follower
			rf.mu.Lock();
			rf.currentTerm = reply.Term
			rf.state = "follower";
			rf.votedFor = -1;
			////fmt.Println("\tleader out of date, demoted to follower");
			rf.mu.Unlock();
		}
		if reply.VoteGranted { //vote was received
			rf.mu.Lock();
			rf.voteCount = rf.voteCount + 1;
			////fmt.Println("\tvote granted, vote count now: ", rf.voteCount);
			if rf.state == "candidate" && rf.voteCount > len(rf.peers)/2 {
				rf.electedChannel <- true
				////fmt.Println("\telectedChannel true. ", rf.voteCount, " > ", len(rf.peers)/2);
			}
			rf.mu.Unlock();
		}
	}

	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//leader heartbeat receiver function
func stateChecker(rf *Raft) { //appendLogChannel chan AppendEntriesArgs
	timeToLive := time.Millisecond * time.Duration(rf.timeoutTime);
	heartbeatInterval := 10 * time.Millisecond;

	for {
		rf.mu.Lock();
		curState := rf.state;
		rf.mu.Unlock();
		if curState == "follower" {
			select {
			case <- rf.heartbeatChannel:
				//////fmt.Println("found heartbeat");
			case <- time.After(timeToLive):
				////fmt.Println(rf.me, " no heartbeat, become candidate");
				rf.mu.Lock();
				rf.state = "candidate"; //no heartbeat, become candidate
				rf.mu.Unlock();
			}
		} else if curState == "leader" {
				//send heartbeat to everyone else
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						var appendEntriesArgs AppendEntriesArgs;
						appendEntriesArgs.Term = rf.currentTerm;
						appendEntriesArgs.LeaderId = rf.me;
						//prevLogIndex int
						//prevLogTerm int
						//entries[] string
						//leaderCommit int

						//send append entries
						go func(peerIndex int, appendEntriesArgs AppendEntriesArgs) {
							var appendEntriesReply AppendEntriesReply;
							ok := rf.peers[peerIndex].Call("Raft.AppendEntries", &appendEntriesArgs, &appendEntriesReply);
							rf.mu.Lock();
							rfTerm := rf.currentTerm;
							rf.mu.Unlock();
							if ok {
								if !appendEntriesReply.Success {
									if appendEntriesReply.Term > rfTerm { //leader is not up to date, demote to follower
										rf.mu.Lock();
										rf.currentTerm = appendEntriesReply.Term;
										rf.state = "follower";
										rf.votedFor = -1;
										rf.mu.Unlock();
									}
								}
							}
						}(i, appendEntriesArgs);
					}
				}
				time.Sleep(heartbeatInterval);
		} else { //candidate
			////fmt.Println(rf.me, " candidate mode");
			rf.mu.Lock();
			rf.currentTerm = rf.currentTerm + 1;
			rf.votedFor = rf.me;
			rf.voteCount = 1;
			rf.mu.Unlock();

			//send requestvotes to everyone else
			var requestVoteArgs RequestVoteArgs
			rf.mu.Lock();
			requestVoteArgs.Term = rf.currentTerm;
			requestVoteArgs.CandidateId = rf.me;
			//requestVoteArgs.lastLogTerm
			//requestVoteArgs.lastLogIndex
			rfMe := rf.me;
			rf.mu.Unlock();

			for i := range rf.peers {
				if i != rfMe {
					//if rf.state == "candidate" {
					//send requestVotes in parallel;
					go func(peerIndex int) {
						var requestVoteReply RequestVoteReply;
						////fmt.Println(requestVoteArgs.CandidateId, " sending request vote to ", peerIndex);
						rf.sendRequestVote(peerIndex, &requestVoteArgs, &requestVoteReply);
					}(i);
					//}
				}
			}

			select {
				case <-time.After(timeToLive):
					rf.mu.Lock();
					if rf.state != "leader" {
						rf.state = "follower";
						////fmt.Println(rf.me, "election timeout, went back to follower");
					}
					rf.mu.Unlock();
				case <-rf.heartbeatChannel:
					rf.mu.Lock();
					rf.state = "follower";
					////fmt.Println(rf.me, " found heartbeat. went back to follower");
					rf.mu.Unlock();
				case <-rf.electedChannel:
					rf.mu.Lock()
					rf.state = "leader";
					////fmt.Println(rf.me, " BECAME LEADER");
					/*
					rf.nextIndex = make([]int, len(rf.peers));
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.Logs) - 1
					}*/
					rf.mu.Unlock()
			}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0;
	rf.votedFor = -1;
	rf.state = "follower";
	rf.heartbeatChannel = make(chan bool, 100);
	rf.electedChannel = make(chan bool, 100);
	//rf.votingChannel = make(chan bool, 100);


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//coroutine that checks periodically for leader heartbeats
	//if none received within the time heartBeatTimeoutTime specified,
	//then start an election as a candidate
	//rand.Seed(time.Now().Unix())
	//maxTimeout := 500;
	//minTimeout := 250;
	maxTimeout := 500;
	minTimeout := 250;
	rf.timeoutTime = rand.Intn(maxTimeout - minTimeout) + minTimeout;//generate random election timeout time
	
	go stateChecker(rf);

	return rf
}
