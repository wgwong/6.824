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
	//"fmt"
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
	Command interface{}
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

	//CHANNELS
	heartbeatChannel chan bool;
	//commitChannel chan bool;
	electedChannel chan bool;
	ApplyMsgChannel chan ApplyMsg;

	//PERSISTENT
	//pre-defined
	currentTerm int
	votedFor int;
	log[] LogEntry;
	//additional
	state string; //TODO check whether state is persistent or whether elections occur after every recovery
	timeoutTime int;

	//VOLATILE ON SERVERS
	//pre-defined
	commitIndex int;
	lastApplied int;
	//additional
	voteCount int;
	replicateCount int;

	//VOLATILE ON LEADERS
	nextIndex[] int;
	matchIndex[] int;

}


func min(x int, y int) int {
	if x < y {
		return x;
	} else {
		return y;
	}
}

func max(x int, y int) int {
	if x > y {
		return x;
	} else {
		return y;
	}
}

func (rf *Raft) lock() {
	////5//fmt.Println("LOCK", rf.me, " acquiring lock");
	rf.mu.Lock();
}

func (rf *Raft) unlock() {
	////5//fmt.Println("UNLOCK", rf.me, " releasing lock");
	rf.mu.Unlock();
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.lock();
	term = (*rf).currentTerm;
	isleader = (*rf).state == "leader";
	////5//fmt.Println("GETSTATE()", rf.me, " is leader? ", isleader, " term: ", term);
	rf.unlock();
	return term, isleader;
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
	Entries[] LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	Index int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	//////////5//fmt.Println(args.CandidateId, " requestvote called on raft id: ", rf.me);
	//////////5//fmt.Println("\tstate: ", rf.state);
	//////////5//fmt.Println("\tvotedFor: ", rf.votedFor);
	//////////5//fmt.Println("\tcurrentTerm: ", rf.currentTerm);

	rf.lock()
	defer rf.unlock();
	//rfState := rf.state;
	rfTerm := rf.currentTerm;
	rfVotedFor := rf.votedFor;


	// Your code here (2A, 2B).
	/*
	if rfState == "leader" {
		reply.Term = rfTerm;
		reply.VoteGranted = false;
		//5//fmt.Println("\t", rf.me, " already leader and rejects", args.CandidateId," [reject]");
		return;
	}*/

	if rfTerm > args.Term {
		reply.Term = rfTerm;
		reply.VoteGranted = false;
		//5//fmt.Println("\t", rf.me, " term higher than candidate ", args.CandidateId, " [reject]");
		return;
	}

	if rfTerm == args.Term && rfVotedFor != -1 {
		//&&rf.votedFor != args.CandidateId
		if rfVotedFor != args.CandidateId { //reject if the person I voted for is not the candidate
			reply.Term = rfTerm;
			reply.VoteGranted = false;

			//5//fmt.Println("\t", rf.me, " already voted for ", rf.votedFor ," and rejects ", args.CandidateId," [reject]");
			return;
		}
	}

	if rfTerm < args.Term { //self is not up to date, convert to follower if not already follower
		//////5//fmt.Println("\t", rf.me, " term out of date, convert to follower, previous: ", rf.state);
		rf.state = "follower";
		rf.currentTerm = args.Term;
	}

	rfLastLogIndex := len(rf.log) - 1;
	rfLastLogTerm := 0;
	if rfLastLogIndex >= 0 {
		rfLastLogTerm = rf.log[rfLastLogIndex].Term;
	}

	//make sure candidate has more up to date log, otherwise reject vote request
	if args.LastLogTerm < rfLastLogTerm || args.LastLogTerm == rfLastLogTerm && args.LastLogIndex < rfLastLogIndex {
		reply.VoteGranted = false;
		//5//fmt.Println("\t", rf.me, " has more up to date log than candidate ", args.CandidateId ," [reject]\n\t\targs.LastLogTerm: ", args.LastLogTerm,"\n\t\trfLastLogTerm: ", rfLastLogTerm,"\n\t\targs.LastLogIndex: ", args.LastLogIndex,"\n\t\trfLastLogIndex: ", rfLastLogIndex,"\n\t\trf.log: ", rf.log);
	} else {
		rf.votedFor = args.CandidateId;
		rf.state = "follower";
		rf.currentTerm = args.Term;
		rfTerm = rf.currentTerm;
		reply.VoteGranted = true;

		//5//fmt.Println("\t", rf.me, " voted for ", rf.votedFor, ". currentTerm: ", rf.currentTerm, "[accept]");
	}
	reply.Term = rfTerm;
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

	//////5//fmt.Println(rf.me, " sendRequestVote: called ", server);
	//////5//fmt.Println("\targs| term: ", args.Term, ", CandidateId: ", args.CandidateId);
	//////5//fmt.Println("\treply| term:  ", reply.Term, ", votedGranted: ", reply.VoteGranted);

	if ok {
		rf.lock();
		rfState := rf.state;
		rfTerm := rf.currentTerm;
		rf.unlock();

		if rfState != "candidate" {
			return ok;
		}
		if args.Term != rfTerm {
			return ok;
		}
		if reply.Term > rfTerm { //leader out of date, demoted to follower
			rf.lock();
			rf.currentTerm = reply.Term
			rf.state = "follower";
			rf.votedFor = -1;
			//////////5//fmt.Println("\tleader out of date, demoted to follower");
			rf.unlock();
		}
		if reply.VoteGranted { //vote was received
			rf.lock();
			rf.voteCount = rf.voteCount + 1;
			//////5//fmt.Println("\tvote granted, vote count now: ", rf.voteCount);
			if rf.state == "candidate" && rf.voteCount > len(rf.peers)/2 {
				rf.electedChannel <- true
				//////5//fmt.Println("\telectedChannel true. ", rf.voteCount, " > ", len(rf.peers)/2);
			}
			rf.unlock();
		}
	}

	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock();
	defer rf.unlock();
	rfTerm := rf.currentTerm;
	//used for checking commitIndex
	//rfCommitIndex := rf.commitIndex;
	//rfLogLength := len(rf.log);
	if (len(args.Entries) > 0) {
		//5//fmt.Println(args.LeaderId, " called AppendEntries on ", rf.me, "with args: ", args, "\n\trf.log: ", rf.log);
	}

	reply.Term = rfTerm;
	reply.Index = -1;

	//AppendEntries RPC Rule 1
	if rfTerm != args.Term {
		if rfTerm > args.Term {
			//5//fmt.Println(rf.me, " has higher term (", rfTerm ,") than args/leader (", args.Term, ") for candidate: ", args.LeaderId);
			//5//fmt.Println("\trejecting due to rule 1 ");
			reply.Success = false;
			return;
		} else { // leader has higher term, squash any potential candidates/leaders
			//5//fmt.Println(rf.me, " has lower term (", rfTerm ,") than leader ", args.LeaderId,"'s args.Term (", args.Term, ")");
			//5//fmt.Println("\tsquashing, updating rf.currentTerm");
			rf.currentTerm = args.Term;
			rf.state = "follower";
			rf.votedFor = -1;
		}
	}

	rf.heartbeatChannel <- true; //receive heartbeat if valid leader

	////////5//fmt.Println("\t", rf.me, " rf.commitIndex: ", rf.commitIndex, ", args.LeaderCommit: ", args.LeaderCommit);

	rfPrevLogIndex := len(rf.log)-1;
	/*
	rfPrevLogTerm := 0;
	if rfPrevLogIndex >= 0 {
		rfPrevLogTerm = rf.log[rfPrevLogIndex].Term;
	}*/


	//AppendEntries RPC Rule 2
	if args.PrevLogIndex >= 0 {
		if (args.PrevLogIndex > rfPrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
			
				//5//fmt.Println(rf.me, " rejected ", args.LeaderId, "due to Rule 2\n\tAppendEntriesArgs: ", args);
				//5//fmt.Println("\trfPrevLogIndex: ", rfPrevLogIndex);
				//5//fmt.Println("\targs.PrevLogIndex: ", args.PrevLogIndex);
				//5//fmt.Println("\t", rfPrevLogIndex >= args.PrevLogIndex)
				/*
				//5//fmt.Println("\trfPrevLogTerm: ", rf.log[args.PrevLogIndex].Term);
				//5//fmt.Println("\targs.PrevLogTerm: ", args.PrevLogTerm);
				//5//fmt.Println("\t", rf.log[args.PrevLogIndex].Term == args.PrevLogTerm);
				*/

			reply.Success = false;

			return;
		}
	}

	//AppendEntries RPC Rule 3	
	
	if (len(args.Entries) > 0 ) {
		//5//fmt.Println(rf.me, " accepted AppendEntriesArgs from ", args.LeaderId,": ", args, "\n\t rf.log before: ", rf.log);
		//5//fmt.Println(rf.me, " rf.log previously: ", rf.log);
		//5//fmt.Println(rf.me, " endInterval args.PrevLogIndex+1 is ", args.PrevLogIndex+1);
	}

	//AppendEntries RPC Rule 4
	rf.log = rf.log[:args.PrevLogIndex+1];
	if (len(args.Entries) > 0) {
		//5//fmt.Println(rf.me, " rf.log truncated to: ", rf.log);
	}


	for i := 0; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i]);
	}

	if (len(args.Entries) > 0) {
		//5//fmt.Println(rf.me, "appended from ", args.LeaderId, ". entries: ", args.Entries, ". rf.log now: ", rf.log);
	}

	if (rf.me == 1) {
		////5//fmt.Println("1111 rf.commitIndex: ", rf.commitIndex);
		////5//fmt.Println("1111 args.LeaderCommit: ", args.LeaderCommit);
		if rf.commitIndex != args.LeaderCommit {
			////5//fmt.Println("1111 1111 changing commitIndex");
		}
	}

	//AppendEntries RPC Rule 5
	//5//fmt.Println(rf.me, " has rf.commitIndex: ", rf.commitIndex, " leader has args.LeaderCommit: ", args.LeaderCommit);

	if rf.commitIndex < args.LeaderCommit {
		//5//fmt.Println("\t", rf.me, "'s commitIndex (", rf.commitIndex, ") < args.LeaderCommit: ", args.LeaderCommit);
		//5//fmt.Println("\trf.log: ", rf.log);
		//////5//fmt.Println(rf.me, " has outdated commitIndex (", rf.commitIndex, ") vs leader's ", args.LeaderCommit, " it's now: ", min(args.LeaderCommit, len(rf.log)-1));
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1);
		//5//fmt.Println(rf.me, "has new commitIndex: ", rf.commitIndex, "\n\twith c: rf.log: ", rf.log);
	}

	reply.Success = true;

	return;
}

func (rf *Raft) transmitAppendEntries(peerIndex int, args *AppendEntriesArgs) AppendEntriesReply {
	var finalReply AppendEntriesReply;

	//5//fmt.Println(rf.me, " transmitting to ", peerIndex, "\n\targs: ", args);

	successfulCall := rf.peers[peerIndex].Call("Raft.AppendEntries", args, &finalReply);

	rf.lock();
	defer rf.unlock();

	//TODO, make sure snapshotted term = current term (make sure leader didn't become leader again later with a later term)
	if rf.state == "leader" && successfulCall && len(args.Entries) > 0 { //only if not a heartbeat message
		//5//fmt.Println(rf.me, " successfully called append entries on ", peerIndex);
		if rf.currentTerm < finalReply.Term { //leader is outdated, revert to follower
			//5//fmt.Println("\tbut ", rf.me," outdated. reverted to follower");
			rf.currentTerm = finalReply.Term;
			rf.state = "follower";
			rf.votedFor = -1;
		} else {
			if finalReply.Success {
				updatedIndex := args.Entries[len(args.Entries)-1].Index;
				//5//fmt.Println(rf.me, " successfully replicated entry to ", peerIndex);
				//5//fmt.Println("rf.matchIndex[",peerIndex,"] before: ", rf.matchIndex[peerIndex]);
				rf.matchIndex[peerIndex] = updatedIndex;
				rf.nextIndex[peerIndex] = len(args.Entries) + args.PrevLogIndex + 1;
				////5//fmt.Println("ACCEPTED rf.nextIndex[peerIndex] changed to: ", rf.nextIndex[peerIndex], ", with len(args.Entries)=", len(args.Entries), " + args.PrevLogTerm +1:", (args.PrevLogIndex+1)," with rf.log.length: ", len(rf.log));
				//5//fmt.Println("rf.matchIndex[",peerIndex,"] after: ", rf.matchIndex[peerIndex]);
			} else { //rejected for some reason, retry with an older entry
				if (peerIndex == 1) {
					////5//fmt.Println(peerIndex, " rejected from ", rf.me, "\n\targs: ", args, "\n\told rf.nextIndex[peerIndex]: ", rf.nextIndex[peerIndex]);
				}

				rf.nextIndex[peerIndex] = rf.nextIndex[peerIndex] - 1;

				////5//fmt.Println("REJECTED rf.nextIndex[peerIndex] changed to: ", rf.nextIndex[peerIndex], ", with rf.log.length: ", len(rf.log));

				//todo, continously retry with older entry maybe here too
				//IDK if necessary, since we just wait for the next heartbeat (which is < 100 ms);
			}
		}

	} else { //debug, due to follower timeout/failure??
		////////5//fmt.Println("FATAL ERROR 2");
		////////5//fmt.Println("successfulCall: ", successfulCall, ". len(args.Entries): ", len(args.Entries));
		//////5//fmt.Println(rf.me, " couldn't contact ", peerIndex, ". resetting rf.nextIndex to 0");
		//rf.nextIndex[peerIndex] = 0; //resend entire log from beginning if follower comes back
	}
	return finalReply;
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
	rf.lock();
	defer rf.unlock();

	index := len(rf.log);
	if index < 0 {
		index = 0;
	}

	term := rf.currentTerm;
	isLeader := true;

	// Your code here (2B).
	term = rf.currentTerm;
	isLeader = rf.state == "leader";
	if isLeader {
		//5//fmt.Println(rf.me, "'s start() called on command: ", command);
		//add LogEntry to own leader log
		var entry LogEntry;
		entry.Term = term;
		entry.Command = command;
		entry.Index = index;
		rf.nextIndex[rf.me] = index + 1; //index+1? original: index
		rf.matchIndex[rf.me] = index;
		////////5//fmt.Println(rf.me, "'s rf.log before: ", rf.log);
		rf.log = append(rf.log, entry);
		////////5//fmt.Println(rf.me, "'s rf.log after: ", rf.log);

		//5//fmt.Println(rf.me, "has peers: ", rf.peers);

		//transmit LogEntry to other rafts
		//repCountingChannel := make(chan bool, 100);
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func (peerIndex int) {
					////5//fmt.Println(rf.me, "'s REPLICATE called on peerIndex: ", peerIndex);
					rf.lock();
					var args AppendEntriesArgs;
					args.Term = rf.currentTerm;
					args.LeaderId = rf.me;
					////5//fmt.Println("rf.nextIndex: ", rf.nextIndex);
					////5//fmt.Println("peerIndex: ", peerIndex);
					args.PrevLogIndex = rf.nextIndex[peerIndex] - 1; //we subtract 1 more because we already appended the new command
					////5//fmt.Println("START", rf.me, "'s args.PrevLogIndex: ", args.PrevLogIndex, " for ", peerIndex);
					args.PrevLogTerm = 0;
					if args.PrevLogIndex >= 0 {
						args.PrevLogTerm = rf.log[args.PrevLogIndex].Term;
					} else {
						args.PrevLogTerm = 0;
					}
					if rf.nextIndex[peerIndex] >= 0 {
						args.Entries = rf.log[rf.nextIndex[peerIndex]:];
						////5//fmt.Println("START", rf.me, "for", peerIndex, ". rf.nextIndex[peerIndex]: ", rf.nextIndex[peerIndex]);
						////5//fmt.Println("START", rf.me, "for", peerIndex, ". rf.log: ", rf.log);
					}
					////////5//fmt.Println("\t", rf.me, "'s args.Entries: ", args.Entries, " for ", peerIndex);
					args.LeaderCommit = rf.commitIndex;
					rf.unlock();
					////5//fmt.Println(rf.me, " about to transmitAppendEntries to ", peerIndex, " with args: ", args);

					_ = rf.transmitAppendEntries(peerIndex, &args);
				}(i);
			}
		}
	}

	index = index + 1; //needs to be 1-indexed apparently
	return index, term, isLeader;
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
func stateChecker(rf *Raft) {
	timeToLive := time.Millisecond * time.Duration(rf.timeoutTime);
	//lastTimeChecked := time.Now();

	////5//fmt.Println(rf.me, " time to live: ", timeToLive);

	heartbeatInterval := 100 * time.Millisecond;

	for {
		rf.lock();
		curState := rf.state;
		rf.unlock();
		if curState == "follower" {

			select {
			case <- rf.heartbeatChannel:
				////5//fmt.Println("there's a heartbeat");
			case <- time.After(timeToLive)://time.Now().After(lastTimeChecked.Add(timeToLive)):
				//lastTimeChecked = time.Now();
				//5//fmt.Println(rf.me, " no heartbeat, became candidate");
				rf.lock();
				rf.state = "candidate"; //no heartbeat, become candidate
				rf.unlock();
			}
		} else if curState == "leader" {
				//send heartbeat to everyone else
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						rf.lock();
						var appendEntriesArgs AppendEntriesArgs;
						appendEntriesArgs.Term = rf.currentTerm;
						appendEntriesArgs.LeaderId = rf.me;
						appendEntriesArgs.PrevLogIndex = rf.nextIndex[i] - 1; //TODO CHECK THIS
							//previously len(log) - 1;
						if appendEntriesArgs.PrevLogIndex >= 0 {
							////5//fmt.Println("HEARTBEAT PREPARE ARGS.", rf.me, " appendEntriesArgs.PrevLogIndex: ", appendEntriesArgs.PrevLogIndex, " because rf.nextIndex[",i,"] is: ", rf.nextIndex[i], ". log: ", rf.log);
							appendEntriesArgs.PrevLogTerm = rf.log[appendEntriesArgs.PrevLogIndex].Term;
						}
						if rf.nextIndex[i] >= 0 {
							appendEntriesArgs.Entries = rf.log[rf.nextIndex[i]:];
						}
						appendEntriesArgs.LeaderCommit = rf.commitIndex;
						rf.unlock();

						

						////5//fmt.Println(rf.me, " sending heartbeat to ", i, ", entries: ", appendEntriesArgs.Entries, ",\n\trf.log: ", rf.log, ",\n\trf.nextIndex[1]: ", rf.nextIndex[i]);
						

						//send append entries
						go func(peerIndex int, appendEntriesArgs AppendEntriesArgs) {
							var appendEntriesReply AppendEntriesReply;
							ok := rf.peers[peerIndex].Call("Raft.AppendEntries", &appendEntriesArgs, &appendEntriesReply);
							rf.lock();
							rfTerm := rf.currentTerm;
							rf.unlock();
							if ok {
								if appendEntriesReply.Success {
									if len(appendEntriesArgs.Entries) > 0 {
										updatedIndex := appendEntriesArgs.Entries[len(appendEntriesArgs.Entries)-1].Index;
										//5//fmt.Println("\t", rf.me, " successfully replicated entry to ", peerIndex, ".\n\trf.nextIndex[peerIndex] previously: ", rf.nextIndex[peerIndex], ". now: ", updatedIndex);
										rf.lock();
										rf.matchIndex[peerIndex] = updatedIndex;
										rf.nextIndex[peerIndex] = len(appendEntriesArgs.Entries) + appendEntriesArgs.PrevLogIndex + 1;
										rf.unlock();
									}
								} else {
									if appendEntriesReply.Term > rfTerm { //leader is not up to date, demote to follower
										rf.lock();
										rf.currentTerm = appendEntriesReply.Term;
										rf.state = "follower";
										rf.votedFor = -1;
										//////5//fmt.Println(rf.me, "'s term: ", rfTerm, " is < appendEntriesReply.Term: ", appendEntriesReply.Term, " so demoted back to follower");
										rf.unlock();
									} else { //TODO CHECKT THIS decrease nextIndex??
										if (peerIndex == 1) {
											////5//fmt.Println("need to decrease index cuz heartbeat failed on rf.nextIndex[1]: ", rf.nextIndex[peerIndex]);
										}
										stillFailed := true;
										for stillFailed {//immediately retry
											rf.lock();
											rf.nextIndex[peerIndex] = rf.nextIndex[peerIndex] - 1;
											////5//fmt.Println("FAILED rf.nextIndex[peerIndex] changed to: ", rf.nextIndex[peerIndex], ", with rf.log.length: ", len(rf.log));
											var args AppendEntriesArgs;
											args.Term = rf.currentTerm;
											args.LeaderId = rf.me;
											args.PrevLogIndex = rf.nextIndex[peerIndex] - 1;
											if args.PrevLogIndex >= 0 {
												args.PrevLogTerm = rf.log[args.PrevLogIndex].Term;
											}
											if rf.nextIndex[peerIndex] >= 0 { //we send entire log if nextIndex = -1?
												args.Entries = append(args.Entries, rf.log[rf.nextIndex[peerIndex]:len(rf.log)]...);
												////5//fmt.Println("IMMEDIATE RETRY ENTRIES - rf.nextIndex[1]: ", rf.nextIndex[peerIndex], " on rf.log: ", rf.log);
											} else {
												args.Entries = append(args.Entries, rf.log[:]...);
											}
											args.LeaderCommit = rf.commitIndex;

											if (peerIndex == 1) {
												////5//fmt.Println("immediately retrying with rf.nextIndex[1]: ", rf.nextIndex[peerIndex], "new args: ", args);
											}

											var reply AppendEntriesReply;
											rf.unlock();
											ok := rf.peers[peerIndex].Call("Raft.AppendEntries", &args, &reply);
											rf.lock();
											//TODO, also check if leader not up to date
											//better, recurve above code, efficient

											if ok {
												stillFailed = !reply.Success;
												if stillFailed == false {
													////5//fmt.Println("IMMEDIATE RETRY: finally succeeded appending entry");
													updatedIndex := args.Entries[len(args.Entries)-1].Index;
													////////5//fmt.Println("\t", rf.me, " successfully replicated entry to ", peerIndex, ".\n\trf.nextIndex[peerIndex] previously: ", rf.nextIndex[peerIndex], ". now: ", updatedIndex);
													rf.matchIndex[peerIndex] = updatedIndex;
													rf.nextIndex[peerIndex] = len(args.Entries) + args.PrevLogIndex + 1;
												}
												rf.unlock();
											} else {

											}
										}
									}
								}
							}
						}(i, appendEntriesArgs);
					}
				}
				time.Sleep(heartbeatInterval);
		} else { //candidate
			rf.lock();
			//5//fmt.Println(rf.me, " candidate mode");
			//////5//fmt.Println(rf.me, "rf.currentTerm prev: ", rf.currentTerm);
			rf.currentTerm = rf.currentTerm + 1;
			//////5//fmt.Println(rf.me, "rf.currentTerm after: ", rf.currentTerm);
			rf.votedFor = rf.me;
			rf.voteCount = 1;

			//send requestvotes to everyone else
			var requestVoteArgs RequestVoteArgs
			requestVoteArgs.Term = rf.currentTerm;
			requestVoteArgs.CandidateId = rf.me;
			requestVoteArgs.LastLogIndex = len(rf.log)-1;
			requestVoteArgs.LastLogTerm = 1;
			if requestVoteArgs.LastLogIndex >= 0 {
				requestVoteArgs.LastLogTerm = rf.log[requestVoteArgs.LastLogIndex].Term;
			}
			////5//fmt.Println("[CANDIDACY]", rf.me, "args.LastLogIndex: ", requestVoteArgs.LastLogIndex, "\t\nrf.log: ", rf.log, " args.lastLogTerm: ", requestVoteArgs.LastLogTerm);

			for i := range rf.peers {
				if i != rf.me {
					//if rf.state == "candidate" {
					//send requestVotes in parallel;
					go func(peerIndex int) {
						var requestVoteReply RequestVoteReply;
						//////5//fmt.Println(requestVoteArgs.CandidateId, " sending request vote to ", peerIndex);
						rf.sendRequestVote(peerIndex, &requestVoteArgs, &requestVoteReply);
					}(i);
					//}
				}
			}
			rf.unlock();

			select {
				case <-time.After(timeToLive):
					rf.lock();
					if rf.state != "leader" {
						rf.state = "follower";
						//5//fmt.Println(rf.me, "election timeout, went back to follower");
					}
					rf.unlock();
				case <-rf.heartbeatChannel:
					rf.lock();
					rf.state = "follower";
					//5//fmt.Println(rf.me, " found heartbeat. went back to follower");
					rf.unlock();
				case <-rf.electedChannel:
					rf.lock()
					rf.state = "leader";
					//5//fmt.Println(rf.me, " BECAME LEADER on TERM: ", rf.currentTerm);

					//initialize after every election win
					rf.nextIndex = make([]int, len(rf.peers));
					rf.matchIndex = make([]int, len(rf.peers));
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log);
						rf.matchIndex[i] = -1;
					}
					rf.unlock();
			}
		}
	}
}

func applyChannelChecker(rf *Raft, applyCh chan ApplyMsg) {
	for {
		time.Sleep(10 * time.Millisecond);
		rf.lock();
		rfCommitIndex := rf.commitIndex;
		prevLastApplied  := rf.lastApplied;
		rf.unlock();
		if rfCommitIndex > prevLastApplied {
			////////5//fmt.Println("raft ", rf.me, " has commitIndex ", rf.commitIndex, " > lastApplied: ", rf.lastApplied);
			rf.lock();

			rf.lastApplied = rf.lastApplied + 1;
			prevLastApplied = rf.lastApplied;

			if (rf.me == 1) {
				////5//fmt.Println("\tapplying message for ", rf.me);

				////5//fmt.Println("\tprevLastApplied: ", prevLastApplied);
				////5//fmt.Println("\t", rf.me, " has rfCommitIndex: ", rf.commitIndex);
				////5//fmt.Println("\t", rf.me, " rf.lastApplied now: ", rf.lastApplied);
				////5//fmt.Println("\trf.log: ", rf.log);
			}

			applyCommand := rf.log[prevLastApplied].Command;

			rf.unlock();

			//time.Sleep(10 * time.Millisecond);


			var message ApplyMsg;
			message.Index = prevLastApplied + 1; //account for 1-indexing for clients
			message.Command = applyCommand;
			
			applyCh <- message;

			//5//fmt.Println("[APPLY CH]", rf.me, " sent message contents: ", message);


			/*
			for i := prevLastApplied+1; i <= rfCommitIndex; i++ {
				////////5//fmt.Println("\tfor index: ", i);
				var message ApplyMsg;
				message.Index = i + 1;
				message.Command = rfLog[i].Command;
				//////5//fmt.Println("\t", rf.me, " sending message contents: ", message);
				applyCh <- message;

				rf.lock();
				rf.lastApplied = rf.lastApplied + 1;
				rf.unlock();
			}*/
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

	//5//fmt.Println("initialized ", rf.me);

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0;
	rf.votedFor = -1;
	rf.state = "follower";
	rf.heartbeatChannel = make(chan bool, 100);
	//rf.commitChannel = make(chan bool, 100);
	rf.electedChannel = make(chan bool, 100);
	//applyCh = make(chan ApplyMsg, 100);
	rf.commitIndex = -1; //starting at 0 conflicts with code, also illogical
	rf.lastApplied = -1; //starting at 0 conflicts with code, also illogical


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//coroutine that checks periodically for leader heartbeats
	//if none received within the time heartBeatTimeoutTime specified,
	//then start an election as a candidate
	//rand.Seed(time.Now().Unix())
	//maxTimeout := 500;
	//minTimeout := 250;
	maxTimeout := 1000;
	minTimeout := 500;
	rf.timeoutTime = rand.Intn(maxTimeout - minTimeout) + minTimeout;//generate random election timeout time
	

	go stateChecker(rf);
	go applyChannelChecker(rf, applyCh);

	go func (rf *Raft) {
		for {
			time.Sleep(50 * time.Millisecond);
			rf.lock();
			if rf.state == "leader" {
				//quorum := len(rf.peers)/2;

				//if (rf.me == 2) {
					////5//fmt.Println("[COMMITCHECK]: currentCommitIndex: ", rf.commitIndex);
				//}

				//find majority matchindex and assign it to candidateindex
				candidateIndex := rf.matchIndex[rf.me];
				candidateCount := 1;
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						//if (rf.me == 2) {
							//mt.Println("[COMMITCHECK]: peer: ", i);
						//}
						matchIndex := rf.matchIndex[i];
						//if (rf.me == 2) {
							////5//fmt.Println("[COMMITCHECK]: matchIndex: ", matchIndex);
						//}
						if matchIndex == candidateIndex {
							//if (rf.me == 2) {
								////5//fmt.Println("[COMMITCHECK]: matchIndex equal to candidateIndex");
							//}
							candidateCount = candidateCount + 1;
							//if (rf.me == 2) {
								////5//fmt.Println("[COMMITCHECK]: candidateCount now: ", candidateCount);
							//}
						} else {
							//if (rf.me == 2) {
								////5//fmt.Println("[COMMITCHECK]: matchIndex not equal to candidateIndex");
							//}
							candidateCount = candidateCount - 1;
							//if (rf.me == 2) {
								////5//fmt.Println("[COMMITCHECK]: candidateCount now: ", candidateCount);
							//}
							if candidateCount == 0 {
								candidateIndex = matchIndex;
								candidateCount = 1;
								//if (rf.me == 2) {
									////5//fmt.Println("[COMMITCHECK]: candidateCount 0, new candidateIndex: ", candidateIndex, "candidateCount 1");
								//}
							}
						}
					}
				}
				//majority matchindex after this should be candidateIndex
				if candidateIndex > rf.commitIndex && rf.log[candidateIndex].Term == rf.currentTerm {
					//if (rf.me == 2) {
						//5//fmt.Println("[COMMITCHECK]", rf.me, "leader found higher matchIndex:", candidateIndex);
					//}
					rf.commitIndex = candidateIndex;
				}
			}

			rf.unlock();
		}
	}(rf);

	return rf;
}