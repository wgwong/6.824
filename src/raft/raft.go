//safe
//before changing requestvote rpc


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
	"sort"
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
	quitChannel chan struct{};

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
	// Your code here (2A, 2B).

	rf.lock()
	defer rf.unlock();

	DPrintf("[RequestVote]", args.CandidateId, " requestvote called on raft id: ", rf.me, "state: ", rf.state, "votedFor: ", rf.votedFor, "currentTerm: ", rf.currentTerm);

	reply.Term = rf.currentTerm;

	if rf.currentTerm > args.Term {
		reply.VoteGranted = false;
		DPrintf("[RequestVote]", rf.me, " term higher than candidate ", args.CandidateId, " [reject]");
		return;
	}

	if rf.currentTerm == args.Term && rf.votedFor != -1 {
		//&&rf.votedFor != args.CandidateId
		if rf.votedFor != args.CandidateId { //reject if the person I voted for is not the candidate
			reply.VoteGranted = false;
			DPrintf("[RequestVote]", rf.me, " already voted for ", rf.votedFor ," and rejects ", args.CandidateId," [reject]");
			return;
		}
	}

	if rf.currentTerm < args.Term { //self is not up to date, convert to follower if not already follower
		DPrintf("[RequestVote]", rf.me, " term out of date, convert to follower, previous: ", rf.state);
		rf.state = "follower";
		rf.currentTerm = args.Term;
		rf.persist();
	}

	rfLastLogIndex := len(rf.log) - 1;
	rfLastLogTerm := 0;
	if rfLastLogIndex >= 0 {
		rfLastLogTerm = rf.log[rfLastLogIndex].Term;
	}

	//make sure candidate has more up to date log, otherwise reject vote request
	if args.LastLogTerm < rfLastLogTerm || args.LastLogTerm == rfLastLogTerm && args.LastLogIndex < rfLastLogIndex {
		reply.VoteGranted = false;
		DPrintf("[RequestVote]", rf.me, " has more up to date log than candidate ", args.CandidateId ," [reject]\n\t\targs.LastLogTerm: ", args.LastLogTerm,"\n\t\trfLastLogTerm: ", rfLastLogTerm,"\n\t\targs.LastLogIndex: ", args.LastLogIndex,"\n\t\trfLastLogIndex: ", rfLastLogIndex,"\n\t\trf.log: ", rf.log);
	} else {
		rf.votedFor = args.CandidateId;
		rf.state = "follower";
		rf.currentTerm = args.Term;
		reply.Term = args.Term;
		reply.VoteGranted = true;

		DPrintf("[RequestVote]", rf.me, " voted for ", rf.votedFor, ". currentTerm: ", rf.currentTerm, "[accept]");
	}
	rf.persist() //persistmark 3
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

	rf.lock();
	snapshotTerm := rf.currentTerm;
	rf.unlock();

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply);

	//////5//fmt.Println(rf.me, " sendRequestVote: called ", server);
	//////5//fmt.Println("\targs| term: ", args.Term, ", CandidateId: ", args.CandidateId);
	//////5//fmt.Println("\treply| term:  ", reply.Term, ", votedGranted: ", reply.VoteGranted);

	rf.lock();
	defer rf.unlock();
	if ok {
		
		//rfState := rf.state;
		//rfTerm := rf.currentTerm;

		if args.Term != rf.currentTerm {
			return ok;
		}
		if reply.Term > rf.currentTerm { //leader out of date, demoted to follower
			rf.currentTerm = reply.Term
			rf.state = "follower";
			rf.votedFor = -1;
			rf.persist(); //persistmark 6
			//fmt.Println(rf.me, "leader out of date, demoted to follower");
		}
		if reply.VoteGranted && snapshotTerm == rf.currentTerm { //vote was received and term is still up to date
			rf.voteCount = rf.voteCount + 1;
			//fmt.Println(rf.me, "vote granted from ", server, " , vote count now: ", rf.voteCount);
			if rf.state == "candidate" && rf.voteCount > len(rf.peers)/2 {
				rf.electedChannel <- true;
				//fmt.Println(rf.me, "electedChannel true. ", rf.voteCount, " > ", len(rf.peers)/2);
			}
		}
	}
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock();
	defer rf.unlock();
	//used for checking commitIndex
	//rfCommitIndex := rf.commitIndex;
	//rfLogLength := len(rf.log);
	if (len(args.Entries) > 0) {
		//fmt.Println(args.LeaderId, " called AppendEntries on ", rf.me, "with args:\n\tterm: ", args.Term, "\n\tleaderid: ", args.LeaderId, "\n\tprevlogindex: ", args.PrevLogIndex, "\n\tprevlogterm: ", args.PrevLogTerm, "\n\tentries[0]: ", args.Entries[0],"\n\tentries[last]: ", args.Entries[len(args.Entries)-1]);
	}

	reply.Term = rf.currentTerm;
	reply.Index = -1;

	//AppendEntries RPC Rule 1
	if rf.currentTerm != args.Term {
		if rf.currentTerm > args.Term {
			//5//fmt.Println(rf.me, " has higher term (", rf.currentTerm ,") than args/leader (", args.Term, ") for candidate: ", args.LeaderId);
			//5//fmt.Println("\trejecting due to rule 1 ");
			reply.Success = false;
			return;
		} else { // leader has higher term, squash any potential candidates/leaders
			//5//fmt.Println(rf.me, " has lower term (", rf.currentTerm ,") than leader ", args.LeaderId,"'s args.Term (", args.Term, ")");
			//5//fmt.Println("\tsquashing, updating rf.currentTerm");
			rf.currentTerm = args.Term;
			rf.state = "follower";
			rf.votedFor = -1;
			rf.persist();
		}
	}

	rf.heartbeatChannel <- true; //receive heartbeat if valid leader

	////////5//fmt.Println("\t", rf.me, " rf.commitIndex: ", rf.commitIndex, ", args.LeaderCommit: ", args.LeaderCommit);

	/*
	rfPrevLogTerm := 0;
	if rfPrevLogIndex >= 0 {
		rfPrevLogTerm = rf.log[rfPrevLogIndex].Term;
	}*/

	if (len(rf.log) > 0) {
		//fmt.Println("appendentries()", rf.me, " has rf.log[:1]: ", rf.log[:1]);
	} else {
		//fmt.Println("appendentries()", rf.me, " has empty rf.log");
	}

	//AppendEntries RPC Rule 2
	if args.PrevLogIndex >= 0 { //prevents trying to get prevlogindex of empty log
		if (args.PrevLogIndex > len(rf.log) - 1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
			if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
				//5//fmt.Println(rf.me, " rejected ", args.LeaderId, "due to Rule 2. args:\n\tterm: ", args.Term, "\n\tleaderid: ", args.LeaderId, "\n\tprevlogindex: ", args.PrevLogIndex, "\n\tprevlogterm: ", args.PrevLogTerm, "\n\trfPrevLogIndex: ", len(rf.log)-1, ". rf.log[", args.PrevLogIndex, "].Term: ",  rf.log[args.PrevLogIndex].Term);
			} else if args.PrevLogIndex >= len(rf.log) {
				//5//fmt.Println(rf.me, " rejected ", args.LeaderId, "due to Rule 2. args:\n\tterm: ", args.Term, "\n\tleaderid: ", args.LeaderId, "\n\tprevlogindex: ", args.PrevLogIndex, "\n\tprevlogterm: ", args.PrevLogTerm, "\n\trfPrevLogIndex: ", len(rf.log)-1, ". rf.log[", args.PrevLogIndex, "].Term: nonexistent");
			} else {
				//5//fmt.Println(rf.me, " rejected ", args.LeaderId, "due to Rule 2. args:\n\tterm: ", args.Term, "\n\tleaderid: ", args.LeaderId, "\n\tprevlogindex: ", args.PrevLogIndex, "\n\tprevlogterm: ", args.PrevLogTerm, "\n\trfPrevLogIndex: ", len(rf.log)-1);
			}
			/*
			//5//fmt.Println("\trfPrevLogTerm: ", rf.log[args.PrevLogIndex].Term);
			//5//fmt.Println("\targs.PrevLogTerm: ", args.PrevLogTerm);
			//5//fmt.Println("\t", rf.log[args.PrevLogIndex].Term == args.PrevLogTerm);
			*/

			if args.PrevLogIndex > len(rf.log) - 1 {
				reply.Index = len(rf.log);
			} else {
				//find index of first entry that shares the same term as the conflicting entry
				conflictTerm := rf.log[args.PrevLogIndex].Term;
				firstEntryIndex := args.PrevLogIndex;
				for i := rf.log[args.PrevLogIndex].Index; i >= 0; i-- {
					if rf.log[i].Term != conflictTerm {
						break;
					}
					firstEntryIndex = rf.log[i].Index;
				}
				reply.Index = firstEntryIndex;
			}

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

	if args.PrevLogIndex + 1 < 0 || args.PrevLogIndex + 1 > len(rf.log) {
		//5//fmt.Println("PANIC args.PrevLogIndex: ", args.PrevLogIndex);
		//5//fmt.Println("\trf.log: ", rf.log);
	}

	DPrintf("[AppendEntries]", rf.me, " rf.log before (last 10):", rf.log[max(0, len(rf.log)-11):len(rf.log)]);

	DPrintf("[AppendEntries]", rf.me, " len(rf.log): ", len(rf.log), ". args.PrevLogIndex + 1", args.PrevLogIndex + 1);

	rf.log = rf.log[:args.PrevLogIndex+1];

	DPrintf("[AppendEntries]", rf.me, " rf.log after (last 10):", rf.log[max(0, len(rf.log)-11):len(rf.log)]);

	DPrintf("[AppendEntries]", rf.me, " rf.log truncated to (last 10): ", rf.log[max(0, len(rf.log)-11):len(rf.log)]);


	for i := 0; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i]);
	}

	DPrintf("[AppendEntries]", rf.me, " rf.log appended (last 10): ", rf.log[max(0, len(rf.log)-11):len(rf.log)]);

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
		DPrintf("[APPENDENTRIES]", rf.me, "'s commitIndex (", rf.commitIndex, ") < args.LeaderCommit: ", args.LeaderCommit, " now: ", min(args.LeaderCommit, len(rf.log)-1), ", args.LeaderCommit: ", args.LeaderCommit, ", len(rf.log)-1: ", len(rf.log)-1, "rf.log[last-10:last]: ", rf.log[max(0, len(rf.log)-11):len(rf.log)]);
		//5//fmt.Println("\trf.log: ", rf.log);
		//////5//fmt.Println(rf.me, " has outdated commitIndex (", rf.commitIndex, ") vs leader's ", args.LeaderCommit, " it's now: ", min(args.LeaderCommit, len(rf.log)-1));
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1);
		//5//fmt.Println(rf.me, "has new commitIndex: ", rf.commitIndex, "\n\twith c: rf.log: ", rf.log);
	}

	reply.Success = true;
	rf.persist() //persistmark 1

	return;
}

func (rf *Raft) sendAppendEntries(peerIndex int, args *AppendEntriesArgs) {
	var finalReply AppendEntriesReply;

	rf.lock();
	snapshotTerm := rf.currentTerm; //snapshot the leader's term before calling AppendEntries
	//this is to make sure the leader didn't become leader again later with a later term and handles the reply of an AppendEntries sent when it was leader on an older term
	rf.unlock();

	successfulCall := rf.peers[peerIndex].Call("Raft.AppendEntries", args, &finalReply);

	rf.lock();

	if rf.state == "leader" && snapshotTerm == rf.currentTerm && successfulCall { //snapshotTerm == rf.currentTerm ensures we don't handle stale replies
		if len(args.Entries) > 0 {
			//5//fmt.Println(rf.me, " successfully called append entries on ", peerIndex);
		}
		if rf.currentTerm < finalReply.Term { //leader is outdated, revert to follower
			//5//fmt.Println("\tbut ", rf.me," outdated. reverted to follower");
			rf.currentTerm = finalReply.Term;
			rf.state = "follower";
			rf.votedFor = -1;
			rf.persist(); //unchecked persistmark
		} else {
			if finalReply.Success {
				if len(args.Entries) > 0 { //TODO check this logic: only update matchIndex & nextIndex if not heartbeat
					updatedIndex := args.Entries[len(args.Entries)-1].Index;
					//5//fmt.Println(rf.me, " successfully replicated entry to ", peerIndex);
					//5//fmt.Println("rf.matchIndex[",peerIndex,"] before: ", rf.matchIndex[peerIndex]);
					rf.matchIndex[peerIndex] = updatedIndex;

					rf.nextIndex[peerIndex] = len(args.Entries) + args.PrevLogIndex + 1;

					rf.updateLeaderCommitIndex();
					DPrintf("[SEND APPENDENTRIES]", peerIndex, " ACCEPTED rf.nextIndex[", peerIndex, "] changed to: ", rf.nextIndex[peerIndex], ", with len(args.Entries)=", len(args.Entries), " + args.PrevLogTerm +1:", (args.PrevLogIndex+1)," with rf.log.length: ", len(rf.log), "rf.log[last-10:last]: ", rf.log[max(0, len(rf.log)-11):len(rf.log)]);
					//5//fmt.Println("rf.matchIndex[",peerIndex,"] after: ", rf.matchIndex[peerIndex]);
				}
			} else { //rejected for some reason, retry with an older entry
				rf.unlock();
				//if (peerIndex == 1) {
				//5//fmt.Println("Rejected for some reaosn. IMMEDIATE RETRY:", peerIndex, " rejected from ", rf.me, "\n\targs: ", args, "\n\told rf.nextIndex[peerIndex]: ", rf.nextIndex[peerIndex]);
				//}

				////5//fmt.Println("REJECTED rf.nextIndex[peerIndex] changed to: ", rf.nextIndex[peerIndex], ", with rf.log.length: ", len(rf.log));

				//todo, continously retry with older entry maybe here too
				//IDK if necessary, since we just wait for the next heartbeat (which is < 100 ms);
				stillFailed := true;
				for stillFailed { //immediately retry
					rf.lock();
					if finalReply.Index != -1 {
						rf.nextIndex[peerIndex] = finalReply.Index;
						DPrintf("[SEND APPENDENTRIES]", peerIndex, " still fail existing reply.index rf.nextIndex[", peerIndex, "] changed to: ", rf.nextIndex[peerIndex], ". len(rf.log): ", len(rf.log));
					} else {
						rf.nextIndex[peerIndex] = rf.nextIndex[peerIndex] - 1;
						DPrintf("[SEND APPENDENTRIES]", peerIndex, " still fail -1 reply.index rf.nextIndex[", peerIndex, "] changed to: ", rf.nextIndex[peerIndex], ". len(rf.log): ", len(rf.log));
					}

					//5//fmt.Println(rf.me, "rf.nextIndex[", peerIndex, "] decremented to: ", rf.nextIndex[peerIndex]);

					var newArgs AppendEntriesArgs;
					newArgs.Term = rf.currentTerm;
					newArgs.LeaderId = rf.me;
					newArgs.PrevLogIndex = rf.nextIndex[peerIndex] - 1;
					if newArgs.PrevLogIndex >= 0 {
						newArgs.PrevLogTerm = rf.log[newArgs.PrevLogIndex].Term;
					} else {
						newArgs.PrevLogTerm = 0;
					}
					if rf.nextIndex[peerIndex] >= 0 {//send entire log if nextIndex = -1;
						newArgs.Entries = append(newArgs.Entries, rf.log[rf.nextIndex[peerIndex]:len(rf.log)]...);
					} else {
						newArgs.Entries = append(newArgs.Entries, rf.log[:]...);
					}
					newArgs.LeaderCommit = rf.commitIndex;
					var reply AppendEntriesReply;
					rf.unlock();
					ok := rf.peers[peerIndex].Call("Raft.AppendEntries", &newArgs, &reply);

					rf.lock();
					if rf.state == "leader" && ok { //shouldn't need the len(args.Entries) > 0 since we're always adding more to args.Entries each time
						if rf.currentTerm < reply.Term { //leader is outdated, revert to follower
							//5//fmt.Println("\tbut ", rf.me," outdated. reverted to follower");
							rf.currentTerm = reply.Term;
							rf.state = "follower";
							rf.votedFor = -1;
							rf.persist(); //unchecked persistmark
							rf.unlock();
							return;
						}
						stillFailed = !reply.Success;
						if !stillFailed {
							updatedIndex := newArgs.Entries[len(newArgs.Entries)-1].Index;
							//5//fmt.Println(rf.me, " successfully replicated entry to ", peerIndex);
							//5//fmt.Println("rf.matchIndex[",peerIndex,"] before: ", rf.matchIndex[peerIndex]);
							rf.matchIndex[peerIndex] = updatedIndex;

							rf.nextIndex[peerIndex] = len(newArgs.Entries) + newArgs.PrevLogIndex + 1;
							DPrintf("[SEND APPENDENTRIES]", peerIndex, " ACCEPTED from stillfail rf.nextIndex[", peerIndex, "] changed to: ", rf.nextIndex[peerIndex], ", with len(args.Entries)=", len(newArgs.Entries), " + args.PrevLogTerm +1:", (newArgs.PrevLogIndex+1)," with rf.log.length: ", len(rf.log), " rf.log(last 10): ", rf.log[max(0, len(rf.log)-11):len(rf.log)]);

							rf.updateLeaderCommitIndex();
						}
					} else {
						if rf.state != "leader" { //if for some reason we're no longer leader, stop everything
							rf.unlock();
							return;
						}
					}
					rf.unlock();
				}
				rf.lock(); //this is because we were in the locked state right before we entered this else statement
				//we lock again so that we can simply unlock for all cases at the bottom;
			}
		}

	} else { //debug, due to follower timeout/failure??
		////////5//fmt.Println("FATAL ERROR 2");
		////////5//fmt.Println("successfulCall: ", successfulCall, ". len(args.Entries): ", len(args.Entries));
		//////5//fmt.Println(rf.me, " couldn't contact ", peerIndex, ". resetting rf.nextIndex to 0");
		//rf.nextIndex[peerIndex] = 0; //resend entire log from beginning if follower comes back
	}
	rf.persist(); //persistmark 2
	rf.unlock();
	return;
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
		rf.persist() //persistmark 5

		//transmit LogEntry to other rafts
		//repCountingChannel := make(chan bool, 100);
		/*
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
					args.PrevLogIndex = rf.nextIndex[peerIndex] - 1;
					//5//fmt.Println("START", rf.me, "'s args.PrevLogIndex: ", args.PrevLogIndex, " for ", peerIndex);
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
					////5//fmt.Println(rf.me, " about to sendAppendEntries to ", peerIndex, " with args: ", args);


					DPrintf("[START]", rf.me, " sending (start) sendappendentries to ", peerIndex, ", entries (last 10): ", args.Entries[max(0, len(args.Entries)-11):len(args.Entries)], ", rf.log (last 10): ",rf.log[max(0, len(rf.log)-11):len(rf.log)], ", rf.nextIndex[",peerIndex,"]: ", rf.nextIndex[peerIndex]);
					rf.sendAppendEntries(peerIndex, &args);
				}(i);
			}
		}*/
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
	close(rf.quitChannel);
}


func (rf *Raft) updateLeaderCommitIndex() {
	matchIndexCopy := make([]int, len(rf.matchIndex));
	copy(matchIndexCopy, rf.matchIndex);
	sort.Ints(matchIndexCopy);

	//fmt.Println("[cic] sorted matchIndex: ", matchIndexCopy);

	candidateIndex := matchIndexCopy[len(matchIndexCopy)/2];

	//majority matchindex after this should be candidateIndex
	if candidateIndex > rf.commitIndex && rf.log[candidateIndex].Term == rf.currentTerm {
		//if (rf.me == 2) {
			//5//fmt.Println("[COMMITCHECK]", rf.me, "leader found higher matchIndex:", candidateIndex);
		//}
		DPrintf("[COMMITCHECK] leader: ", rf.me, " found higher commitIndex: prev: ", rf.commitIndex, ". after: ", candidateIndex, ". len(rf.log): ", len(rf.log));
		rf.commitIndex = candidateIndex;
	}
}


//leader heartbeat receiver function
func stateChecker(rf *Raft) {
	timeToLive := time.Millisecond * time.Duration(rf.timeoutTime);
	//lastTimeChecked := time.Now();

	////5//fmt.Println(rf.me, " time to live: ", timeToLive);

	heartbeatInterval := 100 * time.Millisecond;

	for {
		select {
		case <- rf.quitChannel:
			////5//fmt.Println("closing stateChecker");
			return;
		default:
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
							var args AppendEntriesArgs;
							args.Term = rf.currentTerm;
							args.LeaderId = rf.me;
							args.PrevLogIndex = rf.nextIndex[i] - 1; //TODO CHECK THIS
								//previously len(log) - 1;
							if args.PrevLogIndex >= 0 {
								////5//fmt.Println("HEARTBEAT PREPARE ARGS.", rf.me, " args.PrevLogIndex: ", args.PrevLogIndex, " because rf.nextIndex[",i,"] is: ", rf.nextIndex[i], ". log: ", rf.log);
								args.PrevLogTerm = rf.log[args.PrevLogIndex].Term;
							}
							if rf.nextIndex[i] >= 0 {
								args.Entries = rf.log[rf.nextIndex[i]:];
							}
							args.LeaderCommit = rf.commitIndex;
							DPrintf("[HEARTBEAT]", rf.me, " sending heartbeat to ", i, ", entries (last 10): ", args.Entries[max(0, len(args.Entries)-11):len(args.Entries)], ", rf.log (last 10): ",rf.log[max(0, len(rf.log)-11):len(rf.log)], ", rf.nextIndex[",i,"]: ", rf.nextIndex[i]);
							rf.unlock();
							go rf.sendAppendEntries(i, &args);
						}
					}
					time.Sleep(heartbeatInterval);
			} else { //candidate
				rf.lock();
				//fmt.Println(rf.me, " candidate mode, rf.log: ", rf.log);
				//fmt.Println(rf.me, "rf.currentTerm prev: ", rf.currentTerm);
				rf.currentTerm = rf.currentTerm + 1;
				//fmt.Println(rf.me, "rf.currentTerm after: ", rf.currentTerm);
				rf.votedFor = rf.me;
				rf.voteCount = 1;
				rf.electedChannel = make(chan bool, 100);

				rf.persist(); //unchecked persistmark

				//send requestvotes to everyone else
				var requestVoteArgs RequestVoteArgs
				requestVoteArgs.Term = rf.currentTerm;
				requestVoteArgs.CandidateId = rf.me;
				requestVoteArgs.LastLogIndex = len(rf.log)-1;
				requestVoteArgs.LastLogTerm = 1;
				if requestVoteArgs.LastLogIndex >= 0 {
					requestVoteArgs.LastLogTerm = rf.log[requestVoteArgs.LastLogIndex].Term;
				}
				DPrintf("[CANDIDACY]", rf.me, "args.LastLogIndex: ", requestVoteArgs.LastLogIndex, "\t\nrf.log (last 10): ", rf.log[max(0, len(rf.log)-11):len(rf.log)], " args.lastLogTerm: ", requestVoteArgs.LastLogTerm);

				for i := range rf.peers {
					if i != rf.me {
						//if rf.state == "candidate" {
						//send requestVotes in parallel;
						go func(peerIndex int) {
							var requestVoteReply RequestVoteReply;
							//fmt.Println(requestVoteArgs.CandidateId, " sending request vote to ", peerIndex);
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
							//fmt.Println("[CANDIDACY]", rf.me, "election timeout, went back to follower");
						}
						rf.unlock();
					case <-rf.heartbeatChannel:
						rf.lock();
						rf.state = "follower";
						//fmt.Println("[CANDIDACY]", rf.me, " found heartbeat. went back to follower");
						rf.unlock();
					case <-rf.electedChannel:
						rf.lock()
						rf.state = "leader";
						DPrintf("[CANDIDACY]", rf.me, " BECAME LEADER on TERM: ", rf.currentTerm, ". rf.log[last-10:last]: ", rf.log[max(0, len(rf.log)-11):len(rf.log)]);
						//5//fmt.Println(rf.me, " BECAME LEADER on TERM: ", rf.currentTerm, ". rf.log: ", rf.log);

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
}

func applyChannelChecker(rf *Raft, applyCh chan ApplyMsg) {
	for {
		select {
		case <- rf.quitChannel:
			////5//fmt.Println("closing apply Channel checker");
			return;
		default:
			time.Sleep(10 * time.Millisecond);
			rf.lock();
			if rf.commitIndex > rf.lastApplied {
				////////5//fmt.Println("raft ", rf.me, " has commitIndex ", rf.commitIndex, " > lastApplied: ", rf.lastApplied);

				rf.lastApplied = rf.lastApplied + 1;

				DPrintf("[APPLY CH]", rf.me, ". has rfCommitIndex: ", rf.commitIndex, ". rf.lastApplied now: ", rf.lastApplied, ". len(rf.log): ", len(rf.log), ". rf.log[0:10]: ", rf.log[0:min(len(rf.log), 11)], ". rf.log[last-10:last]: ", rf.log[max(0, len(rf.log)-11):len(rf.log)]);

				applyCommand := rf.log[rf.lastApplied].Command;

				//time.Sleep(10 * time.Millisecond);


				var message ApplyMsg;
				message.Index = rf.lastApplied + 1; //account for 1-indexing for clients
				message.Command = applyCommand;
				
				applyCh <- message;

				DPrintf("[APPLY CH]", rf.me, " sent message contents: ", message, "rf.votedFor: ", rf.votedFor);

			}
			rf.unlock();
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

	////5//fmt.Println("initialized ", rf.me);

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0;
	rf.votedFor = -1;
	rf.state = "follower";
	rf.heartbeatChannel = make(chan bool, 100);
	//rf.commitChannel = make(chan bool, 100);
	rf.electedChannel = make(chan bool, 100);
	rf.quitChannel = make(chan struct{});
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
	//go commitIndexChecker(rf);

	return rf;
}