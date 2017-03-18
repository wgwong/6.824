package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	//"fmt"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	Action string
	ClientId int64
	SequenceNumber int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyValueMap map[string]string
	commandMap map[int64]int //makes each command unique through tracking unique serial #, returns result immediately if already processed
		/*
			solution is for clients to assign unique serial numbers to
			every command. Then, the state machine tracks the latest
			serial number processed for each client, along with the associated
			response. If it receives a command whose serial
			number has already been executed, it responds immediately
			without re-executing the request.
		*/
	res map[int] chan Op
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op Op;
	op.Key = args.Key;
	op.Value = ""; //Get() doesn't require Value
	op.Action = "Get";
	op.ClientId = args.ClientId;
	op.SequenceNumber = args.SequenceNumber;

	reply.WrongLeader = true;
	reply.Err = "";


	index, _, isLeader := kv.rf.Start(op); //don't care about term
	if isLeader {
		kv.mu.Lock();
		_, ok := kv.res[index];
		if !ok {
			kv.res[index] = make(chan Op, 1);
		}
		kv.mu.Unlock();

		select {
			case receivedOp := <- kv.res[index]:
				if receivedOp == op {
					reply.WrongLeader = false;
					kv.mu.Lock();
					reply.Value = kv.keyValueMap[args.Key];
					kv.mu.Unlock();
				} else {
					reply.Err = "Error";
				}
			case <- time.After(time.Duration(500)*time.Millisecond):
				reply.Err = "TimeOut";
		}
	}

	kv.mu.Lock();
	delete(kv.res, index);
	kv.mu.Unlock();
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op Op;
	op.Key = args.Key;
	op.Value = args.Value;
	op.Action = args.Op;
	op.ClientId = args.ClientId;
	op.SequenceNumber = args.SequenceNumber;

	reply.WrongLeader = true;
	reply.Err = "";

	index, _, isLeader := kv.rf.Start(op); //don't care about term
	if isLeader {
		kv.mu.Lock();
		_, ok := kv.res[index];
		if !ok {
			kv.res[index] = make(chan Op, 1);
		}
		kv.mu.Unlock();

		select {
			case receivedOp := <- kv.res[index]:
				if receivedOp == op {
					reply.WrongLeader = false;
				} else {
					reply.Err = "Error";
				}
			case <- time.After(time.Duration(500)*time.Millisecond):
				reply.Err = "TimeOut";
		}
	}

	kv.mu.Lock();
	delete(kv.res, index);
	kv.mu.Unlock();
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{});

	kv := new(RaftKV);
	kv.me = me;
	kv.maxraftstate = maxraftstate;

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg);
	kv.rf = raft.Make(servers, me, persister, kv.applyCh);

	// You may need initialization code here.

	kv.keyValueMap = make(map[string]string);
	kv.commandMap = make(map[int64]int);
	kv.res = make(map[int] chan Op);

	go func() {
		for {
			select {
			case msg := <- kv.applyCh:
				op, ok := msg.Command.(Op);
				if ok {
					kv.mu.Lock();

					if op.SequenceNumber > kv.commandMap[op.ClientId] {
						if op.Action == "Put" {
							kv.keyValueMap[op.Key] = op.Value;
						} else if op.Action == "Append" {
							kv.keyValueMap[op.Key] = kv.keyValueMap[op.Key] + op.Value;
						}
						kv.commandMap[op.ClientId] = op.SequenceNumber; //update the sequence number
					}

					_, channelExists := kv.res[msg.Index];

					if channelExists {
						kv.res[msg.Index] <- op;
					} else {
						kv.res[msg.Index] = make(chan Op, 1);
					}
					
					kv.mu.Unlock();
				}
				
			}
		}
	}();

	return kv;
}
