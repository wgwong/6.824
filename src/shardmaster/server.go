package shardmaster

import (
	"raft"
	"labrpc"
	"sync"
	"encoding/gob"
	"sort"
	"time"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	result map[int] chan OpReply
	commandMap map[int64] int;
}


type Op struct {
	SequenceNumber int
	ClientId int64
	Action string
	Servers map[int][]string
	GIDs []int
	Shard int
	GID   int
	Num int
}

type OpReply struct {
	SequenceNumber int
	ClientId int64
	Config Config
	WrongLeader bool
	Err Err
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	var op Op;
	op.Action = "Join";
	op.SequenceNumber = args.SequenceNumber;
	op.ClientId = args.ClientId;
	op.Servers = args.Servers;

	var opReply OpReply;
	opReply.SequenceNumber = op.SequenceNumber;
	opReply.ClientId = op.ClientId;
	opReply.Err = "";
	opReply.WrongLeader = true;

	//send to raft
	index, _, isLeader := sm.rf.Start(op);
	if isLeader {
		sm.mu.Lock();
		_, ok := sm.result[index];
		if !ok {
			sm.result[index] = make(chan OpReply, 1);
		}
		replyChannel := sm.result[index];
		sm.mu.Unlock();
		timeoutTime := 100;

		select {
		case result := <- replyChannel:
			if result.SequenceNumber == op.SequenceNumber && result.ClientId == op.ClientId {
				opReply.WrongLeader = false;
				opReply.Config = result.Config;
			}  else {
				opReply.Err = "Error";
			}
		case <- time.After(time.Duration(timeoutTime) * time.Millisecond):
			opReply.Err = "TimeOut";
		}
	}

	sm.mu.Lock();
	delete(sm.result, index);
	sm.mu.Unlock();

	reply.WrongLeader = opReply.WrongLeader;
	reply.Err = "";
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	var op Op;
	op.Action = "Leave";
	op.SequenceNumber = args.SequenceNumber;
	op.ClientId = args.ClientId;
	op.GIDs = args.GIDs;

	var opReply OpReply;
	opReply.SequenceNumber = op.SequenceNumber;
	opReply.ClientId = op.ClientId;
	opReply.Err = "";
	opReply.WrongLeader = true;

	//send to raft
	index, _, isLeader := sm.rf.Start(op);
	if isLeader {
		sm.mu.Lock();
		_, ok := sm.result[index];
		if !ok {
			sm.result[index] = make(chan OpReply, 1);
		}
		replyChannel := sm.result[index];
		sm.mu.Unlock();
		timeoutTime := 100;

		select {
		case result := <- replyChannel:
			if result.SequenceNumber == op.SequenceNumber && result.ClientId == op.ClientId {
				opReply.WrongLeader = false;
				opReply.Config = result.Config;
			}  else {
				opReply.Err = "Error";
			}
		case <- time.After(time.Duration(timeoutTime) * time.Millisecond):
			opReply.Err = "TimeOut";
		}
	}

	reply.WrongLeader = opReply.WrongLeader;
	reply.Err = "";
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	var op Op;
	op.Action = "Move";
	op.SequenceNumber = args.SequenceNumber;
	op.ClientId = args.ClientId;
	op.Shard = args.Shard;
	op.GID = args.GID;

	var opReply OpReply;
	opReply.SequenceNumber = op.SequenceNumber;
	opReply.ClientId = op.ClientId;
	opReply.Err = "";
	opReply.WrongLeader = true;

	//send to raft
	index, _, isLeader := sm.rf.Start(op);
	if isLeader {
		sm.mu.Lock();
		_, ok := sm.result[index];
		if !ok {
			sm.result[index] = make(chan OpReply, 1);
		}
		replyChannel := sm.result[index];
		sm.mu.Unlock();
		timeoutTime := 100;

		select {
		case result := <- replyChannel:
			if result.SequenceNumber == op.SequenceNumber && result.ClientId == op.ClientId {
				opReply.WrongLeader = false;
				opReply.Config = result.Config;
			}  else {
				opReply.Err = "Error";
			}
		case <- time.After(time.Duration(timeoutTime) * time.Millisecond):
			opReply.Err = "TimeOut";
		}
	}

	reply.WrongLeader = opReply.WrongLeader;
	reply.Err = "";
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	var op Op;
	op.Action = "Query";
	op.SequenceNumber = args.SequenceNumber;
	op.ClientId = args.ClientId;
	op.Num = args.Num;

	var opReply OpReply;
	opReply.SequenceNumber = op.SequenceNumber;
	opReply.ClientId = op.ClientId;
	opReply.Err = "";
	opReply.WrongLeader = true;

	//send to raft
	index, _, isLeader := sm.rf.Start(op);
	if isLeader {
		sm.mu.Lock();
		_, ok := sm.result[index];
		if !ok {
			sm.result[index] = make(chan OpReply, 1);
		}
		replyChannel := sm.result[index];
		sm.mu.Unlock();
		timeoutTime := 100;

		select {
		case result := <- replyChannel:
			if result.SequenceNumber == op.SequenceNumber && result.ClientId == op.ClientId {
				opReply.WrongLeader = false;
				opReply.Config = result.Config;
			}  else {
				opReply.Err = "Error";
			}
		case <- time.After(time.Duration(timeoutTime) * time.Millisecond):
			opReply.Err = "TimeOut";
		}
	}

	reply.WrongLeader = opReply.WrongLeader;
	reply.Err = "";
	reply.Config = opReply.Config;
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.configs[0].Num = 0;
	sm.configs[0].Shards = [NShards]int{};

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.result = make(map[int]chan OpReply);
	sm.commandMap = make(map[int64]int);

	go func() {
		for {
			select {
			case applyMsg := <-sm.applyCh:
				op, ok := applyMsg.Command.(Op)
				if ok {
					sm.mu.Lock();

					var opReply OpReply;
					opReply.ClientId = op.ClientId;
					opReply.SequenceNumber = op.SequenceNumber;
					opReply.WrongLeader = true;

					if sm.commandMap[op.ClientId] < op.SequenceNumber {
						if op.Action == "Join" {
							opReply.WrongLeader = false;

							//new config
							var newConfig Config;
							newConfig.Num = len(sm.configs);
							newConfig.Shards = [NShards] int{};
							newConfig.Groups = make(map[int][]string);

							for key, val := range sm.configs[len(sm.configs)-1].Groups {
								newConfig.Groups[key] = val;
							}

							for i := 0; i < len(newConfig.Shards); i++ {
								newConfig.Shards[i] = sm.configs[len(sm.configs)-1].Shards[i];
							}

							//join
							for key, val := range op.Servers {
								newConfig.Groups[key] = val;
							}

							var gidList [] int;
							for k := range newConfig.Groups {
								gidList = append(gidList, k)
							}
							sort.Ints(gidList);

							for i := 0; i < len(newConfig.Shards); i++ {
								newConfig.Shards[i] = gidList[i % len(gidList)];
							}

							sm.configs = append(sm.configs, newConfig);

						} else if op.Action == "Leave" {
							opReply.WrongLeader = false;

							//new config
							var newConfig Config;
							newConfig.Num = len(sm.configs);
							newConfig.Shards = [NShards] int{};
							newConfig.Groups = make(map[int][]string);

							for key, val := range sm.configs[len(sm.configs)-1].Groups {
								newConfig.Groups[key] = val;
							}

							for i := 0; i < len(newConfig.Shards); i++ {
								newConfig.Shards[i] = sm.configs[len(sm.configs)-1].Shards[i];
							}

							//leave
							for _, val := range op.GIDs {
								delete(newConfig.Groups, val);
							}

							var gidList [] int;
							for k := range newConfig.Groups {
								gidList = append(gidList, k)
							}
							sort.Ints(gidList);

							for i := 0; i < len(newConfig.Shards); i ++ {
								newConfig.Shards[i] = gidList[i % len(gidList)];
							}

							sm.configs = append(sm.configs, newConfig);

						} else if op.Action == "Move" {
							opReply.WrongLeader = false;
							
							//new config
							var newConfig Config;
							newConfig.Num = len(sm.configs);
							newConfig.Shards = [NShards] int{};
							newConfig.Groups = make(map[int][]string);

							for key, val := range sm.configs[len(sm.configs)-1].Groups {
								newConfig.Groups[key] = val;
							}

							for i := 0; i < len(newConfig.Shards); i++ {
								newConfig.Shards[i] = sm.configs[len(sm.configs)-1].Shards[i];
							}

							//move
							newConfig.Shards[op.Shard] = op.GID;
							sm.configs = append(sm.configs, newConfig);

						} else if op.Action == "Query" {
							opReply.WrongLeader = false;
							num := op.Num;
							if num >= len(sm.configs) || num == -1 {
								opReply.Config = sm.configs[len(sm.configs) - 1];
							} else {
								opReply.Config = sm.configs[num];
							}
						}

						sm.commandMap[op.ClientId] = op.SequenceNumber

					} else if op.Action == "Query" {
						num := op.Num;
						if num >= len(sm.configs) || num == -1 {
							opReply.Config = sm.configs[len(sm.configs) - 1];
						} else {
							opReply.Config = sm.configs[num];
						}
					}

					_, channelExists := sm.result[applyMsg.Index];

					if channelExists {
						sm.result[applyMsg.Index] <- opReply;
					} else {
						sm.result[applyMsg.Index] = make(chan OpReply, 1);
					}

					sm.mu.Unlock();

				}
			}
		}
	}();

	return sm;
}