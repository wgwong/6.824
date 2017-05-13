package shardmaster

import (
	"raft"
	"labrpc"
	"sync"
	"encoding/gob"
	"time"
	"os"
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

	reply.WrongLeader = opReply.WrongLeader
	reply.Err = ""
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
	sm.rf.Kill();
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf;
}

func (sm *ShardMaster) RebalanceConfig(config *Config, action string, gid int) {
	shardsCount := map[int][]int{};
	for k := range config.Groups {
		shardsCount[k] = []int{};
	}
	for k, v := range config.Shards {
		shardsCount[v] = append(shardsCount[v], k);
	}

	if action == "Join" {
		for i := 0; i < NShards/len(config.Groups); i++ {
			max := -1;
			maxGid := 0;

			for g, shards := range shardsCount {
				if max < len(shards) {
					max = len(shards);
					maxGid = g;
				}
			}

			if len(shardsCount[maxGid]) == 0 {
				os.Exit(-1);
			}
			config.Shards[shardsCount[maxGid][0]] = gid;
			shardsCount[maxGid] = shardsCount[maxGid][1:];
		}
	} else if action == "Leave" {
		delShards := shardsCount[gid];
		delete(shardsCount, gid);
		for _,shard := range delShards {
			min := -1;
			minGid := 0;

			for g, shards := range shardsCount {
				if min > len(shards) || min == -1 {
					min = len(shards);
					minGid = g;
				}
			}

			config.Shards[shard] = minGid;
			shardsCount[minGid] = append(shardsCount[minGid],shard);
		}
	}
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

					// execute command
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
							for gid, servers := range op.Servers {
								_, found := newConfig.Groups[gid];
								if !found {
									newConfig.Groups[gid] = servers;
									sm.RebalanceConfig(&newConfig, "Join", gid);
								}
							}

							sm.configs = append(sm.configs, newConfig);

						} else if op.Action == "Leave" {
							opReply.WrongLeader = false;

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
							for _, gid := range op.GIDs {
								_, found := newConfig.Groups[gid];
								if found {
									delete(newConfig.Groups, gid);
									sm.RebalanceConfig(&newConfig, "Leave", gid);
								}
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

						sm.commandMap[op.ClientId] = op.SequenceNumber;
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
	}()

	return sm;
}