package shardkv

import (
	"labrpc"
	"raft"
	"sync"
	"encoding/gob"
	"time"
	"bytes"
	"shardmaster"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Action string
	Args interface{}
}

type OpReply struct {
	Action string
	Args interface{}
	Reply interface{}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyValueMap [shardmaster.NShards] map[string] string
	commandMap map[int64] int

	notificationChannel map[int] chan OpReply
	mck *shardmaster.Clerk
	config shardmaster.Config
}

func (kv *ShardKV) snapshot(index int) {
	kv.mu.Lock();
	if kv.rf.RaftStateSize() > kv.maxraftstate && kv.maxraftstate > 0 {
		w := new(bytes.Buffer);
		e := gob.NewEncoder(w);
		e.Encode(kv.keyValueMap);
		e.Encode(kv.commandMap);
		e.Encode(kv.config);
		data := w.Bytes();
		go kv.rf.CreateSnapshot(data, index);
	}
	kv.mu.Unlock();
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op Op;
	op.Action = "Get";
	op.Args = *args;

	reply.WrongLeader = true;
	reply.Err = "";

	index, _, isLeader := kv.rf.Start(op);

	notificationTimeout := 200 * time.Millisecond;

	if isLeader {
		var channel chan OpReply;
		kv.mu.Lock();

		_, found := kv.notificationChannel[index];
		if !found {
			kv.notificationChannel[index] = make(chan OpReply, 1);
		}
		channel = kv.notificationChannel[index];
		kv.mu.Unlock();

		select {
		case response := <- channel:
			rargs, ok := response.Args.(GetArgs);
			if ok {
				if rargs.ClientId == args.ClientId && rargs.SequenceNumber == args.SequenceNumber {
					*reply = response.Reply.(GetReply);
					reply.WrongLeader = false;
				}
			} else {
				reply.Err = "Error";
			}
		case <- time.After(notificationTimeout):
			reply.Err = "TimeOut";
		}
	}
	kv.mu.Lock();
	delete(kv.notificationChannel, index);
	kv.mu.Unlock();
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op Op;
	op.Action = args.Op;
	op.Args = *args;

	reply.WrongLeader = true;
	reply.Err = "";

	index, _, isLeader := kv.rf.Start(op);

	notificationTimeout := 200 * time.Millisecond;

	if isLeader {
		var channel chan OpReply;
		kv.mu.Lock();

		_, found := kv.notificationChannel[index];
		if !found {
			kv.notificationChannel[index] = make(chan OpReply, 1);
		}
		channel = kv.notificationChannel[index];
		kv.mu.Unlock();

		select {
		case response := <- channel:
			rargs, ok := response.Args.(PutAppendArgs);
			if ok {
				if rargs.ClientId == args.ClientId && rargs.SequenceNumber == args.SequenceNumber {
					*reply = response.Reply.(PutAppendReply);
					reply.WrongLeader = false;
				}
			} else {
				reply.Err = "Error";
			}
		case <- time.After(notificationTimeout):
			reply.Err = "TimeOut";
		}
	}
	kv.mu.Lock();
	delete(kv.notificationChannel, index);
	kv.mu.Unlock();
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill();
	// Your code here, if desired.
}



func (kv *ShardKV) FetchShards(args *TransferShardArgs, reply *TransferShardReply) {
	kv.mu.Lock();
	defer kv.mu.Unlock();

	if args.ConfigNum > kv.config.Num {
		reply.Err = "NotReady";
		return;
	}

	reply.Err = "OK";

	reply.ClientRequests = make(map[int64]int);
	reply.Transferred = [shardmaster.NShards]map[string]string{};
	for i := range reply.Transferred {
		reply.Transferred[i] = make(map[string]string);
	}

	for _, shard := range args.Shards {
		for k, v := range kv.keyValueMap[shard] {
			reply.Transferred[shard][k] = v;
		}
	}

	for command, val := range kv.commandMap {
		reply.ClientRequests[command] = val;
	}
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{});
	gob.Register(GetArgs{});
	gob.Register(GetReply{});
	gob.Register(PutAppendArgs{});
	gob.Register(PutAppendReply{});
	gob.Register(TransferShardArgs{});
	gob.Register(TransferShardReply{});
	gob.Register(ReConfigArgs{});
	gob.Register(ReConfigReply{});
	gob.Register(shardmaster.Config{});

	kv := new(ShardKV);
	kv.me = me;
	kv.maxraftstate = maxraftstate;
	kv.make_end = make_end;
	kv.gid = gid;
	kv.masters = masters;

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg);
	kv.rf = raft.Make(servers, me, persister, kv.applyCh);

	// Your initialization code here.
	for i := range kv.keyValueMap {
		kv.keyValueMap[i] = make(map[string]string);
	}
	kv.notificationChannel = make(map[int]chan OpReply);
	kv.commandMap = make(map[int64]int);
	kv.mck = shardmaster.MakeClerk(masters);

	go func() {
		notificationTimeout := 200 * time.Millisecond;
		sleepTime := 100 * time.Millisecond;
		for {
			if _, isLeader := kv.rf.GetState(); isLeader {
				cf := kv.mck.Query(-1);
				kv.mu.Lock();
				startNum := kv.config.Num;
				kv.mu.Unlock();
				for i := startNum +1; i <= cf.Num; i++ {
					newConfig := kv.mck.Query(i);

					ok := true;
					var reConfigArgs ReConfigArgs;
					reConfigArgs.NewConfig = newConfig;
					for i := 0; i < shardmaster.NShards; i++ {
						reConfigArgs.Kvs[i] = make(map[string]string);
					}
					reConfigArgs.ClientRequests = make(map[int64]int);

					wanted := map[int][]int{};
					kv.mu.Lock();
					
					for shard, gid := range newConfig.Shards {
						lastGid := kv.config.Shards[shard];
						if lastGid != kv.gid && lastGid != 0 && gid == kv.gid {
							_,ok := wanted[lastGid];
							if !ok {
								wanted[lastGid] = []int{shard};
							} else{
								wanted[lastGid] = append(wanted[lastGid],shard);
							}
						}
					}
					kv.mu.Unlock();

					var mutex sync.Mutex;
					var waitGroup sync.WaitGroup;

					for gid, shards := range wanted {
						waitGroup.Add(1);

						go func(gid int, shards []int) {
							defer waitGroup.Done();

							var args TransferShardArgs;
							args.Shards = shards;
							args.ConfigNum = newConfig.Num;

							servers := kv.config.Groups[gid];
							for si := 0; si < len(servers); si ++ {
								srv := kv.make_end(servers[si]);

								var reply TransferShardReply;

								ok := srv.Call("ShardKV.FetchShards", &args, &reply);
								if ok && reply.Err == "OK" {
									mutex.Lock();
									for shard, kvs := range reply.Transferred {
										for k, v := range kvs {
											reConfigArgs.Kvs[shard][k] = v;
										}
									}
									for command, val := range reply.ClientRequests {
										_, ok := reConfigArgs.ClientRequests[command];
										if !ok {
											reConfigArgs.ClientRequests[command] = val;
										} else {
											if reConfigArgs.ClientRequests[command] < val {
												reConfigArgs.ClientRequests[command] = val;
											}
										}
									}
									mutex.Unlock();

									return;
								}
							}

							mutex.Lock();
							ok = false;
							mutex.Unlock();

						}(gid,shards);
					}

					waitGroup.Wait();

					args := reConfigArgs;


					if ok {

						var reply ReConfigReply;

						var op Op;
						op.Action = "ReConfig";
						op.Args = args;

						reply.Err = "";

						index,_,isLeader := kv.rf.Start(op);
						if isLeader {
							var channel chan OpReply;
							kv.mu.Lock();
							_,ok := kv.notificationChannel[index];
							if !ok {
								kv.notificationChannel[index] = make(chan OpReply, 1);
							}
							channel = kv.notificationChannel[index];
							kv.mu.Unlock();

							select {
							case response := <- channel:
								if recArgs,ok := response.Args.(ReConfigArgs); ok {
									if recArgs.NewConfig.Num == args.NewConfig.Num {
										reply.Err = "OK";
									} else {
										reply.Err = "Error";
									}
								}
							case <- time.After(notificationTimeout):
								reply.Err = "TimeOut";
							}
						}

						kv.mu.Lock();
						delete(kv.notificationChannel, index);
						kv.mu.Unlock();

						if reply.Err != "OK" {
							break;
						}

					} else {
						break;
					}
				}
			}

			time.Sleep(sleepTime);
		}
	}();

	go func() {
		for {
			select {
			case response := <- kv.applyCh:
				if response.UseSnapshot {
					var lastIndex int;
					var lastTerm int;

					r := bytes.NewBuffer(response.Snapshot);
					d := gob.NewDecoder(r);

					kv.mu.Lock();

					d.Decode(&lastIndex);
					d.Decode(&lastTerm);
					kv.keyValueMap = [shardmaster.NShards]map[string]string{};
					for i := range kv.keyValueMap {
						kv.keyValueMap[i] = make(map[string]string);
					}
					kv.commandMap = make(map[int64]int);
					d.Decode(&kv.keyValueMap);
					d.Decode(&kv.commandMap);
					d.Decode(&kv.config);

					kv.mu.Unlock();
				}else {
					op, ok := response.Command.(Op);
					if ok {

						kv.mu.Lock();

						var opReplyVal OpReply;
						opReplyVal.Action = op.Action;
						opReplyVal.Args = op.Args;

						opReply := &opReplyVal;

						if op.Action == "Get" {
							var response GetReply;
							response.WrongLeader = true;
							response.Err = "Error";

							args, ok := op.Args.(GetArgs);
							if ok {
								shard := key2shard(args.Key);
								if kv.config.Shards[shard] == kv.gid {

									if args.SequenceNumber > kv.commandMap[args.ClientId] {
										kv.commandMap[args.ClientId] = args.SequenceNumber;
									}

									response.WrongLeader = false;
									key, val := kv.keyValueMap[shard][args.Key];
									if val {
										response.Err = "OK";
										response.Value = key;
									} else {
										response.Err = "ErrNoKey";
									}
								} else {
									response.Err = "ErrWrongGroup";
								}
							}

							opReply.Reply = response;

						} else if op.Action == "Put" {
							var response PutAppendReply;
							response.WrongLeader = true;
							response.Err = "Error";

							args, ok := op.Args.(PutAppendArgs);
							if ok {
								shard := key2shard(args.Key);
								if kv.config.Shards[shard] == kv.gid {
									if args.SequenceNumber > kv.commandMap[args.ClientId] {
										response.WrongLeader = false;
										response.Err = "OK";
										kv.keyValueMap[shard][args.Key] = args.Value;

										kv.commandMap[args.ClientId] = args.SequenceNumber;
									}
								} else {
									response.Err = "ErrWrongGroup";
								}
							}

							opReply.Reply = response;
						} else if op.Action == "Append" {
							var response PutAppendReply;
							response.WrongLeader = true;
							response.Err = "Error";

							args, ok := op.Args.(PutAppendArgs);
							if ok {
								shard := key2shard(args.Key);
								if kv.config.Shards[shard] == kv.gid {
									if args.SequenceNumber > kv.commandMap[args.ClientId] {
										response.WrongLeader = false;
										response.Err = "OK";
										kv.keyValueMap[shard][args.Key] += args.Value;

										kv.commandMap[args.ClientId] = args.SequenceNumber;
									}
								} else {
									response.Err = "ErrWrongGroup";
								}
							}

							opReply.Reply = response;
						} else if op.Action == "ReConfig" {
							var response ReConfigReply;
							response.Err = "Error";

							args, ok := op.Args.(ReConfigArgs);
							if ok {
								if args.NewConfig.Num > kv.config.Num {
									response.Err = "OK";

									for shard, data := range args.Kvs {
										for k, v := range data {
											kv.keyValueMap[shard][k] = v;
										}
									}

									for command, val := range args.ClientRequests {
										sequence, ok := kv.commandMap[command];
										if ok {
											if val > sequence {
												kv.commandMap[command] = val;
											}
										} else {
											kv.commandMap[command] = val;
										}
									}

									kv.config = args.NewConfig;
								}
							}

							opReply.Reply = response;
						}

						channel, ok := kv.notificationChannel[response.Index];
						if !ok {
							channel = make(chan OpReply, 1);
							kv.notificationChannel[response.Index] = channel;
						} else {
							channel <- *opReply;
						}
						kv.mu.Unlock();


						kv.snapshot(response.Index);
					}
				}
			}
		}
	}();

	return kv;
}