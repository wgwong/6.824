package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	SequenceNumber int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SequenceNumber int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

/*
type TransferShardArgs struct {
	// You'll have to add definitions here.
	Shards []int
	ConfigNum int
}

type TransferShardReply struct {
	Err         Err
	Transferred [shardmaster.NShards]map[string]string
	ClientRequests map[int64]int64
}

type ReConfigArgs struct {
	// You'll have to add definitions here.
	Kvs [shardmaster.NShards]map[string]string
	ClientRequests map[int64]int64
	NewConfig shardmaster.Config
}

type ReConfigReply struct {
	Err         Err
}
*/