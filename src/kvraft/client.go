package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64

	mu sync.Mutex
	leaderId int
	sequenceNumber int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk);
	ck.servers = servers;
	// You'll have to add code here.
	ck.clientId = nrand();
	ck.leaderId = -1;
	ck.sequenceNumber = 0;
	return ck;
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs;
	args.Key = key;
	args.ClientId = ck.clientId;

	ck.mu.Lock();
	ck.sequenceNumber = ck.sequenceNumber + 1;
	args.SequenceNumber = ck.sequenceNumber;
	ck.mu.Unlock();

	for {
		if ck.leaderId != -1 {
			var reply GetReply;
			ok := ck.servers[ck.leaderId].Call("RaftKV.Get", &args, &reply);
			if ok && !reply.WrongLeader {
				return reply.Value;
			}
		}

		for i := 0; i < len(ck.servers); i++ {
			var reply GetReply;
			ok := ck.servers[i].Call("RaftKV.Get", &args, &reply);
			if ok && !reply.WrongLeader {
				ck.leaderId = i;
				return reply.Value;
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs;
	args.Key = key;
	args.Value = value;
	args.Op = op;
	args.ClientId = ck.clientId;

	ck.mu.Lock();
	ck.sequenceNumber = ck.sequenceNumber + 1;
	args.SequenceNumber = ck.sequenceNumber;
	ck.mu.Unlock();

	for {
		for i := 0; i < len(ck.servers); i++ {
			var reply PutAppendReply;
			ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply);
			if ok && !reply.WrongLeader {
				return;
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
