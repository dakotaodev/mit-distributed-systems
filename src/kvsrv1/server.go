package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	cache map[string]KVValue
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.cache = make(map[string]KVValue)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.cache[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	} else {
		reply.Err = rpc.OK
		reply.Value = value.Value
		reply.Version = value.Version
	}

}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.cache[args.Key]
	if !ok {
		if args.Version == 0 {
			// acceptable new value
			kv.cache[args.Key] = KVValue{
				Value:   args.Value,
				Version: 1,
			}
			reply.Err = rpc.OK
			return
		} else {
			reply.Err = rpc.ErrNoKey
			return
		}
	}
	if value.Version == args.Version {
		// match, apply the update
		kv.cache[args.Key] = KVValue{
			Value:   args.Value,
			Version: value.Version + 1,
		}
		reply.Err = rpc.OK
		return
	} else {
		reply.Err = rpc.ErrVersion
		return
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
