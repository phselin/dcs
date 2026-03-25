package raft

import (
	"log"
	"sync"
)

type KVStore struct {
	mu   sync.Mutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

func (kv *KVStore) Apply(cmd Command) ApplyResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch cmd.Op {
	case "put":
		kv.data[cmd.Key] = cmd.Value
		log.Printf("KV PUT %s=%s", cmd.Key, cmd.Value)
		return ApplyResult{Value: cmd.Value, Ok: true}
	case "delete":
		v, ok := kv.data[cmd.Key]
		delete(kv.data, cmd.Key)
		if ok {
			log.Printf("KV DELETE %s=%d", cmd.Key, v)
		}
		return ApplyResult{Ok: ok}
	default:
		log.Printf("KV UNKNOWN OP %s", cmd.Op)
		return ApplyResult{Ok: false}
	}
}

func (kv *KVStore) Get(key string) (val string, ok bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok = kv.data[key]
	return val, ok
}

func (kv *KVStore) GetAll() map[string]string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kvcpy := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		kvcpy[k] = v
	}
	return kvcpy
}
