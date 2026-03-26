package raft

import (
	"log"
	"maps"
	"sync"
)

type KVStore struct {
	mu   sync.RWMutex
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
		log.Printf("KV DELETE %s=%v", cmd.Key, v)
		return ApplyResult{Ok: ok}
	default:
		log.Printf("KV UNKNOWN OP %s", cmd.Op)
		return ApplyResult{Ok: false}
	}
}

func (kv *KVStore) Get(key string) (val string, ok bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok = kv.data[key]
	return val, ok
}

func (kv *KVStore) GetAll() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	kvcpy := make(map[string]string, len(kv.data))
	maps.Copy(kvcpy, kv.data)
	return kvcpy
}
