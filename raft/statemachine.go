package raft

import (
	"log"
	"maps"
	"sync"
)

type KVStore struct {
	mu   sync.RWMutex // concurrent reads but exclusive writes
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
		if !ok {
			log.Printf("KV DELETE FAILED key=%s NOT FOUND", cmd.Key)
		} else {
			log.Printf("KV DELETE %s=%v", cmd.Key, v)
		}
		return ApplyResult{Value: v, Ok: ok}
	case "cas":
		v, ok := kv.data[cmd.Key]
		if !ok && cmd.Expected != "" {
			log.Printf("KV CAS FAILED key=%s NOT FOUND", cmd.Key)
			return ApplyResult{Value: "", Ok: false}
		}
		if ok && cmd.Expected != v {
			log.Printf("KV CAS FAILED key=%s current=%s expected=%s", cmd.Key, v, cmd.Expected)
			return ApplyResult{Value: v, Ok: false}
		}
		kv.data[cmd.Key] = cmd.Value
		log.Printf("KV CAS key=%s val=%s->%s", cmd.Key, v, cmd.Value)
		return ApplyResult{Value: cmd.Value, Ok: true}
	default:
		log.Printf("KV FAILED UNKNOWN OP %s", cmd.Op)
		return ApplyResult{Ok: false}
	}
}

func (kv *KVStore) Get(key string) (val string, ok bool) {
	kv.mu.RLock() // allow concurrent reads
	defer kv.mu.RUnlock()
	val, ok = kv.data[key]
	if !ok {
		log.Printf("KV GET FAILED key=%s NOT FOUND", key)
	}
	return val, ok
}

func (kv *KVStore) GetAll() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	kvcpy := make(map[string]string, len(kv.data))
	maps.Copy(kvcpy, kv.data)
	return kvcpy
}
