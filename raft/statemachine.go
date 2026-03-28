package raft

import (
	"log"
	"maps"
	"sync"
	"time"
)

type KVStore struct {
	mu           sync.RWMutex // concurrent reads but exclusive writes
	data         map[string]string
	leases       map[string]*Lease
	keyToLeaseID map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		data:         make(map[string]string),
		leases:       make(map[string]*Lease),
		keyToLeaseID: make(map[string]string),
	}
}

func (kv *KVStore) Apply(cmd Command) ApplyResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch cmd.Op {
	case "put":
		return kv.handlePut(cmd)
	case "delete":
		return kv.handleDelete(cmd)
	case "cas":
		return kv.handleCAS(cmd)
	case "leaseGrant":
		return kv.handleLeaseGrant(cmd)
	case "leaseRevoke":
		return kv.handleLeaseRevoke(cmd)
	case "leaseRenew":
		return kv.handleLeaseRenew(cmd)
	default:
		log.Printf("KV FAILED UNKNOWN OP %s", cmd.Op)
		return ApplyResult{Ok: false}
	}
}

func (kv *KVStore) handlePut(cmd Command) ApplyResult {
	kv.data[cmd.Key] = cmd.Value
	if cmd.LeaseID != "" {
		lease, ok := kv.leases[cmd.LeaseID]
		if !ok {
			log.Printf("KV PUT %s=%s lease=%s NOT FOUND", cmd.Key, cmd.Value, cmd.LeaseID)
			return ApplyResult{Value: cmd.Value, Ok: true}
		}
		if oldLeaseID, ok := kv.keyToLeaseID[cmd.Key]; ok {
			if oldLeaseID == cmd.LeaseID {
				log.Printf("KV PUT %s=%s lease=%s", cmd.Key, cmd.Value, cmd.LeaseID)
				return ApplyResult{Value: cmd.Value, Ok: true}
			}
			kv.removeKeyFromLease(cmd.Key, oldLeaseID)
		}
		lease.Keys = append(lease.Keys, cmd.Key)
		kv.keyToLeaseID[cmd.Key] = cmd.LeaseID
		log.Printf("KV PUT %s=%s lease=%s", cmd.Key, cmd.Value, cmd.LeaseID)
	} else {
		log.Printf("KV PUT %s=%s", cmd.Key, cmd.Value)
	}
	return ApplyResult{Value: cmd.Value, Ok: true}
}

func (kv *KVStore) handleDelete(cmd Command) ApplyResult {
	v, ok := kv.data[cmd.Key]
	delete(kv.data, cmd.Key)
	if leaseID, ok := kv.keyToLeaseID[cmd.Key]; ok {
		kv.removeKeyFromLease(cmd.Key, leaseID)
		delete(kv.keyToLeaseID, cmd.Key)
	}
	if !ok {
		log.Printf("KV DELETE FAILED key=%s NOT FOUND", cmd.Key)
	} else {
		log.Printf("KV DELETE %s=%v", cmd.Key, v)
	}
	return ApplyResult{Value: v, Ok: ok}
}

func (kv *KVStore) handleCAS(cmd Command) ApplyResult {
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
}

func (kv *KVStore) handleLeaseGrant(cmd Command) ApplyResult {
	expiresAt := cmd.GrantedAt.Add(cmd.TTL)
	lease := &Lease{
		ID:        cmd.LeaseID,
		TTL:       cmd.TTL,
		ExpiresAt: expiresAt,
		Keys:      []string{},
	}
	kv.leases[cmd.LeaseID] = lease
	log.Printf("KV LEASE GRANTED id=%s ttl=%v, expires=%v", lease.ID, lease.TTL, lease.ExpiresAt)
	return ApplyResult{LeaseID: cmd.LeaseID, Ok: true}
}

func (kv *KVStore) handleLeaseRevoke(cmd Command) ApplyResult {
	lease, ok := kv.leases[cmd.LeaseID]
	if !ok {
		log.Printf("KV LEASE REVOKE lease=%s not found", cmd.LeaseID)
		return ApplyResult{Ok: false}
	}
	for _, key := range lease.Keys {
		delete(kv.data, key)
		delete(kv.keyToLeaseID, key)
		log.Printf("KV LEASE REVOKE key=%s", key)
	}
	delete(kv.leases, cmd.LeaseID)
	log.Printf("KV LEASE REVOKED lease=%s", cmd.LeaseID)
	return ApplyResult{LeaseID: cmd.LeaseID, Ok: true}
}

func (kv *KVStore) handleLeaseRenew(cmd Command) ApplyResult {
	lease, ok := kv.leases[cmd.LeaseID]
	if !ok {
		log.Printf("KV LEASE RENEW FAILED lease=%s not found", cmd.LeaseID)
		return ApplyResult{Ok: false}
	}
	lease.ExpiresAt = cmd.GrantedAt.Add(lease.TTL)
	log.Printf("KV LEASE RENEWED id=%s ttl=%v expires=%v", lease.ID, lease.TTL, lease.ExpiresAt)
	return ApplyResult{LeaseID: lease.ID, Ok: true}
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

func (kv *KVStore) removeKeyFromLease(key, leaseID string) {
	lease, ok := kv.leases[leaseID]
	if !ok {
		return
	}
	for i, k := range lease.Keys {
		if k == key {
			lease.Keys = append(lease.Keys[:i], lease.Keys[i+1:]...)
			return
		}
	}
}

func (kv *KVStore) GetExpiredLeases() []string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	now := time.Now()
	var expired []string
	for id, lease := range kv.leases {
		if now.After(lease.ExpiresAt) {
			expired = append(expired, id)
		}
	}
	return expired
}

func (kv *KVStore) GetLease(id string) (*Lease, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	lease, ok := kv.leases[id]
	if !ok {
		return nil, false
	}
	leaseCpy := &Lease{
		ID:        lease.ID,
		TTL:       lease.TTL,
		ExpiresAt: lease.ExpiresAt,
		Keys:      make([]string, len(lease.Keys)),
	}
	// ai code review recommended to copy the keys instead of returning keys directly
	// because otherwise you will copy the reference and modify underlying array
	copy(leaseCpy.Keys, lease.Keys)
	return leaseCpy, true
}

func (kv *KVStore) GetAllLeases() map[string]*Lease {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	result := make(map[string]*Lease, len(kv.leases))
	for id, lease := range kv.leases {
		leaseCpy := &Lease{
			ID:        lease.ID,
			TTL:       lease.TTL,
			ExpiresAt: lease.ExpiresAt,
			Keys:      make([]string, len(lease.Keys)),
		}
		copy(leaseCpy.Keys, lease.Keys)
		result[id] = leaseCpy
	}
	return result
}
