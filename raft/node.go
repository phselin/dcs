package raft

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type Node struct {
	mu sync.Mutex
	wg sync.WaitGroup

	id      string
	address string
	peers   []string

	state       State
	currentTerm int
	votedFor    string

	log         []LogEntry
	commitIndex int
	lastApplied int

	leaderID   string
	nextIndex  map[string]int
	matchIndex map[string]int

	listener net.Listener

	KVStore *KVStore

	dataDir string

	leaseCounter int

	pendingProposals   map[int]chan ApplyResult // receiving and sending replication result for an index entry
	resetElectionCh    chan struct{}
	triggerReplicateCh chan struct{}
	stopCh             chan struct{}
}

func NewNode(id, address string, peers []string, dataDir string) *Node {
	return &Node{
		id:                 id,
		address:            address,
		peers:              peers,
		state:              Follower, // all nodes start out as followers
		resetElectionCh:    make(chan struct{}, 1),
		triggerReplicateCh: make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
		KVStore:            NewKVStore(),
		pendingProposals:   make(map[int]chan ApplyResult),
		dataDir:            dataDir,
	}
}

// starts RPC server, create directory to store node state, start periodic election timer and periodic apply entries loop
func (n *Node) Start() error {
	log.Printf("STARTING node=%s address=%s peers=%v", n.id, n.address, n.peers)

	if n.dataDir != "" {
		os.MkdirAll(n.dataDir, 0755)
		n.loadState()
	}

	if err := n.startRPCServer(); err != nil {
		return err
	}

	n.wg.Add(3)
	go n.runElectionTimer()
	go n.applyLoop()
	go n.leaseLoop()
	return nil
}

// to gracefully shut down a node
func (n *Node) Stop() {
	close(n.stopCh)
	if n.listener != nil {
		n.listener.Close()
	}
	n.wg.Wait()
	log.Printf("node=%s STOPPED", n.id)
}

func (n *Node) GetStatus() (id string, state State, term int, leader string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.id, n.state, n.currentTerm, n.leaderID
}

func (n *Node) GetLogInfo() (logLen, lastApplied, commitIndex int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.log), n.lastApplied, n.commitIndex
}

// steps down to follower and updates its term to highest term seen so far
// clears it votes
// usually called during election but that's not always the case
func (n *Node) stepDown(newTerm int) {
	n.currentTerm = newTerm
	n.state = Follower
	n.votedFor = ""
	log.Printf("node=%s STEPPED DOWN term=%d->%d", n.id, n.currentTerm, newTerm)
	n.persistState() // change in term, state and votedFor
	n.resetElectionTimer()
}

// returms the index and term of the last entry in the log
func (n *Node) lastLogIndexTerm() (index int, term int) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func (n *Node) logTerm(index int) int {
	if index <= 0 || index > len(n.log) {
		return 0
	}
	return n.log[index-1].Term
}

// all nodes
// runs periodically to apply entries upto the commit index
// commit index stores the index value for the entry that is present in the majority of logs
func (n *Node) applyLoop() {
	defer n.wg.Done()

	for {
		select {
		case <-n.stopCh:
			return
		case <-time.After(ApplyInterval):
		}

		n.mu.Lock()
		var entries []LogEntry
		for n.lastApplied < n.commitIndex {
			entries = append(entries, n.log[n.lastApplied])
			n.lastApplied++
		}
		n.mu.Unlock()

		for _, entry := range entries {
			result := n.KVStore.Apply(entry.Command)
			log.Printf("node=%s APPLIED index=%d term=%d op=%s key=%s", n.id, entry.Index, entry.Term, entry.Command.Op, entry.Command.Key)
			n.mu.Lock()
			if ch, ok := n.pendingProposals[entry.Index]; ok {
				ch <- result
				delete(n.pendingProposals, entry.Index)
			}
			n.mu.Unlock()
		}
	}
}

// leader only
// adds an entry to its log and replicates it to its peers
// returns error if it times out without any result
// returns the result (success or failure) otherwise
func (n *Node) Propose(cmd Command, timeout time.Duration) (ApplyResult, error) {
	n.mu.Lock()
	if n.state != Leader {
		defer n.mu.Unlock()
		return ApplyResult{}, fmt.Errorf("%w, leader=%s", ErrNotLeader, n.leaderID)
	}

	lastIndex, _ := n.lastLogIndexTerm()
	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   lastIndex + 1,
		Command: cmd,
	}
	n.log = append(n.log, entry)
	n.matchIndex[n.id] = entry.Index

	pendCh := make(chan ApplyResult, 1)
	n.pendingProposals[entry.Index] = pendCh

	log.Printf("node=%s PROPOSED index=%d op=%s key=%s val=%v", n.id, entry.Index, cmd.Op, cmd.Key, cmd.Value)
	n.persistState() // new log entry
	n.triggerReplicate()
	n.mu.Unlock()

	select {
	case result := <-pendCh:
		if result.Ok {
			log.Printf("APPLY SUCCESS op=%s key=%s value=%v", cmd.Op, cmd.Key, result.Value)
		} else {
			log.Printf("APPLY FAILED op=%s key=%s", cmd.Op, cmd.Key)
		}
		return result, nil
	case <-time.After(timeout):
		n.mu.Lock()
		delete(n.pendingProposals, entry.Index)
		n.mu.Unlock()
		return ApplyResult{}, errors.New("timed out")
	}
}

// leader only
// confirming leadership before a read is required to make reads linearizable
func (n *Node) GetKV(key string) (val string, ok bool, err error) {
	n.mu.Lock()
	if n.state != Leader {
		defer n.mu.Unlock()
		return "", false, fmt.Errorf("%w, leader=%s", ErrNotLeader, n.leaderID)
	}
	n.mu.Unlock()

	if !n.confirmLeadership() {
		return "", false, errors.New("lost leadership")
	}

	val, ok = n.KVStore.Get(key)
	return val, ok, nil
}

// all nodes
// for debugging purposes (can be called for any node)
func (n *Node) GetAllKV() map[string]string {
	return n.KVStore.GetAll()
}

// leader only
// make a rpc on peers to append entries without any log arguments
// (so no entries are added to the log but you get a success/failure reply)
// if majority replies, leadership is confirmed
func (n *Node) confirmLeadership() bool {
	n.mu.Lock()
	if n.state != Leader {
		defer n.mu.Unlock()
		return false
	}
	term := n.currentTerm
	n.mu.Unlock()

	type heartbeatResult struct {
		success bool
	}

	resultCh := make(chan heartbeatResult, len(n.peers))

	for _, peer := range n.peers {
		go func(addr string) {
			reply, err := n.callAppendEntries(addr, &AppendEntriesArgs{
				Term:     term,
				LeaderID: n.id,
			})
			if err != nil {
				resultCh <- heartbeatResult{success: false}
				return
			}
			resultCh <- heartbeatResult{success: reply.Success && reply.Term == term}
		}(peer)
	}

	votes := 1
	majority := (len(n.peers)+1)/2 + 1
	responded := 0
	for responded < len(n.peers) {
		select {
		case res := <-resultCh:
			responded++
			if res.success {
				votes++
				if votes >= majority {
					return true
				}
			}
		case <-time.After(RPCTimeout):
			return votes >= majority
		}
	}
	return votes >= majority
}

func (n *Node) leaseLoop() {
	defer n.wg.Done()

	for {
		select {
		case <-n.stopCh:
			return
		case <-time.After(LeaseInterval):
		}
		n.mu.Lock()
		isLeader := n.state == Leader
		n.mu.Unlock()
		if !isLeader {
			continue
		}
		expired := n.KVStore.GetExpiredLeases()
		for _, leaseID := range expired {
			log.Printf("node=%s lease=%s EXPIRED", n.id, leaseID)
			n.RevokeLease(leaseID)
		}
	}
}

// generate a lease id based on current term + counter
func (n *Node) generateLeaseID() string {
	n.leaseCounter++
	return fmt.Sprintf("lease-%d-%d", n.currentTerm, n.leaseCounter)
}

func (n *Node) GrantLease(ttl time.Duration) (ApplyResult, error) {
	n.mu.Lock()
	leaseID := n.generateLeaseID()
	n.mu.Unlock()

	cmd := Command{
		Op:        "leaseGrant",
		LeaseID:   leaseID,
		TTL:       ttl,
		GrantedAt: time.Now(),
	}
	return n.Propose(cmd, ProposeTimeout)
}

func (n *Node) RevokeLease(leaseID string) (ApplyResult, error) {
	cmd := Command{
		Op:      "leaseRevoke",
		LeaseID: leaseID,
	}
	return n.Propose(cmd, ProposeTimeout)
}

func (n *Node) RenewLease(leaseID string) (ApplyResult, error) {
	cmd := Command{
		Op:        "leaseRenew",
		LeaseID:   leaseID,
		GrantedAt: time.Now(),
	}
	return n.Propose(cmd, ProposeTimeout)
}

// leader only
func (n *Node) GetLease(leaseID string) (*Lease, bool, error) {
	n.mu.Lock()
	if n.state != Leader {
		defer n.mu.Unlock()
		return nil, false, fmt.Errorf("%w, leader=%s", ErrNotLeader, n.leaderID)
	}
	n.mu.Unlock()
	lease, ok := n.KVStore.GetLease(leaseID)
	return lease, ok, nil
}

func (n *Node) GetAllLeases() map[string]*Lease {
	return n.KVStore.GetAllLeases()
}
