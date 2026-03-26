package raft

import (
	"errors"
	"log"
	"net"
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

	pendingProposals   map[int]chan ApplyResult
	resetElectionCh    chan struct{}
	triggerReplicateCh chan struct{}
	stopCh             chan struct{}
}

func NewNode(id, address string, peers []string) *Node {
	return &Node{
		id:                 id,
		address:            address,
		peers:              peers,
		state:              Follower,
		resetElectionCh:    make(chan struct{}, 1),
		triggerReplicateCh: make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
		KVStore:            NewKVStore(),
		pendingProposals:   make(map[int]chan ApplyResult),
	}
}

func (n *Node) Start() error {
	log.Printf("STARTING node=%s address=%s peers=%v", n.id, n.address, n.peers)

	if err := n.startRPCServer(); err != nil {
		return err
	}

	n.wg.Add(2)
	go n.runElectionTimer()
	go n.applyLoop()
	return nil
}

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

// TODO remove
func (n *Node) GetLogInfo() (logLen int, commitIndex int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.log), n.commitIndex
}

func (n *Node) stepDown(newTerm int) {
	n.currentTerm = newTerm
	n.state = Follower
	n.votedFor = ""
	log.Printf("node=%s STEPPED DOWN term=%d->%d", n.id, n.currentTerm, newTerm)
	n.resetElectionTimer()
}

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

func (n *Node) Propose(cmd Command, timeout time.Duration) (ApplyResult, error) {
	n.mu.Lock()
	if n.state != Leader {
		defer n.mu.Unlock()
		return ApplyResult{}, errors.New("not a leader, leader=" + n.leaderID)
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

	n.triggerReplicate()
	n.mu.Unlock()

	select {
	case result := <-pendCh:
		return result, nil
	case <-time.After(timeout):
		delete(n.pendingProposals, entry.Index)
		return ApplyResult{}, errors.New("timed out")
	}
}

func (n *Node) GetKV(key string) (val string, ok bool, err error) {
	n.mu.Lock()
	if n.state != Leader {
		defer n.mu.Unlock()
		return "", false, errors.New("not a leader, leader=" + n.leaderID)
	}
	n.mu.Unlock()
	val, ok = n.KVStore.Get(key)
	return val, ok, nil
}

func (n *Node) GetAllKV() map[string]string {
	return n.KVStore.GetAll()
}
