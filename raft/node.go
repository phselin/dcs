package raft

import (
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
		case <-time.After(10 * time.Millisecond):
		}

		n.mu.Lock()
		var entries []LogEntry
		for n.lastApplied < n.commitIndex {
			entries = append(entries, n.log[n.lastApplied])
			n.lastApplied++
		}
		n.mu.Unlock()

		for _, entry := range entries {
			log.Printf("node=%s APPLIED index=%d term=%d cmd=%v", n.id, entry.Index, entry.Term, entry.Command)
		}
	}
}
