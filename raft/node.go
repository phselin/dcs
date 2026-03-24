package raft

import (
	"log"
	"math/rand"
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

	leaderID string
	votedFor string

	log []LogEntry

	listener net.Listener

	resetElectionCh chan struct{}
	stopCh          chan struct{}
}

func NewNode(id, address string, peers []string) *Node {
	return &Node{
		id:              id,
		address:         address,
		peers:           peers,
		state:           Follower,
		resetElectionCh: make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
	}
}

func (n *Node) Start() error {
	log.Printf("STARTING node=%s address=%s peers=%v", n.id, n.address, n.peers)

	if err := n.startRPCServer(); err != nil {
		return err
	}

	n.wg.Add(1)
	go n.runElectionTimer()
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

func randomElectionTimeout() time.Duration {
	return MinElectionTimeout + time.Duration(rand.Int63n(int64(MaxElectionTimeout-MinElectionTimeout)))
}

func (n *Node) runElectionTimer() {
	defer n.wg.Done()

	for {
		timeout := randomElectionTimeout()
		select {
		case <-time.After(timeout):
			n.mu.Lock()
			isLeader := n.state == Leader
			n.mu.Unlock()
			if !isLeader {
				n.startElection()
			}
		case <-n.resetElectionCh:
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) resetElectionTimer() {
	select {
	case n.resetElectionCh <- struct{}{}:
	default:
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	term := n.currentTerm
	lastTerm, lastIndex := n.lastLogTermIndex()
	log.Printf("node=%s STARTING ELECTION term=%d", n.id, term)
	n.mu.Unlock()

	votes := 1
	majority := (len(n.peers)+1)/2 + 1 // majority of all nodes (peer + self)

	for _, peer := range n.peers {
		go func(addr string) {
			reply, err := n.callRequestVote(addr, &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogTerm:  lastTerm,
				LastLogIndex: lastIndex,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.currentTerm != term || n.state != Candidate {
				return
			}

			if reply.Term > n.currentTerm {
				n.stepDown(reply.Term)
				return
			}

			if reply.VoteGranted {
				votes++
				if votes >= majority {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

func (n *Node) becomeLeader() {
	if n.state != Candidate {
		return
	}
	n.state = Leader
	n.leaderID = n.id
	log.Printf("node=%s BECAME LEADER term=%d", n.id, n.currentTerm)
	n.wg.Add(1)
	go n.runHeartbeats()
}

func (n *Node) stepDown(newTerm int) {
	n.currentTerm = newTerm
	n.state = Follower
	n.votedFor = ""
	log.Printf("node=%s STEPPED DOWN term=%d->%d", n.id, n.currentTerm, newTerm)
	n.resetElectionTimer()
}

func (n *Node) runHeartbeats() {
	defer n.wg.Done()

	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	n.sendHeartbeats()

	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			isLeader := n.state == Leader
			n.mu.Unlock()
			if !isLeader {
				return
			}
			n.sendHeartbeats()
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	term := n.currentTerm
	n.mu.Unlock()

	for _, peer := range n.peers {
		go func(addr string) {
			reply, err := n.callAppendEntries(addr, &AppendEntriesArgs{
				Term:     term,
				LeaderID: n.id,
			})
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.currentTerm {
				n.stepDown(reply.Term)
			}
		}(peer)
	}
}

func (n *Node) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.currentTerm
	reply.VoteGranted = false
	if args.Term < n.currentTerm {
		return nil
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	lastTerm, lastIndex := n.lastLogTermIndex()
	if (n.votedFor == "" || n.votedFor == args.CandidateID) &&
		(args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)) {
		reply.VoteGranted = true
		n.resetElectionTimer()
		log.Printf("node=%s VOTED peer=%s term=%d", n.id, args.CandidateID, n.currentTerm)
	}
	return nil
}

func (n *Node) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	reply.Term = n.currentTerm
	reply.Success = false
	if args.Term < n.currentTerm {
		return nil
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	n.state = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimer()
	reply.Success = true
	return nil
}

func (n *Node) lastLogTermIndex() (term int, index int) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Term, last.Index
}
