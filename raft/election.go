package raft

import (
	"log"
	"math/rand"
	"time"
)

func randomElectionTimeout() time.Duration {
	return MinElectionTimeout + time.Duration(rand.Int63n(int64(MaxElectionTimeout-MinElectionTimeout)))
}

// starts an election every randomized timeout unless election is reset
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

// resets election timer
// tries to send a signal to reset election channel with a buffer capacity of one
// if it fails, the election has been already reset by some other process
func (n *Node) resetElectionTimer() {
	select {
	case n.resetElectionCh <- struct{}{}:
	default:
	}
}

// starts an election in a new term and elects itself as candidate and requests vote from its peers
// upon receiving majority it becomes a leader
// upon receiving a reply from a node with a higher term, it steps down to a follower
func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	term := n.currentTerm
	lastIndex, lastTerm := n.lastLogIndexTerm()
	n.persistState() // change in term and votedFor
	log.Printf("node=%s STARTING ELECTION term=%d", n.id, term)
	n.mu.Unlock()

	votes := 1                         // self
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

// change state to leader and update leader id to itself
// create map for nextIndex and matchIndex for its peers
// start sending heartbeats
func (n *Node) becomeLeader() {
	if n.state != Candidate {
		return
	}
	n.state = Leader
	n.leaderID = n.id

	lastIndex, _ := n.lastLogIndexTerm()
	n.nextIndex = make(map[string]int)
	n.matchIndex = make(map[string]int)
	n.matchIndex[n.id] = lastIndex
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}

	log.Printf("node=%s BECAME LEADER term=%d", n.id, n.currentTerm)

	n.wg.Add(1)
	go n.runHeartbeats()
}

// grants a vote if
// requesting node has higher term
// requesting node has same the term and does not have a lower log index
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
	lastIndex, lastTerm := n.lastLogIndexTerm()
	if (n.votedFor == "" || n.votedFor == args.CandidateID) &&
		(args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)) {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.persistState() // change in votedFor
		n.resetElectionTimer()
		log.Printf("node=%s VOTED peer=%s term=%d", n.id, args.CandidateID, n.currentTerm)
	}
	return nil
}
