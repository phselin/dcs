package raft

import (
	"errors"
	"log"
	"sort"
	"time"
)

func (n *Node) Propose(command any) (logIndex int, term int, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return 0, 0, errors.New("not leader")
	}

	lastIndex, _ := n.lastLogIndexTerm()
	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   lastIndex + 1,
		Command: command,
	}
	n.log = append(n.log, entry)
	n.matchIndex[n.id] = entry.Index

	log.Printf("node=%s PROPOSED index=%d term=%d cmd=%v", n.id, entry.Index, entry.Term, entry.Command)

	n.triggerReplicate()

	return entry.Index, entry.Term, nil
}

func (n *Node) triggerReplicate() {
	select {
	case n.triggerReplicateCh <- struct{}{}:
	default:
	}
}

func (n *Node) runHeartbeats() {
	defer n.wg.Done()

	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	n.replicateToAll()

	for {
		select {
		case <-ticker.C:
		case <-n.triggerReplicateCh:
		case <-n.stopCh:
			return
		}

		n.mu.Lock()
		isLeader := n.state == Leader
		n.mu.Unlock()
		if !isLeader {
			return
		}
		n.replicateToAll()
	}
}

func (n *Node) replicateToAll() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	n.mu.Unlock()

	for _, peer := range n.peers {
		go n.replicateToPeer(peer, term)
	}
}

func (n *Node) replicateToPeer(peer string, term int) {
	n.mu.Lock()

	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	nextIndex := n.nextIndex[peer]
	prevLogIndex := nextIndex - 1
	var entries []LogEntry
	if nextIndex <= len(n.log) {
		entries = make([]LogEntry, len(n.log)-nextIndex+1)
		copy(entries, n.log[nextIndex-1:])
	}

	args := &AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  n.logTerm(prevLogIndex),
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	reply, err := n.callAppendEntries(peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.stepDown(reply.Term)
		return
	}

	if n.state != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		newMatchIndex := prevLogIndex + len(entries)
		if newMatchIndex > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatchIndex
			n.nextIndex[peer] = newMatchIndex + 1
		}
		n.advanceCommitIndex()
	} else {
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
		log.Printf("node=%s REPLICATION peer=%s FAILED nextIndex=%d", n.id, peer, n.nextIndex[peer])
	}
}

func (n *Node) advanceCommitIndex() {
	matches := make([]int, 0, len(n.peers)+1)
	matches = append(matches, n.matchIndex[n.id])

	for _, peer := range n.peers {
		matches = append(matches, n.matchIndex[peer])
	}

	sort.Sort(sort.Reverse(sort.IntSlice(matches)))
	majorityPos := len(matches) / 2
	newCommitIndex := matches[majorityPos]

	if newCommitIndex > n.commitIndex && n.logTerm(newCommitIndex) == n.currentTerm {
		log.Printf("node=%s COMMIT ADVANCED %d->%d", n.id, n.commitIndex, newCommitIndex)
		n.commitIndex = newCommitIndex
	}
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

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(n.log) { // server crash scenario
			log.Printf("node=%s REJECT AppendEntries index=%d log=%d",
				n.id, args.PrevLogIndex, len(n.log))
			return nil
		}
		if n.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			n.log = n.log[:args.PrevLogIndex-1]
			log.Printf("node=%s REJECT AppendEntries CONFLICT index=%d",
				n.id, args.PrevLogIndex)
			return nil
		}
	}

	// overwrite entries only if there is a conflict
	for i, entry := range args.Entries {
		logIndex := args.PrevLogIndex + 1 + i
		if logIndex <= len(n.log) {
			if n.log[logIndex-1].Term != entry.Term {
				n.log = n.log[:logIndex-1]
				n.log = append(n.log, args.Entries[i:]...)
				break
			}
		} else {
			n.log = append(n.log, args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > n.commitIndex {
		lastIndex, _ := n.lastLogIndexTerm()
		n.commitIndex = min(args.LeaderCommit, lastIndex)
	}

	reply.Success = true

	if len(args.Entries) > 0 {
		log.Printf("node=%s REPLICATED entries=%d log=%d commit=%d ", n.id, len(args.Entries), len(n.log), n.commitIndex)
	}

	return nil
}
