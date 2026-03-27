package raft

import (
	"log"
	"sort"
	"time"
)

// leader only
// triggers replicate
// tries to send a signal to the trigger replicate channel with a buffer capacity of one
// if it fails, the replication has been already triggered by some other process
func (n *Node) triggerReplicate() {
	select {
	case n.triggerReplicateCh <- struct{}{}:
	default:
	}
}

// leader only
// replicates entries to peers periodically every heartbeat interval or when replication is triggered
func (n *Node) runHeartbeats() {
	defer n.wg.Done()

	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		n.mu.Lock()
		isLeader := n.state == Leader
		if !isLeader {
			n.mu.Unlock()
			return
		}
		term := n.currentTerm
		n.mu.Unlock()
		for _, peer := range n.peers {
			go n.replicateToPeer(peer, term)
		}
		select {
		case <-ticker.C:
		case <-n.triggerReplicateCh:
		case <-n.stopCh:
			return
		}
	}
}

// leader only
// makes a rpc on its peers to append entries
// entries are added starting from the last index in peer's log known by the leader
// up to the latest entry in the leader's log
// leader knows if its behind based on reply term and steps down
// advances commit index upon receiving success reply
// reduces last index known for the peer upon receiving failure reply and retries on next hearbeat
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

// leader only
// updates commit index based on quorum matching log entry index
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
		n.persistState() // change in commitIndex
	}
}

// returns failure if ahead (leader behind)
// updates its term to match leader and steps down (peer behind)
// appends entries to its logs
// checks and resolves any conflicts between entries sent by leader and entries in logs
// updates its commit index to apply entries in the next apply loop
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
			n.persistState() // change in log entries
			return nil
		}
	}

	logChanged := false // persisting on every heartbeat is unnecessary (handleAppend called on every hearbeat)

	for i, entry := range args.Entries {
		logIndex := args.PrevLogIndex + 1 + i
		if logIndex <= len(n.log) {
			if n.log[logIndex-1].Term != entry.Term { // overwrite entries only if there is a conflict
				n.log = n.log[:logIndex-1]
				n.log = append(n.log, args.Entries[i:]...)
				logChanged = true
				break
			}
		} else {
			n.log = append(n.log, args.Entries[i:]...)
			logChanged = true
			break
		}
	}

	if args.LeaderCommit > n.commitIndex {
		lastIndex, _ := n.lastLogIndexTerm()
		n.commitIndex = min(args.LeaderCommit, lastIndex)
		logChanged = true
	}

	if logChanged {
		n.persistState()
	}

	reply.Success = true

	if len(args.Entries) > 0 { // too many logs otherwise (handleAppend on every hearbeat)
		log.Printf("node=%s REPLICATED entries=%d log=%d commit=%d ", n.id, len(args.Entries), len(n.log), n.commitIndex)
	}

	return nil
}
