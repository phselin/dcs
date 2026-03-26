package raft

import (
	"errors"
	"time"
)

type State int

const (
	Leader State = iota
	Candidate
	Follower
)

func (s State) String() string {
	switch s {
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	case Follower:
		return "Follower"
	default:
		return "Unknown"
	}
}

const (
	MinElectionTimeout = 500 * time.Millisecond
	MaxElectionTimeout = 1000 * time.Millisecond
	HeartbeatInterval  = 150 * time.Millisecond
	RPCTimeout         = 300 * time.Millisecond
	ApplyInterval      = 10 * time.Millisecond
)

type Command struct {
	Op            string `json:"op"`
	Key           string `json:"key"`
	Value         string `json:"value"`
	ExpectedValue string `json:"expected_value"`
}

type LogEntry struct {
	Term    int     `json:"term"`
	Index   int     `json:"index"`
	Command Command `json:"command"`
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogTerm  int
	LastLogIndex int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     string
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type ApplyResult struct {
	Value string
	Ok    bool
	Err   error
}

var ErrNotLeader = errors.New("not a leader")
