package raft

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
)

const fileName = "state.json"

type persistentState struct {
	CurrentTerm int        `json:"currentTerm"`
	VotedFor    string     `json:"votedFor"`
	Log         []LogEntry `json:"log"`
	CommitIndex int        `json:"commitIndex"`
}

// store state in n.dataDir/fileName
func (n *Node) persistState() {
	if n.dataDir == "" {
		return
	}

	state := persistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
		CommitIndex: n.commitIndex,
	}

	data, err := json.Marshal(state)
	if err != nil {
		log.Printf("node=%s PERSIST ERROR MARSHAL %v", n.id, err)
		return
	}

	path := filepath.Join(n.dataDir, fileName)

	// use tmp file to avoid corruption (process crash mid-write, power failure, etc.)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		log.Printf("node=%s PERSIST ERROR WRITE %v", n.id, err)
		return
	}
	// rename tmp file
	if err := os.Rename(tmpPath, path); err != nil {
		log.Printf("node=%s PERSIST ERROR RENAME %v", n.id, err)
		return
	}

	log.Printf("node=%s PERSIST SUCCESS term=%d votedFor=%s log=%d commitIndex=%d", n.id, n.currentTerm, n.votedFor, len(n.log), n.commitIndex)
}

// load state from n.dataDir/fileName
// and apply entries to kv store upto commitIndex
func (n *Node) loadState() {
	if n.dataDir == "" {
		return
	}

	path := filepath.Join(n.dataDir, fileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("node=%s PERSIST NO STATE", n.id)
			return
		}
		log.Printf("node=%s PERSIST LOAD ERROR %v", n.id, err)
		return
	}

	var state persistentState
	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("node=%s PERSIST LOAD ERROR MARSHAL %v", n.id, err)
		return
	}
	n.currentTerm = state.CurrentTerm
	n.votedFor = state.VotedFor
	n.log = state.Log
	n.commitIndex = state.CommitIndex

	for i := 0; i < n.commitIndex && i < len(n.log); i++ {
		n.KVStore.Apply(n.log[i].Command)
		n.lastApplied = n.log[i].Index
	}

	log.Printf("node=%s PERSIST LOAD SUCCESS term=%d votedFor=%s log=%d commitIndex=%d", n.id, n.currentTerm, n.votedFor, len(n.log), n.commitIndex)
}
