package main

import (
	"dcs/httpapi"
	"dcs/raft"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
)

type nodeInfo struct {
	id        string
	node      *raft.Node
	raftAddr  string
	httpAddr  string
	peers     []string
	server    *httpapi.Server
	alive     bool
	dataDir   string
	peersHTTP map[string]string
}

var (
	nodes       map[string]*nodeInfo
	nodeIDs     []string
	baseDataDir = "./data"
	mu          sync.Mutex
	initialized = false
)

func main() {
	startServer(":7000")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig

	mu.Lock()
	defer mu.Unlock()
	for _, n := range nodes {
		if n.alive {
			stopNode(n)
		}
	}
	os.RemoveAll(baseDataDir)
}

func initCluster(count int) {
	initialized = true
	nodeIDs = make([]string, count)
	raftAddrs := make([]string, count)
	httpAddrs := make([]string, count)

	for i := range count {
		nodeIDs[i] = fmt.Sprintf("node%d", i+1)
		raftAddrs[i] = fmt.Sprintf(":%d", 9001+i)
		httpAddrs[i] = fmt.Sprintf(":%d", 8001+i)
	}

	peerHTTP := make(map[string]string, count)
	for i, id := range nodeIDs {
		peerHTTP[id] = "localhost" + httpAddrs[i]
	}

	nodes = make(map[string]*nodeInfo)

	for i := range nodeIDs {
		peers := make([]string, 0, count-1)
		for j := range raftAddrs {
			if j != i {
				peers = append(peers, raftAddrs[j])
			}
		}
		n := &nodeInfo{
			id:        nodeIDs[i],
			raftAddr:  raftAddrs[i],
			httpAddr:  httpAddrs[i],
			peers:     peers,
			dataDir:   filepath.Join(baseDataDir, nodeIDs[i]),
			peersHTTP: peerHTTP,
		}
		nodes[nodeIDs[i]] = n
		startNode(n)
	}
}

func startNode(n *nodeInfo) {
	n.node = raft.NewNode(n.id, n.raftAddr, n.peers, n.dataDir)
	n.node.Start()
	n.server = httpapi.NewServer(n.node, n.peersHTTP)
	n.server.Start(n.httpAddr)
	n.alive = true
}

func stopNode(n *nodeInfo) {
	n.server.Stop()
	n.node.Stop()
	n.alive = false
}

func startServer(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/init", handleInit)
	mux.HandleFunc("/status", handleStatus)
	mux.HandleFunc("/kill/", handleKill)
	mux.HandleFunc("/restart/", handleRestart)
	mux.HandleFunc("/wipe", handleWipeAll)
	mux.HandleFunc("/wipe/", handleWipe)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		mux.ServeHTTP(w, r)
	})

	go http.ListenAndServe(addr, handler)
}

func handleInit(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	if initialized {
		httpapi.WriteJSON(w, http.StatusConflict, map[string]string{"error": "already initialized"})
		return
	}

	var req struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Count < 1 || req.Count > 10 {
		httpapi.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "count should be from 1 to 10"})
		return
	}

	initCluster(req.Count)
	httpapi.WriteJSON(w, http.StatusOK, map[string]any{"nodes": nodeIDs})
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()

	if !initialized {
		httpapi.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not initialized"})
		return
	}

	result := make([]map[string]any, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		n := nodes[id]
		info := map[string]any{
			"id":       n.id,
			"alive":    n.alive,
			"httpAddr": "localhost" + n.httpAddr,
		}
		if n.alive {
			_, state, term, leader := n.node.GetStatus()
			logLen, _, commitIndex := n.node.GetLogInfo()
			info["state"] = state.String()
			info["term"] = term
			info["leader"] = leader
			info["logLength"] = logLen
			info["commitIndex"] = commitIndex
		}
		result = append(result, info)
	}

	httpapi.WriteJSON(w, http.StatusOK, map[string]any{"nodes": result})
}

func handleKill(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/kill/")
	mu.Lock()
	defer mu.Unlock()

	n, ok := nodes[id]
	if !ok {
		httpapi.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "unknown node"})
		return
	}
	if !n.alive {
		httpapi.WriteJSON(w, http.StatusConflict, map[string]string{"error": "already dead"})
		return
	}
	stopNode(n)
	httpapi.WriteJSON(w, http.StatusOK, map[string]string{"killed": id})
}

func handleRestart(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/restart/")
	mu.Lock()
	defer mu.Unlock()

	n, ok := nodes[id]
	if !ok {
		httpapi.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "unknown node"})
		return
	}
	if n.alive {
		httpapi.WriteJSON(w, http.StatusConflict, map[string]string{"error": "already running"})
		return
	}
	startNode(n)
	httpapi.WriteJSON(w, http.StatusOK, map[string]string{"restarted": id})
}

func handleWipeAll(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()

	for _, n := range nodes {
		if n.alive {
			stopNode(n)
		}
	}
	os.RemoveAll(baseDataDir)
	for _, id := range nodeIDs {
		startNode(nodes[id])
	}

	httpapi.WriteJSON(w, http.StatusOK, map[string]string{"wiped": "all"})
}

func handleWipe(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/wipe/")
	mu.Lock()
	defer mu.Unlock()

	n, ok := nodes[id]
	if !ok {
		httpapi.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "unknown node"})
		return
	}
	os.RemoveAll(n.dataDir)
	httpapi.WriteJSON(w, http.StatusOK, map[string]string{"wiped": id})
}
