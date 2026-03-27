package httpapi

import (
	"bytes"
	"context"
	"dcs/raft"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

const clientTimeout = 5 * time.Second
const shutdownTimeout = 3 * time.Second
const proposeTimeout = 5 * time.Second

type Server struct {
	node       *raft.Node
	peerHTTP   map[string]string
	httpClient *http.Client
	httpServer *http.Server
}

func NewServer(node *raft.Node, peerHTTP map[string]string) *Server {
	return &Server{
		node:       node,
		peerHTTP:   peerHTTP,
		httpClient: &http.Client{Timeout: clientTimeout},
	}
}

func (s *Server) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/keys/", s.handleKeys)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/dump", s.handleDump)
	s.httpServer = &http.Server{Addr: addr, Handler: mux}
	log.Printf("STARTING HTTP API on %s", addr)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP Server error=%v", err)
		}
	}()
	return nil
}

func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	s.httpServer.Shutdown(ctx)
}

func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/keys/")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing key"})
		return
	}

	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		r.Body.Close()
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key, body)
	case http.MethodPut:
		s.handlePut(w, r, key, body)
	case http.MethodDelete:
		s.handleDelete(w, r, key, body)
	case http.MethodPatch:
		s.handleCAS(w, r, key, body)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key string, body []byte) {
	val, ok, err := s.node.GetKV(key)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, body)
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("%v", err)})
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found", "key": key})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": val})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string, body []byte) {
	var req struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	cmd := raft.Command{Op: "put", Key: key, Value: req.Value}
	result, err := s.node.Propose(cmd, proposeTimeout)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, body)
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("%v", err)})
		return
	}
	// result.Ok will never be false for put
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": result.Value})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key string, body []byte) {
	cmd := raft.Command{Op: "delete", Key: key}
	result, err := s.node.Propose(cmd, proposeTimeout)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, body)
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("%v", err)})
		return
	}
	if !result.Ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found", "key": key})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": result.Value, "deleted": "true"})
}

func (s *Server) handleCAS(w http.ResponseWriter, r *http.Request, key string, body []byte) {
	var req struct {
		Expected string `json:"expected"`
		Value    string `json:"value"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	cmd := raft.Command{Op: "cas", Key: key, Value: req.Value, Expected: req.Expected}
	result, err := s.node.Propose(cmd, proposeTimeout)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, body)
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("%v", err)})
		return
	}
	if !result.Ok {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "cas failed", "key": key, "value": result.Value})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": result.Value})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	id, state, term, leader := s.node.GetStatus()
	logLen, lastApplied, commitIndex := s.node.GetLogInfo()
	writeJSON(w, http.StatusOK, map[string]any{
		"id":          id,
		"state":       state.String(),
		"term":        term,
		"leader":      leader,
		"logLength":   logLen,
		"lastApplied": lastApplied,
		"commitIndex": commitIndex,
	})
}

func (s *Server) handleDump(w http.ResponseWriter, r *http.Request) {
	data := s.node.GetAllKV()
	id, state, _, _ := s.node.GetStatus()
	writeJSON(w, http.StatusOK, map[string]any{
		"id":    id,
		"state": state.String(),
		"data":  data,
	})
}

func (s *Server) forwardToLeader(w http.ResponseWriter, r *http.Request, body []byte) {
	_, _, _, leader := s.node.GetStatus()
	if leader == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no leader"})
		return
	}
	leaderHTTPAddr, ok := s.peerHTTP[leader]
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "leader HTTP address unknown"})
		return
	}

	url := "http://" + leaderHTTPAddr + r.URL.Path
	req, err := http.NewRequest(r.Method, url, bytes.NewReader(body))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create forward request"})
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to reach leader"})
		return
	}

	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
