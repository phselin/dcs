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
	mux.HandleFunc("/lease", s.handleLease)
	mux.HandleFunc("/lease/", s.handleLeaseID)
	s.httpServer = &http.Server{Addr: addr, Handler: corsMiddleware(mux)}
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

// https://www.stackhawk.com/blog/golang-cors-guide-what-it-is-and-how-to-enable-it/ (Using Middleware for Better Organization)
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/keys/")
	if key == "" {
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "missing key"})
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
		WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key string, body []byte) {
	val, ok, err := s.node.GetKV(key)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, body)
			return
		}
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("%v", err)})
		return
	}
	if !ok {
		WriteJSON(w, http.StatusNotFound, map[string]string{"error": "not found", "key": key})
		return
	}
	WriteJSON(w, http.StatusOK, map[string]string{"key": key, "value": val})
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string, body []byte) {
	var req struct {
		Value   string `json:"value"`
		LeaseID string `json:"leaseID"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	cmd := raft.Command{Op: "put", Key: key, Value: req.Value, LeaseID: req.LeaseID}
	result, err := s.node.Propose(cmd, proposeTimeout)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, body)
			return
		}
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("%v", err)})
		return
	}
	// result.Ok will never be false for put
	WriteJSON(w, http.StatusOK, map[string]string{"key": key, "value": result.Value})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key string, body []byte) {
	cmd := raft.Command{Op: "delete", Key: key}
	result, err := s.node.Propose(cmd, proposeTimeout)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, body)
			return
		}
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("%v", err)})
		return
	}
	if !result.Ok {
		WriteJSON(w, http.StatusNotFound, map[string]string{"error": "not found", "key": key})
		return
	}
	WriteJSON(w, http.StatusOK, map[string]string{"key": key, "value": result.Value, "deleted": "true"})
}

func (s *Server) handleCAS(w http.ResponseWriter, r *http.Request, key string, body []byte) {
	var req struct {
		Expected string `json:"expected"`
		Value    string `json:"value"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	cmd := raft.Command{Op: "cas", Key: key, Value: req.Value, Expected: req.Expected}
	result, err := s.node.Propose(cmd, proposeTimeout)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, body)
			return
		}
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("%v", err)})
		return
	}
	if !result.Ok {
		WriteJSON(w, http.StatusConflict, map[string]string{"error": "cas failed", "key": key, "value": result.Value})
		return
	}
	WriteJSON(w, http.StatusOK, map[string]string{"key": key, "value": result.Value})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	id, state, term, leader := s.node.GetStatus()
	logLen, lastApplied, commitIndex := s.node.GetLogInfo()
	WriteJSON(w, http.StatusOK, map[string]any{
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
	WriteJSON(w, http.StatusOK, map[string]any{
		"id":    id,
		"state": state.String(),
		"data":  data,
	})
}

func (s *Server) handleLease(w http.ResponseWriter, r *http.Request) {
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		r.Body.Close()
	}
	switch r.Method {
	case http.MethodGet:
		s.handleLeaseList(w)
	case http.MethodPost:
		s.handleLeaseGrant(w, r, body)
	default:
		WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (s *Server) handleLeaseList(w http.ResponseWriter) {
	leases := s.node.GetAllLeases()
	result := make([]map[string]any, 0, len(leases))
	for _, lease := range leases {
		result = append(result, map[string]any{
			"leaseID":   lease.ID,
			"ttl":       lease.TTL,
			"expiresAt": lease.ExpiresAt,
			"keys":      lease.Keys,
		})
	}
	WriteJSON(w, http.StatusOK, map[string]any{"leases": result})
}

func (s *Server) handleLeaseGrant(w http.ResponseWriter, r *http.Request, body []byte) {
	var req struct {
		TTL int `json:"ttl"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.TTL <= 0 {
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}

	result, err := s.node.GrantLease(time.Duration(req.TTL) * time.Second)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, body)
			return
		}
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("%v", err)})
		return
	}
	WriteJSON(w, http.StatusOK, map[string]string{"leaseID": result.LeaseID, "ttl": fmt.Sprintf("%v", req.TTL)})
}

func (s *Server) handleLeaseID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/lease/")
	parts := strings.SplitN(path, "/", 2)
	leaseID := parts[0]
	if leaseID == "" {
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "missing lease id"})
		return
	}

	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		r.Body.Close()
	}

	if len(parts) == 2 && parts[1] == "renew" && r.Method == http.MethodPost {
		s.handleLeaseRenew(w, r, leaseID, body)
		return
	}

	switch r.Method {
	case http.MethodDelete:
		s.handleLeaseRevoke(w, r, leaseID, body)
	case http.MethodGet:
		s.handleLeaseGet(w, r, leaseID, body)
	default:
		WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (s *Server) handleLeaseRenew(w http.ResponseWriter, r *http.Request, leaseID string, body []byte) {
	result, err := s.node.RenewLease(leaseID)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, body)
			return
		}
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("%v", err)})
		return
	}
	if !result.Ok {
		WriteJSON(w, http.StatusNotFound, map[string]string{"error": "lease not found", "leaseID": leaseID})
		return
	}
	WriteJSON(w, http.StatusOK, map[string]string{"leaseID": result.LeaseID, "renewed": "true"})
}

func (s *Server) handleLeaseRevoke(w http.ResponseWriter, r *http.Request, leaseID string, body []byte) {
	result, err := s.node.RevokeLease(leaseID)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, body)
			return
		}
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("%v", err)})
		return
	}
	if !result.Ok {
		WriteJSON(w, http.StatusNotFound, map[string]string{"error": "lease not found", "leaseID": leaseID})
		return
	}
	WriteJSON(w, http.StatusOK, map[string]string{"leaseID": result.LeaseID, "revoked": "true"})
}

func (s *Server) handleLeaseGet(w http.ResponseWriter, r *http.Request, leaseID string, body []byte) {
	lease, ok, err := s.node.GetLease(leaseID)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			s.forwardToLeader(w, r, body)
			return
		}
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("%v", err)})
		return
	}
	if !ok {
		WriteJSON(w, http.StatusNotFound, map[string]string{"error": "lease not found", "leaseID": leaseID})
		return
	}
	WriteJSON(w, http.StatusOK, map[string]any{
		"leaseID":   lease.ID,
		"ttl":       lease.TTL,
		"expiresAt": lease.ExpiresAt,
		"keys":      lease.Keys,
	})
}

func (s *Server) forwardToLeader(w http.ResponseWriter, r *http.Request, body []byte) {
	_, _, _, leader := s.node.GetStatus()
	if leader == "" {
		WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no leader"})
		return
	}
	leaderHTTPAddr, ok := s.peerHTTP[leader]
	if !ok {
		WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "leader HTTP address unknown"})
		return
	}

	url := "http://" + leaderHTTPAddr + r.URL.Path
	req, err := http.NewRequest(r.Method, url, bytes.NewReader(body))
	if err != nil {
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to create forward request"})
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		WriteJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to reach leader"})
		return
	}

	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func WriteJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
