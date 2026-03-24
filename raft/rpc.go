package raft

import (
	"net"
	"net/rpc"
	"time"
)

type RPCService struct {
	node *Node
}

func (n *Node) startRPCServer() error {
	svc := &RPCService{node: n}
	server := rpc.NewServer()

	if err := server.RegisterName("Raft", svc); err != nil {
		return err
	}

	ln, err := net.Listen("tcp", n.address)

	if err != nil {
		return err
	}
	n.listener = ln

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-n.stopCh:
					return
				default:
					continue
				}
			}
			go server.ServeConn(conn)
		}
	}()

	return nil
}

func rpcDial(addr string) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", addr, RPCTimeout)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(RPCTimeout))
	return rpc.NewClient(conn), nil
}

func (s *RPCService) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	return s.node.handleRequestVote(args, reply)
}

func (n *Node) callRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	client, err := rpcDial(peer)
	if err != nil {
		return nil, err
	}

	defer client.Close()
	reply := &RequestVoteReply{}
	if err := client.Call("Raft.RequestVote", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (s *RPCService) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	return s.node.handleAppendEntries(args, reply)
}

func (n *Node) callAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	client, err := rpcDial(peer)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	reply := &AppendEntriesReply{}
	if err := client.Call("Raft.AppendEntries", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}
