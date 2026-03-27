package main

import (
	"bufio"
	"dcs/httpapi"
	"dcs/raft"
	"fmt"
	"os"
	"os/signal"
	"strings"
)

type nodeInfo struct {
	id       string
	node     *raft.Node
	raftAddr string
	httpAddr string
	peers    []string
	server   *httpapi.Server
	alive    bool
}

func main() {
	raftAddrs := []string{":9001", ":9002", ":9003"}
	httpAddrs := []string{":8001", ":8002", ":8003"}
	ids := []string{"node1", "node2", "node3"}

	peerHTTP := make(map[string]string, len(ids))
	for i, id := range ids {
		peerHTTP[id] = "localhost" + httpAddrs[i]
	}

	nodes := make(map[string]*nodeInfo)

	for i := range ids {
		peers := append([]string{}, raftAddrs[:i]...)
		peers = append(peers, raftAddrs[i+1:]...)
		n := &nodeInfo{
			id:       ids[i],
			raftAddr: raftAddrs[i],
			httpAddr: httpAddrs[i],
			peers:    peers,
		}
		nodes[ids[i]] = n
		startNode(n, peerHTTP)
	}

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			input := strings.TrimSpace(scanner.Text())
			if input == "" {
				continue
			}
			parts := strings.Fields(input)
			switch parts[0] {
			case "kill":
				if len(parts) < 2 {
					fmt.Println("incorrect usage")
					continue
				}
				n, ok := nodes[parts[1]]
				if !ok {
					fmt.Printf("unknown node %s\n", parts[1])
					continue
				}
				if !n.alive {
					fmt.Printf("already dead %s\n", n.id)
					continue
				}
				n.server.Stop()
				n.node.Stop()
				n.alive = false
				fmt.Printf("killed node %s\n", n.id)

			case "restart":
				if len(parts) < 2 {
					fmt.Println("incorrect usage")
					continue
				}
				n, ok := nodes[parts[1]]
				if !ok {
					fmt.Printf("unknown node %s\n", parts[1])
					continue
				}
				if n.alive {
					fmt.Printf("already running %s", n.id)
					continue
				}
				startNode(n, peerHTTP)
				fmt.Printf("restarted node %s\n", n.id)

			default:
				fmt.Println("unknown command")
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig

	for _, n := range nodes {
		if n.alive {
			n.server.Stop()
			n.node.Stop()
		}
	}
}

func startNode(n *nodeInfo, peerHTTP map[string]string) {
	n.node = raft.NewNode(n.id, n.raftAddr, n.peers)
	n.node.Start()
	n.server = httpapi.NewServer(n.node, peerHTTP)
	n.server.Start(n.httpAddr)
	n.alive = true
}
