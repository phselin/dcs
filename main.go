package main

import (
	"bufio"
	"dcs/raft"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const proposalTimeout = 5 * time.Second

func main() {
	id := flag.String("id", "", "unique node id")
	addr := flag.String("addr", "", "address")
	peersFlag := flag.String("peers", "", "comma separated peer addresses")
	flag.Parse()

	if *id == "" || *addr == "" {
		fmt.Println("Incorrect usage")
		os.Exit(1)
	}

	var peers []string
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	node := raft.NewNode(*id, *addr, peers)
	if err := node.Start(); err != nil {
		fmt.Printf("Failed to START node=%v\n", err)
		os.Exit(1)
	}

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			nid, state, term, leader := node.GetStatus()
			fmt.Printf("node=%s, state=%s term=%d leader=%s\n", nid, state, term, leader)
			<-ticker.C
		}
	}()

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Println("Type command to Propose")
		for scanner.Scan() {
			cmd := strings.TrimSpace(scanner.Text())
			if cmd == "" {
				continue
			}
			handleCommandInput(node, cmd)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	node.Stop()
}

func handleCommandInput(node *raft.Node, cmdInput string) {
	parts := strings.Fields(cmdInput)
	if len(parts) == 0 {
		return
	}
	switch strings.ToLower(parts[0]) {
	case "put":
		if len(parts) < 3 {
			fmt.Println("Incorrect usage")
			return
		}
		key := parts[1]
		value := strings.Join(parts[2:], " ")
		cmd := raft.Command{Op: "put", Key: key, Value: value}
		result, err := node.Propose(cmd, proposalTimeout)
		if err == nil {
			fmt.Printf("PUT %s=%s\n", key, result.Value)
		}
	case "get":
		if len(parts) < 2 {
			fmt.Println("Incorrect usage")
			return
		}
		key := parts[1]
		val, ok, err := node.GetKV(key)
		if err == nil {
			if ok {
				fmt.Printf("GET %s=%s\n", key, val)
			} else {
				fmt.Printf("NOT FOUND %s\n", key)
			}
		}
	case "delete":
		if len(parts) < 2 {
			fmt.Println("Incorrect usage")
			return
		}
		key := parts[1]
		cmd := raft.Command{Op: "delete", Key: key}
		result, err := node.Propose(cmd, proposalTimeout)
		if err == nil {
			fmt.Printf("DELETE %s=%s", key, result.Value)
		}
	case "dump":
		data := node.GetAllKV()
		fmt.Println("KV Store:")
		for k, v := range data {
			fmt.Printf("key=%s value=%s\n", k, v)
		}
	case "default":
		fmt.Println("Unknown command")
	}
}
