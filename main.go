package main

import (
	"bufio"
	"dcs/raft"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	id := flag.String("id", "", "unique node id")
	addr := flag.String("addr", "", "listen address")
	peersFlag := flag.String("peers", "", "comma separated peer addresses")
	flag.Parse()

	if *id == "" || *addr == "" {
		log.Fatalln("Incorrect usage")
		os.Exit(1)
	}

	var peers []string
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	node := raft.NewNode(*id, *addr, peers)
	if err := node.Start(); err != nil {
		log.Fatalf("Failed to START node=%v", err)
	}

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			nid, state, term, leader := node.GetStatus()
			log.Printf("node=%s, state=%s term=%d leader=%s", nid, state, term, leader)
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
			index, term, err := node.Propose(cmd)
			if err != nil {
				_, _, _, leader := node.GetStatus()
				log.Printf("PROPOSE FAILED leader=%s error=%v", leader, err)
			} else {
				log.Printf("PROPOSE SUCCESS index=%d term=%d cmd%s", index, term, cmd)
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	node.Stop()
}
