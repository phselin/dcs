package main

import (
	"dcs/raft"
	"flag"
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

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	node.Stop()
}
