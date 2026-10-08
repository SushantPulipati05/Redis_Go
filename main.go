package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	// Command-line options. A 3-node cluster with automatic failover:
	//   go run . -port 6380 -peers localhost:6381,localhost:6382
	//   go run . -port 6381 -replicaof localhost:6380 -peers localhost:6380,localhost:6382
	//   go run . -port 6382 -replicaof localhost:6380 -peers localhost:6380,localhost:6381
	port := flag.Int("port", 6380, "port to listen on")
	aofFlag := flag.String("aof", "auto", `append-only file path ("auto" = appendonly-<port>.aof, "" disables persistence)`)
	fsyncFlag := flag.String("appendfsync", "everysec", "fsync policy: always | everysec | no")
	replicaOf := flag.String("replicaof", "", `start as a follower of this leader, e.g. "localhost:6380"`)
	peersFlag := flag.String("peers", "", "comma-separated addresses of the OTHER nodes; enables automatic failover")
	announce := flag.String("announce", "", `address other nodes use to reach this one (default "localhost:<port>")`)
	flag.Parse()

	policy, err := ParseFsyncPolicy(*fsyncFlag)
	if err != nil {
		log.Fatal(err)
	}

	// Each server needs its own file: two processes appending to one file
	// would interleave their writes and corrupt it.
	aofPath := *aofFlag
	if aofPath == "auto" {
		aofPath = fmt.Sprintf("appendonly-%d.aof", *port)
	}

	store := NewStore()
	srv := NewServer(store)
	srv.port = *port
	srv.addr = *announce
	if srv.addr == "" {
		srv.addr = fmt.Sprintf("localhost:%d", *port)
	}
	if *peersFlag != "" {
		for _, p := range strings.Split(*peersFlag, ",") {
			if p = strings.TrimSpace(p); p != "" {
				srv.peers = append(srv.peers, p)
			}
		}
	}

	// Rebuild the data from the AOF BEFORE accepting clients, so nobody
	// sees a half-loaded database. srv.aof is still nil here, so replayed
	// commands aren't written to the file a second time.
	if aofPath != "" {
		start := time.Now()
		n, err := LoadAOF(aofPath, srv)
		if err != nil {
			log.Fatalf("could not load %s: %v", aofPath, err)
		}
		fmt.Printf("loaded %d commands from %s in %v\n", n, aofPath, time.Since(start).Round(time.Millisecond))

		aof, err := OpenAOF(aofPath, policy)
		if err != nil {
			log.Fatal(err)
		}
		srv.aof = aof
	}

	go store.RunActiveExpiry()

	if *replicaOf != "" {
		srv.setLeaderAddr(*replicaOf)
		srv.isReplica.Store(true)
	} else if len(srv.peers) > 0 {
		// Started as leader -- but if the cluster already has a newer leader
		// (we crashed and someone took over), join it instead.
		srv.discoverLeaderAtStartup()
	}

	if srv.isReplica.Load() {
		fmt.Printf("role: follower of %s\n", srv.getLeaderAddr())
		srv.startReplicaLink()
	} else {
		fmt.Println("role: leader")
		srv.startLeaderLoops()
	}
	if len(srv.peers) > 0 {
		fmt.Printf("automatic failover on: I am %s, peers %v\n", srv.addr, srv.peers)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("listening on :%d\n", *port)

	// Graceful shutdown: on Ctrl+C, flush the AOF to disk before exiting,
	// so writes still sitting in the buffer aren't lost.
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		fmt.Println("\nshutting down...")
		srv.writeMu.Lock() // wait for any in-flight write to finish, block new ones
		if srv.aof != nil {
			if err := srv.aof.Close(); err != nil {
				log.Printf("AOF close: %v", err)
			}
		}
		listener.Close()
		os.Exit(0)
	}()

	srv.Serve(listener)
}
