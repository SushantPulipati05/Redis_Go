package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Command-line options, e.g.:
	//   go run .                                  leader on 6380
	//   go run . -port 6381 -replicaof localhost:6380   follower of that leader
	port := flag.Int("port", 6380, "port to listen on")
	aofFlag := flag.String("aof", "auto", `append-only file path ("auto" = appendonly-<port>.aof, "" disables persistence)`)
	fsyncFlag := flag.String("appendfsync", "everysec", "fsync policy: always | everysec | no")
	replicaOf := flag.String("replicaof", "", `run as a follower of this leader, e.g. "localhost:6380"`)
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
		srv.leaderAddr = *replicaOf
		srv.isReplica.Store(true)
		go srv.runReplicaLink()
		fmt.Printf("role: follower of %s\n", *replicaOf)
	} else {
		go srv.heartbeatLoop()
		fmt.Println("role: leader")
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
