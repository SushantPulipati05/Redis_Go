// Command redis-go runs a Redis-compatible server node.
//
// A three-node cluster with automatic failover:
//
//	redis-go -port 6380 -peers localhost:6381,localhost:6382
//	redis-go -port 6381 -replicaof localhost:6380 -peers localhost:6380,localhost:6382
//	redis-go -port 6382 -replicaof localhost:6380 -peers localhost:6380,localhost:6381
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/SushantPulipati05/Redis_Go/internal/server"
)

func main() {
	port := flag.Int("port", 6380, "port to listen on")
	aof := flag.String("aof", "auto", `append-only file ("auto" = appendonly-<port>.aof, "" disables persistence)`)
	fsync := flag.String("appendfsync", "everysec", "always | everysec | no")
	replicaOf := flag.String("replicaof", "", "start as a follower of host:port")
	peers := flag.String("peers", "", "comma-separated addresses of the other nodes; enables failover")
	announce := flag.String("announce", "", "address other nodes use to reach this one (default localhost:<port>)")
	flag.Parse()

	policy, err := server.ParseFsyncPolicy(*fsync)
	if err != nil {
		log.Fatal(err)
	}
	if *aof == "auto" {
		*aof = fmt.Sprintf("appendonly-%d.aof", *port)
	}

	srv, err := server.New(server.Config{
		Port:      *port,
		Addr:      *announce,
		Peers:     splitList(*peers),
		ReplicaOf: *replicaOf,
		AOFPath:   *aof,
		Fsync:     policy,
	})
	if err != nil {
		log.Fatal(err)
	}
	srv.Start()

	if srv.IsLeader() {
		log.Printf("leader on :%d", *port)
	} else {
		log.Printf("follower of %s on :%d", srv.LeaderAddr(), *port)
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		log.Print("shutting down")
		if err := srv.Shutdown(); err != nil {
			log.Printf("shutdown: %v", err)
		}
		os.Exit(0)
	}()

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
