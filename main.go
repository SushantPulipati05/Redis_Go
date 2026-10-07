package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Command-line options, e.g.:  go run . -port 6381 -aof data.aof -appendfsync always
	port := flag.Int("port", 6380, "port to listen on")
	aofPath := flag.String("aof", "appendonly.aof", "append-only file path (empty string disables persistence)")
	fsyncFlag := flag.String("appendfsync", "everysec", "fsync policy: always | everysec | no")
	flag.Parse()

	policy, err := ParseFsyncPolicy(*fsyncFlag)
	if err != nil {
		log.Fatal(err)
	}

	store := NewStore()
	srv := NewServer(store)

	// Rebuild the data from the AOF BEFORE accepting clients, so nobody
	// sees a half-loaded database. srv.aof is still nil here, so replayed
	// commands aren't written to the file a second time.
	if *aofPath != "" {
		start := time.Now()
		n, err := LoadAOF(*aofPath, srv)
		if err != nil {
			log.Fatalf("could not load %s: %v", *aofPath, err)
		}
		fmt.Printf("loaded %d commands from %s in %v\n", n, *aofPath, time.Since(start).Round(time.Millisecond))

		aof, err := OpenAOF(*aofPath, policy)
		if err != nil {
			log.Fatal(err)
		}
		srv.aof = aof
	}

	go store.RunActiveExpiry()

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

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			fmt.Println("accept error:", err)
			continue // one bad connection shouldn't kill the server
		}

		// Serve each client in its own goroutine so many can connect at once.
		go handleConn(conn, srv)
	}
}

// handleConn serves one client until it disconnects.
func handleConn(conn net.Conn, srv *Server) {
	defer conn.Close()
	fmt.Println("client connected:", conn.RemoteAddr())

	reader := NewRespReader(conn)

	for {
		// Read exactly one full command, however the bytes arrived over TCP.
		cmd, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("client disconnected:", conn.RemoteAddr())
			} else {
				// Malformed input: tell the client, then drop the connection
				// (we can't know where the next command starts).
				conn.Write(Err("ERR Protocol error: " + err.Error()).Marshal())
				fmt.Println("protocol error from", conn.RemoteAddr(), err)
			}
			return
		}

		reply := dispatch(srv, cmd)

		if _, err := conn.Write(reply.Marshal()); err != nil {
			fmt.Println("write error:", err)
			return
		}
	}
}
