package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
)

func main() {

	listener, err := net.Listen("tcp", ":6380")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	fmt.Println("listening on :6380")

	store := NewStore()

	// Background goroutine that deletes expired keys nobody reads again.
	go store.RunActiveExpiry()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("accept error:", err)
			continue // one bad connection shouldn't kill the server
		}

		// Serve each client in its own goroutine so many can connect at once.
		go handleConn(conn, store)
	}
}

// handleConn serves one client until it disconnects.
func handleConn(conn net.Conn, store *Store) {
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

		reply := dispatch(store, cmd)

		if _, err := conn.Write(reply.Marshal()); err != nil {
			fmt.Println("write error:", err)
			return
		}
	}
}
