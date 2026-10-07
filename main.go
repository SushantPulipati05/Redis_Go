package main

import (
	"fmt"
	"log"
	"net"
)

func main() {
	// Open TCP port 6380 (real Redis uses 6379, so we avoid clashing with it).
	listener, err := net.Listen("tcp", ":6380")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	fmt.Println("listening on :6380")

	// Block here until one client (e.g. redis-cli) connects.
	conn, err := listener.Accept()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	fmt.Println("client connected:", conn.RemoteAddr())

	// Buffer that each Read fills with whatever bytes the client sent.
	buf := make([]byte, 1024)

	for {
		// n = how many bytes actually arrived; only buf[:n] is real data.
		n, err := conn.Read(buf)
		if err != nil {
			// io.EOF here means the client closed the connection.
			fmt.Println("read error:", err)
			break
		}

		// %q shows \r\n literally, so you can see the RESP format.
		fmt.Printf("received: %q\n", buf[:n])

		// Reply in RESP "simple string" format: + then text then \r\n.
		// For now we answer PONG to everything; parsing comes next.
		conn.Write([]byte("+PONG\r\n"))
	}
}
