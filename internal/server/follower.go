package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/SushantPulipati05/Redis_Go/internal/resp"
)

func (srv *Server) startReplicaLink() {
	if !srv.linkRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		srv.runReplicaLink()
		srv.linkRunning.Store(false)
		if srv.isReplica.Load() && !srv.stopped.Load() {
			srv.startReplicaLink()
		}
	}()
}

// runReplicaLink keeps the follower connected to its leader and starts a
// failover if the leader stays unreachable.
func (srv *Server) runReplicaLink() {
	srv.markContact()
	for srv.isReplica.Load() && !srv.stopped.Load() {
		err := srv.syncWithLeader()
		srv.linkUp.Store(false)
		if !srv.isReplica.Load() || srv.stopped.Load() {
			return
		}
		log.Printf("link to leader %s lost: %v", srv.getLeaderAddr(), err)

		if len(srv.peers) > 0 && srv.sinceContact() >= failoverAfter {
			srv.handleLeaderDown()
			if !srv.isReplica.Load() {
				return
			}
		}
		time.Sleep(retryInterval)
	}
}

func (srv *Server) setLeaderConn(c net.Conn) {
	srv.leaderConnMu.Lock()
	srv.leaderConn = c
	srv.leaderConnMu.Unlock()
}

func (srv *Server) closeLeaderConn() {
	srv.leaderConnMu.Lock()
	if srv.leaderConn != nil {
		srv.leaderConn.Close()
	}
	srv.leaderConnMu.Unlock()
}

// syncWithLeader runs one connection to the leader: handshake, resync, then
// the live stream until the connection fails or goes quiet.
func (srv *Server) syncWithLeader() error {
	addr := srv.getLeaderAddr()
	conn, err := net.DialTimeout("tcp", addr, peerTimeout)
	if err != nil {
		return err
	}
	srv.setLeaderConn(conn)
	defer func() {
		srv.setLeaderConn(nil)
		conn.Close()
	}()

	reader := resp.NewReader(conn)

	send := func(parts ...string) error {
		conn.SetWriteDeadline(time.Now().Add(leaderReadTimeout))
		_, err := conn.Write(resp.Command(parts...))
		return err
	}
	expect := func(prefixes ...string) (string, error) {
		conn.SetReadDeadline(time.Now().Add(leaderReadTimeout))
		v, err := reader.Read()
		if err != nil {
			return "", err
		}
		for _, p := range prefixes {
			if v.Type == resp.SimpleString && strings.HasPrefix(v.Str, p) {
				return v.Str, nil
			}
		}
		return "", fmt.Errorf("handshake: expected %v, got %q", prefixes, v.Str)
	}

	if err := send("PING"); err != nil {
		return err
	}
	if _, err := expect("PONG"); err != nil {
		return err
	}
	if err := send("REPLCONF", "listening-port", strconv.Itoa(srv.port)); err != nil {
		return err
	}
	if _, err := expect("OK"); err != nil {
		return err
	}
	myID, myOffset := srv.getReplID(), srv.replOffset.Load()
	if err := send("PSYNC", myID, strconv.FormatInt(myOffset, 10)); err != nil {
		return err
	}
	line, err := expect("FULLRESYNC", "CONTINUE")
	if err != nil {
		return err
	}
	fields := strings.Fields(line)

	if fields[0] == "CONTINUE" {
		// Partial resync: the missing bytes arrive as part of the stream.
		// After a failover the leader has a new id that continues ours.
		if len(fields) == 2 {
			srv.setReplID(fields[1])
		}
		log.Printf("partial resync with %s from offset %d", addr, myOffset)
	} else {
		if len(fields) != 3 {
			return fmt.Errorf("bad FULLRESYNC line %q", line)
		}
		leaderOffset, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return fmt.Errorf("bad offset in %q", line)
		}
		conn.SetReadDeadline(time.Now().Add(leaderReadTimeout))
		snap, err := reader.Read()
		if err != nil {
			return err
		}
		if snap.Type != resp.BulkString {
			return errors.New("expected snapshot as a bulk string")
		}
		n, err := srv.loadSnapshot(snap.Str, leaderOffset)
		if err != nil {
			return err
		}
		srv.setReplID(fields[1])
		log.Printf("full resync with %s: %d keys loaded, offset %d", addr, n, leaderOffset)
	}
	srv.linkUp.Store(true)
	srv.markContact()

	done := make(chan struct{})
	defer close(done)
	ackNow := make(chan struct{}, 1)
	go srv.sendAcks(conn, done, ackNow)

	for {
		conn.SetReadDeadline(time.Now().Add(leaderReadTimeout))
		cmd, err := reader.Read()
		if err != nil {
			return err
		}
		srv.markContact()

		// GETACK is sent by WAIT. It is part of the stream (so it counts
		// towards the offset) but is not a data command.
		getAck := cmd.Name() == "REPLCONF" && len(cmd.Array) >= 2 &&
			strings.EqualFold(cmd.Array[1].Str, "GETACK")

		if !getAck {
			if reply := execute(srv, cmd, true); reply.Type == resp.Error {
				log.Printf("replication: command from leader failed: %s", reply.Str)
			}
		}

		// Re-encoding yields exactly the bytes the leader sent, keeping
		// our offset and backlog in step with the leader's.
		raw := cmd.Marshal()
		srv.writeMu.Lock()
		srv.backlog.write(raw)
		srv.replOffset.Add(int64(len(raw)))
		srv.writeMu.Unlock()

		if getAck {
			select {
			case ackNow <- struct{}{}:
			default:
			}
		}
	}
}

// sendAcks reports our offset to the leader every heartbeat, and
// immediately when ackNow fires.
func (srv *Server) sendAcks(conn net.Conn, done, ackNow chan struct{}) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		ack := resp.Command("REPLCONF", "ACK", strconv.FormatInt(srv.replOffset.Load(), 10))
		conn.SetWriteDeadline(time.Now().Add(leaderReadTimeout))
		if _, err := conn.Write(ack); err != nil {
			return
		}
		select {
		case <-done:
			return
		case <-ticker.C:
		case <-ackNow:
		}
	}
}

// loadSnapshot replaces all data with the leader's snapshot.
func (srv *Server) loadSnapshot(payload string, offset int64) (int, error) {
	srv.writeMu.Lock()
	srv.store.Flush()
	srv.backlog.reset(offset)
	srv.replOffset.Store(offset)
	if srv.aof != nil {
		if err := srv.aof.Reset(); err != nil {
			srv.writeMu.Unlock()
			return 0, err
		}
	}
	srv.writeMu.Unlock()

	reader := resp.NewReader(strings.NewReader(payload))
	count := 0
	for {
		cmd, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return count, nil
		}
		if err != nil {
			return count, fmt.Errorf("bad snapshot: %w", err)
		}
		if reply := execute(srv, cmd, true); reply.Type == resp.Error {
			return count, fmt.Errorf("snapshot command failed: %s", reply.Str)
		}
		count++
	}
}
