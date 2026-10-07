package main

// AOF = Append-Only File: our persistence.
//
// Every write command is appended to a file, in the same RESP format clients
// use. On startup we replay the file from the top, which rebuilds the exact
// same data. The file for "SET name sushant" then "DEL name" literally contains:
//
//   *3\r\n$3\r\nSET\r\n$4\r\nname\r\n$7\r\nsushant\r\n
//   *2\r\n$3\r\nDEL\r\n$4\r\nname\r\n
//
// Reusing RESP means the loader is just our existing parser + dispatch.

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

// FsyncPolicy decides how often we force data from memory onto the disk.
//
// Writing to a file normally only reaches the OS's memory cache; the OS saves
// it to disk "later". fsync (file.Sync) forces it onto the disk *now*, but it's
// slow (often milliseconds). So it's a trade-off between safety and speed:
//
//	always   - fsync after every write: lose nothing on a crash, but slowest
//	everysec - fsync once per second: lose at most ~1s of writes (Redis default)
//	no       - let the OS decide: fastest, could lose ~30s on a power cut
type FsyncPolicy string

const (
	FsyncAlways   FsyncPolicy = "always"
	FsyncEverySec FsyncPolicy = "everysec"
	FsyncNo       FsyncPolicy = "no"
)

func ParseFsyncPolicy(s string) (FsyncPolicy, error) {
	switch p := FsyncPolicy(s); p {
	case FsyncAlways, FsyncEverySec, FsyncNo:
		return p, nil
	}
	return "", fmt.Errorf("invalid appendfsync %q (use always, everysec or no)", s)
}

type AOF struct {
	mu     sync.Mutex
	file   *os.File
	w      *bufio.Writer // collects small writes in memory, then writes them in bulk
	policy FsyncPolicy
	done   chan struct{} // closed to stop the background fsync goroutine
	closed bool
}

// OpenAOF opens (or creates) the file for appending.
func OpenAOF(path string, policy FsyncPolicy) (*AOF, error) {
	// O_APPEND: every write goes to the end of the file, never overwriting.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	a := &AOF{
		file:   f,
		w:      bufio.NewWriter(f),
		policy: policy,
		done:   make(chan struct{}),
	}
	if policy == FsyncEverySec {
		go a.syncEverySecond()
	}
	return a, nil
}

// Append writes one command to the log.
func (a *AOF) Append(args []string) error {
	vals := make([]Value, len(args))
	for i, s := range args {
		vals[i] = Bulk(s)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return errors.New("aof is closed")
	}
	if _, err := a.w.Write(ArrayOf(vals...).Marshal()); err != nil {
		return err
	}
	if a.policy == FsyncAlways {
		return a.flushAndSync()
	}
	return nil
}

// flushAndSync pushes buffered bytes to the OS, then forces them to disk.
// Caller must hold a.mu.
func (a *AOF) flushAndSync() error {
	if err := a.w.Flush(); err != nil {
		return err
	}
	return a.file.Sync()
}

func (a *AOF) syncEverySecond() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-a.done:
			return
		case <-ticker.C:
			a.mu.Lock()
			if !a.closed {
				if err := a.flushAndSync(); err != nil {
					log.Printf("AOF fsync failed: %v", err)
				}
			}
			a.mu.Unlock()
		}
	}
}

// Close flushes everything to disk and closes the file. Call on shutdown.
func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	close(a.done)
	err := a.flushAndSync()
	if cerr := a.file.Close(); err == nil {
		err = cerr
	}
	return err
}

// LoadAOF replays the file into srv and returns how many commands it ran.
// A missing file is fine: it just means a fresh start.
//
// Crash safety: if the server died halfway through writing the LAST command,
// the file ends with a partial command. That write never fully reached the
// disk, so we drop the fragment, cut it off the file, and start normally. Real Redis does the same (aof-load-truncated yes).
// Garbage in the MIDDLE of the file is different: that's real corruption, so
// we refuse to start rather than silently lose data.
func LoadAOF(path string, srv *Server) (int, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	src := bytes.NewReader(data)
	reader := NewRespReader(src)
	good := 0 // byte offset where the last complete command ended
	count := 0

	for {
		cmd, err := reader.Read()
		if err != nil {
			if good == len(data) && errors.Is(err, io.EOF) {
				return count, nil // read everything cleanly
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				log.Printf("AOF: last command was incomplete (crash during write?); "+
					"dropping %d trailing bytes", len(data)-good)
				if terr := os.Truncate(path, int64(good)); terr != nil {
					return count, terr
				}
				return count, nil
			}
			return count, fmt.Errorf("AOF corrupted at byte %d: %w", good, err)
		}

		if reply := dispatch(srv, cmd); reply.Type == Error {
			return count, fmt.Errorf("AOF command at byte %d failed: %s", good, reply.Str)
		}
		count++

		// Everything not still waiting in a buffer has been consumed.
		good = len(data) - src.Len() - reader.Buffered()
	}
}
