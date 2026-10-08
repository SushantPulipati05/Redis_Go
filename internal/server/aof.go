package server

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

	"github.com/SushantPulipati05/Redis_Go/internal/resp"
)

// FsyncPolicy controls how often the AOF is forced to disk.
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

// AOF is an append-only log of write commands, stored in RESP format.
type AOF struct {
	mu     sync.Mutex
	file   *os.File
	w      *bufio.Writer
	policy FsyncPolicy
	done   chan struct{}
	closed bool
}

func OpenAOF(path string, policy FsyncPolicy) (*AOF, error) {
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

func (a *AOF) Append(cmd []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return errors.New("aof is closed")
	}
	if _, err := a.w.Write(cmd); err != nil {
		return err
	}
	if a.policy == FsyncAlways {
		return a.flushAndSync()
	}
	return nil
}

// Reset truncates the log. Used by a follower before a full resync.
func (a *AOF) Reset() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return errors.New("aof is closed")
	}
	a.w.Reset(a.file)
	if err := a.file.Truncate(0); err != nil {
		return err
	}
	return a.file.Sync()
}

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

// LoadAOF replays the log at path into srv and returns the number of commands
// applied. A missing file is not an error. An incomplete final command (a
// crash mid-write) is dropped and cut from the file; anything else that
// fails to parse is treated as corruption.
func LoadAOF(path string, srv *Server) (int, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	src := bytes.NewReader(data)
	reader := resp.NewReader(src)
	good := 0 // offset just past the last complete command
	count := 0

	for {
		cmd, err := reader.Read()
		if err != nil {
			if good == len(data) && errors.Is(err, io.EOF) {
				return count, nil
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				log.Printf("AOF: dropping %d bytes of an incomplete final command", len(data)-good)
				if terr := os.Truncate(path, int64(good)); terr != nil {
					return count, terr
				}
				return count, nil
			}
			return count, fmt.Errorf("AOF corrupted at byte %d: %w", good, err)
		}

		if reply := execute(srv, cmd, true); reply.Type == resp.Error {
			return count, fmt.Errorf("AOF command at byte %d failed: %s", good, reply.Str)
		}
		count++

		good = len(data) - src.Len() - reader.Buffered()
	}
}
