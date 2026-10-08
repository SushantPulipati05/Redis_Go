package server

// backlog holds the most recent bytes of the replication stream so that a
// follower that briefly disconnects can catch up without a full resync.

type backlog struct {
	buf   []byte
	start int64
	max   int
}

const defaultBacklogSize = 1 << 20

func newBacklog(start int64) *backlog {
	return &backlog{start: start, max: defaultBacklogSize}
}

func (b *backlog) end() int64 {
	return b.start + int64(len(b.buf))
}

// write appends p. The buffer is trimmed back to max only once it reaches
// twice that size, so the copy happens rarely.
func (b *backlog) write(p []byte) {
	b.buf = append(b.buf, p...)
	if len(b.buf) > 2*b.max {
		drop := len(b.buf) - b.max
		b.buf = append([]byte(nil), b.buf[drop:]...)
		b.start += int64(drop)
	}
}

// readFrom returns a copy of the stream from offset onwards, or false if that
// offset is not covered by the backlog.
func (b *backlog) readFrom(offset int64) ([]byte, bool) {
	if offset < b.start || offset > b.end() {
		return nil, false
	}
	return append([]byte(nil), b.buf[offset-b.start:]...), true
}

func (b *backlog) reset(offset int64) {
	b.buf = b.buf[:0]
	b.start = offset
}
