package main

// backlog keeps the most recent part of the replication stream in memory.
//
// Every write the leader streams (and every write a follower applies) is also
// appended here. When a follower briefly disconnects and comes back saying
// "I have everything up to offset 940", the leader can send just bytes
// 940..end from the backlog instead of a whole new snapshot. That's a
// PARTIAL resync, and it's what makes short network blips cheap.
//
// If the follower was gone so long that offset 940 has already been trimmed
// out of the backlog, the leader falls back to a FULL resync.
//
//	offsets:   start                         end
//	           |<------- buf (recent bytes) ------>|
//	           1000                         1800    -> can serve any offset 1000..1800
type backlog struct {
	buf   []byte
	start int64 // replication offset of buf[0]
	max   int   // keep roughly this many bytes (we trim at 2x to avoid copying on every write)
}

const defaultBacklogSize = 1 << 20 // 1 MB, same default as real Redis

func newBacklog(start int64) *backlog {
	return &backlog{start: start, max: defaultBacklogSize}
}

// end is the offset just after the newest byte.
func (b *backlog) end() int64 {
	return b.start + int64(len(b.buf))
}

func (b *backlog) write(p []byte) {
	b.buf = append(b.buf, p...)
	if len(b.buf) > 2*b.max {
		drop := len(b.buf) - b.max
		b.buf = append([]byte(nil), b.buf[drop:]...) // copy the newest max bytes
		b.start += int64(drop)
	}
}

// readFrom returns a copy of everything from offset to the end, or false if
// that offset is no longer (or not yet) in the backlog.
func (b *backlog) readFrom(offset int64) ([]byte, bool) {
	if offset < b.start || offset > b.end() {
		return nil, false
	}
	return append([]byte(nil), b.buf[offset-b.start:]...), true
}

// reset empties the backlog and starts counting from offset
// (used after a follower loads a fresh snapshot).
func (b *backlog) reset(offset int64) {
	b.buf = b.buf[:0]
	b.start = offset
}
