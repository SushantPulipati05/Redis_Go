package main

// Automatic failover: when the leader dies, a follower takes over.
//
// Every node is started with -peers listing the other nodes. The hard part
// isn't promoting a follower -- it's making sure EXACTLY ONE gets promoted,
// even if two followers notice the dead leader at the same moment. Two
// leaders accepting different writes ("split brain") would corrupt the data.
//
// The approach is a simplified version of how Raft elects a leader:
//
//  1. EPOCHS. Every election has a number (epoch), which only goes up.
//     A newer epoch always wins over an older one.
//
//  2. VOTES. A follower whose leader has been silent for failoverAfter asks
//     every peer: "REPLVOTE <epoch> <me> <my offset>". A peer says yes only if:
//       - it can't reach a leader either (its own link is down, and it isn't
//         a working leader itself)          -> don't replace a live leader
//       - it hasn't voted for someone else in this epoch -> one vote per epoch
//       - the candidate's offset >= its own  -> never elect someone who has
//                                              less data than a voter
//     The candidate needs a MAJORITY of the whole cluster (counting itself).
//     Two candidates can't both get a majority in the same epoch, because
//     two majorities always share at least one node, and that node only
//     votes once. That's what rules out two leaders.
//
//  3. ANNOUNCE. The winner promotes itself and tells everyone
//     "REPLLEADER <epoch> <me>". Followers switch to it. An old leader that
//     comes back (or was cut off) learns there's a newer epoch and steps
//     down to become a follower.
//
//  4. NO WRITES WITHOUT A MAJORITY. A leader that can't hear from a majority
//     of the cluster (via REPLCONF ACKs) refuses writes with NOREPLICAS
//     (see hasQuorumLocked in replication.go). In a network split, only the
//     majority side can accept writes, so the two sides never diverge.
//
// Like real Redis, replication is asynchronous, so a write acknowledged in
// the last moment before a crash may not have reached anyone yet. Picking
// the follower with the most data (highest offset) keeps that loss minimal.

import (
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"
)

var (
	failoverAfter       = 3 * time.Second // follower: leader silent this long -> try failover
	electionJitter      = time.Second     // random pause before running for leader
	leaderWatchInterval = 2 * time.Second // leader: how often to check for a newer leader
	peerTimeout         = time.Second     // max time for one call to a peer
)

// majority is how many nodes (including ourselves) make more than half.
func (srv *Server) majority() int {
	return (len(srv.peers)+1)/2 + 1
}

// peerCall sends one command to another node and returns its reply.
func peerCall(addr string, parts ...string) (Value, error) {
	conn, err := net.DialTimeout("tcp", addr, peerTimeout)
	if err != nil {
		return Value{}, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(peerTimeout))

	vals := make([]Value, len(parts))
	for i, p := range parts {
		vals[i] = Bulk(p)
	}
	if _, err := conn.Write(ArrayOf(vals...).Marshal()); err != nil {
		return Value{}, err
	}
	return NewRespReader(conn).Read()
}

// peerStatus is what a node reports about itself (REPLSTATUS).
type peerStatus struct {
	addr   string
	leader bool
	epoch  int64
	offset int64
}

func askStatus(addr string) (peerStatus, bool) {
	v, err := peerCall(addr, "REPLSTATUS")
	if err != nil || v.Type != Array || len(v.Array) != 3 {
		return peerStatus{}, false
	}
	return peerStatus{
		addr:   addr,
		leader: v.Array[0].Str == "master",
		epoch:  v.Array[1].Num,
		offset: v.Array[2].Num,
	}, true
}

// findLeader asks every peer for its status and returns the reachable
// leader with the highest epoch, if any.
func (srv *Server) findLeader() (peerStatus, bool) {
	var best peerStatus
	found := false
	for _, p := range srv.peers {
		st, ok := askStatus(p)
		if ok && st.leader && (!found || st.epoch > best.epoch) {
			best, found = st, true
		}
	}
	return best, found
}

// handleLeaderDown runs on a follower whose leader has been silent too long.
func (srv *Server) handleLeaderDown() {
	// 1. Maybe the leader moved and someone else already took over:
	//    if a peer is a leader with an epoch at least as new as ours, follow it.
	if st, ok := srv.findLeader(); ok && st.epoch >= srv.epoch.Load() {
		srv.raiseEpoch(st.epoch)
		if st.addr != srv.getLeaderAddr() {
			log.Printf("failover: following %s (epoch %d)", st.addr, st.epoch)
			srv.setLeaderAddr(st.addr)
		}
		return
	}

	// 2. Nobody is leading. Wait a random moment so followers don't all
	//    start elections at the exact same time and split the vote.
	time.Sleep(time.Duration(rand.Int63n(int64(electionJitter) + 1)))
	if !srv.isReplica.Load() || srv.stopped.Load() || srv.sinceContact() < failoverAfter {
		return // something changed while we waited (e.g. someone announced a leader)
	}

	// 3. Run for leader.
	if srv.runElection() {
		srv.promote()
	}
}

// runElection asks every peer for a vote. Returns true if we won a majority.
func (srv *Server) runElection() bool {
	srv.electionMu.Lock()
	epoch := srv.epoch.Load() + 1
	srv.epoch.Store(epoch)
	srv.votedEpoch, srv.votedFor = epoch, srv.addr // vote for ourselves
	srv.electionMu.Unlock()

	offset := srv.replOffset.Load()
	votes := 1
	for _, p := range srv.peers {
		v, err := peerCall(p, "REPLVOTE", strconv.FormatInt(epoch, 10), srv.addr, strconv.FormatInt(offset, 10))
		if err == nil && v.Type == Integer && v.Num == 1 {
			votes++
		}
	}
	won := votes >= srv.majority()
	log.Printf("election for epoch %d: %d/%d votes (need %d) -> won=%v",
		epoch, votes, len(srv.peers)+1, srv.majority(), won)
	return won
}

// handleVote decides whether to vote for a candidate (REPLVOTE command).
func (srv *Server) handleVote(epoch int64, candidate string, candOffset int64) bool {
	srv.electionMu.Lock()
	defer srv.electionMu.Unlock()

	if !srv.isReplica.Load() {
		return false // we're a working leader: no failover needed
	}
	if srv.linkUp.Load() {
		return false // we can still reach the leader: it isn't dead
	}
	if epoch < srv.epoch.Load() {
		return false // an old election
	}
	if epoch > srv.epoch.Load() {
		srv.epoch.Store(epoch)
	}
	if srv.votedEpoch == epoch && srv.votedFor != "" && srv.votedFor != candidate {
		return false // already voted for someone else in this epoch
	}
	if candOffset < srv.replOffset.Load() {
		return false // we have more data than the candidate
	}
	srv.votedEpoch, srv.votedFor = epoch, candidate
	return true
}

// raiseEpoch moves our epoch forward (never backward).
func (srv *Server) raiseEpoch(e int64) {
	srv.electionMu.Lock()
	if e > srv.epoch.Load() {
		srv.epoch.Store(e)
	}
	srv.electionMu.Unlock()
}

// promote turns this follower into the leader.
func (srv *Server) promote() {
	srv.writeMu.Lock()
	if !srv.isReplica.Load() {
		srv.writeMu.Unlock()
		return
	}
	// Start a new history that continues the old one. Followers that were on
	// the old history (up to our offset) can partial-resync from us.
	srv.replID2 = srv.getReplID()
	srv.secondOffset = srv.replOffset.Load()
	srv.setReplID(newReplID())
	srv.isReplica.Store(false)
	srv.setLeaderAddr("")
	srv.writeMu.Unlock()

	epoch := srv.epoch.Load()
	log.Printf("PROMOTED to leader (epoch %d)", epoch)
	srv.startLeaderLoops()

	// Tell everyone. Nodes that are down will learn later via REPLSTATUS.
	for _, p := range srv.peers {
		go peerCall(p, "REPLLEADER", strconv.FormatInt(epoch, 10), srv.addr)
	}
}

// followLeader makes this node a follower of addr (REPLLEADER, or an old
// leader discovering a newer one).
func (srv *Server) followLeader(addr string, epoch int64) {
	srv.raiseEpoch(epoch)
	if addr == srv.addr {
		return
	}

	srv.writeMu.Lock()
	wasLeader := !srv.isReplica.Load()
	same := !wasLeader && srv.getLeaderAddr() == addr
	srv.setLeaderAddr(addr)
	srv.isReplica.Store(true)
	if wasLeader {
		// We're no longer in charge: drop our followers (they'll be told
		// about the new leader too).
		for r := range srv.replicas {
			srv.removeReplicaLocked(r)
		}
	}
	srv.writeMu.Unlock()

	if same {
		return
	}
	if wasLeader {
		log.Printf("stepping down: %s is the leader for epoch %d", addr, epoch)
	} else {
		log.Printf("switching to new leader %s (epoch %d)", addr, epoch)
	}
	srv.markContact()
	srv.closeLeaderConn() // break the old link; the loop reconnects to addr
	srv.startReplicaLink()
}

// leaderWatchLoop runs on a leader: if another node has become leader with a
// newer epoch (we were cut off, or we're an old leader that came back),
// step down and follow it.
func (srv *Server) leaderWatchLoop() {
	for !srv.stopped.Load() && !srv.isReplica.Load() {
		time.Sleep(leaderWatchInterval)
		if st, ok := srv.findLeader(); ok && st.epoch > srv.epoch.Load() {
			srv.followLeader(st.addr, st.epoch)
			return
		}
	}
}

// discoverLeaderAtStartup runs before a node configured as leader starts
// serving. If the cluster already has a leader with a newer epoch (e.g. we
// crashed and someone took over), we join as a follower instead of creating
// a second leader.
func (srv *Server) discoverLeaderAtStartup() bool {
	st, ok := srv.findLeader()
	if !ok || st.addr == srv.addr {
		return false
	}
	srv.raiseEpoch(st.epoch)
	srv.setLeaderAddr(st.addr)
	srv.isReplica.Store(true)
	log.Printf("found existing leader %s (epoch %d): starting as its follower", st.addr, st.epoch)
	return true
}

// ---------- commands used between nodes ----------

// REPLSTATUS -> [role, epoch, offset]
func cmdReplStatus(srv *Server, args []string) Value {
	role := "master"
	if srv.isReplica.Load() {
		role = "slave"
	}
	return ArrayOf(Bulk(role), Int(srv.epoch.Load()), Int(srv.replOffset.Load()))
}

// REPLVOTE <epoch> <candidate> <offset> -> 1 (yes) or 0 (no)
func cmdReplVote(srv *Server, args []string) Value {
	if len(args) != 3 {
		return wrongArgs("replvote")
	}
	epoch, err1 := strconv.ParseInt(args[0], 10, 64)
	offset, err2 := strconv.ParseInt(args[2], 10, 64)
	if err1 != nil || err2 != nil {
		return errNotInt
	}
	return boolInt(srv.handleVote(epoch, args[1], offset))
}

// REPLLEADER <epoch> <addr> -> 1 if accepted, 0 if it's an old epoch
func cmdReplLeader(srv *Server, args []string) Value {
	if len(args) != 2 {
		return wrongArgs("replleader")
	}
	epoch, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return errNotInt
	}
	if epoch < srv.epoch.Load() {
		return Int(0)
	}
	if args[1] == srv.addr && !srv.isReplica.Load() {
		return Int(1)
	}
	srv.followLeader(args[1], epoch)
	return Int(1)
}
