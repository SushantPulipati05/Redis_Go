package server

// Automatic failover. Elections follow the core rules of Raft: epochs only
// increase, each node votes at most once per epoch, a candidate needs votes
// from a majority of the whole cluster, and nodes refuse candidates with less
// data than themselves. Since any two majorities overlap, at most one leader
// can be elected per epoch.

import (
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/SushantPulipati05/Redis_Go/internal/resp"
)

var (
	failoverAfter       = 3 * time.Second
	electionJitter      = time.Second
	leaderWatchInterval = 2 * time.Second
	peerTimeout         = time.Second
)

func (srv *Server) majority() int {
	return (len(srv.peers)+1)/2 + 1
}

// peerCall sends a single command to another node and returns the reply.
func peerCall(addr string, parts ...string) (resp.Value, error) {
	conn, err := net.DialTimeout("tcp", addr, peerTimeout)
	if err != nil {
		return resp.Value{}, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(peerTimeout))

	if _, err := conn.Write(resp.Command(parts...)); err != nil {
		return resp.Value{}, err
	}
	return resp.NewReader(conn).Read()
}

type peerStatus struct {
	addr   string
	leader bool
	epoch  int64
	offset int64
}

func askStatus(addr string) (peerStatus, bool) {
	v, err := peerCall(addr, "REPLSTATUS")
	if err != nil || v.Type != resp.Array || len(v.Array) != 3 {
		return peerStatus{}, false
	}
	return peerStatus{
		addr:   addr,
		leader: v.Array[0].Str == "master",
		epoch:  v.Array[1].Num,
		offset: v.Array[2].Num,
	}, true
}

// findLeader returns the reachable leader with the highest epoch.
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

// handleLeaderDown runs when the leader has been silent for failoverAfter.
// It follows an already-elected leader if there is one, otherwise waits a
// random delay (to avoid split votes) and runs for election.
func (srv *Server) handleLeaderDown() {
	if st, ok := srv.findLeader(); ok && st.epoch >= srv.epoch.Load() {
		srv.raiseEpoch(st.epoch)
		if st.addr != srv.getLeaderAddr() {
			log.Printf("failover: following %s (epoch %d)", st.addr, st.epoch)
			srv.setLeaderAddr(st.addr)
		}
		return
	}

	time.Sleep(time.Duration(rand.Int63n(int64(electionJitter) + 1)))
	if !srv.isReplica.Load() || srv.stopped.Load() || srv.sinceContact() < failoverAfter {
		return
	}

	if srv.runElection() {
		srv.promote()
	}
}

func (srv *Server) runElection() bool {
	srv.electionMu.Lock()
	epoch := srv.epoch.Load() + 1
	srv.epoch.Store(epoch)
	srv.votedEpoch, srv.votedFor = epoch, srv.addr
	srv.electionMu.Unlock()

	offset := srv.replOffset.Load()
	votes := 1
	for _, p := range srv.peers {
		v, err := peerCall(p, "REPLVOTE", strconv.FormatInt(epoch, 10), srv.addr, strconv.FormatInt(offset, 10))
		if err == nil && v.Type == resp.Integer && v.Num == 1 {
			votes++
		}
	}
	won := votes >= srv.majority()
	log.Printf("election for epoch %d: %d/%d votes (need %d) -> won=%v",
		epoch, votes, len(srv.peers)+1, srv.majority(), won)
	return won
}

// handleVote decides a REPLVOTE request.
func (srv *Server) handleVote(epoch int64, candidate string, candOffset int64) bool {
	srv.electionMu.Lock()
	defer srv.electionMu.Unlock()

	if !srv.isReplica.Load() {
		return false // we are a working leader
	}
	if srv.linkUp.Load() {
		return false // our leader is still reachable
	}
	if epoch < srv.epoch.Load() {
		return false
	}
	if epoch > srv.epoch.Load() {
		srv.epoch.Store(epoch)
	}
	if srv.votedEpoch == epoch && srv.votedFor != "" && srv.votedFor != candidate {
		return false // already voted this epoch
	}
	if candOffset < srv.replOffset.Load() {
		return false // candidate has less data than us
	}
	srv.votedEpoch, srv.votedFor = epoch, candidate
	return true
}

func (srv *Server) raiseEpoch(e int64) {
	srv.electionMu.Lock()
	if e > srv.epoch.Load() {
		srv.epoch.Store(e)
	}
	srv.electionMu.Unlock()
}

// promote makes this follower the leader. It starts a new replication history
// but remembers the old one, so other followers can resync partially.
func (srv *Server) promote() {
	srv.writeMu.Lock()
	if !srv.isReplica.Load() {
		srv.writeMu.Unlock()
		return
	}
	srv.replID2 = srv.getReplID()
	srv.secondOffset = srv.replOffset.Load()
	srv.setReplID(newReplID())
	srv.isReplica.Store(false)
	srv.setLeaderAddr("")
	srv.writeMu.Unlock()

	epoch := srv.epoch.Load()
	log.Printf("PROMOTED to leader (epoch %d)", epoch)
	srv.startLeaderLoops()

	for _, p := range srv.peers {
		go peerCall(p, "REPLLEADER", strconv.FormatInt(epoch, 10), srv.addr)
	}
}

// followLeader switches this node to follow addr, stepping down if it was
// leading.
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
	srv.closeLeaderConn()
	srv.startReplicaLink()
}

// leaderWatchLoop makes a leader step down if it finds a leader with a newer
// epoch, e.g. after being partitioned away.
func (srv *Server) leaderWatchLoop() {
	for !srv.stopped.Load() && !srv.isReplica.Load() {
		time.Sleep(leaderWatchInterval)
		if st, ok := srv.findLeader(); ok && st.epoch > srv.epoch.Load() {
			srv.followLeader(st.addr, st.epoch)
			return
		}
	}
}

// discoverLeaderAtStartup joins an existing leader instead of starting as a
// second one, e.g. when a former leader restarts after a failover.
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

// REPLSTATUS -> [role, epoch, offset]
func cmdReplStatus(srv *Server, args []string) resp.Value {
	role := "master"
	if srv.isReplica.Load() {
		role = "slave"
	}
	return resp.ArrayOf(resp.Bulk(role), resp.Int(srv.epoch.Load()), resp.Int(srv.replOffset.Load()))
}

// REPLVOTE epoch candidate offset -> 1 if granted
func cmdReplVote(srv *Server, args []string) resp.Value {
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

// REPLLEADER epoch addr -> announces a new leader
func cmdReplLeader(srv *Server, args []string) resp.Value {
	if len(args) != 2 {
		return wrongArgs("replleader")
	}
	epoch, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return errNotInt
	}
	if epoch < srv.epoch.Load() {
		return resp.Int(0)
	}
	if args[1] == srv.addr && !srv.isReplica.Load() {
		return resp.Int(1)
	}
	srv.followLeader(args[1], epoch)
	return resp.Int(1)
}
