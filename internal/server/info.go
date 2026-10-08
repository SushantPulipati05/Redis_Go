package server

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/SushantPulipati05/Redis_Go/internal/resp"
)

func cmdInfo(srv *Server, args []string) resp.Value {
	if len(args) > 1 {
		return wrongArgs("info")
	}
	return resp.Bulk(srv.replicationInfo())
}

// replicationInfo renders INFO replication using Redis's field names.
func (srv *Server) replicationInfo() string {
	var b strings.Builder
	b.WriteString("# Replication\r\n")
	if srv.isReplica.Load() {
		host, port, _ := net.SplitHostPort(srv.getLeaderAddr())
		status := "down"
		if srv.linkUp.Load() {
			status = "up"
		}
		fmt.Fprintf(&b, "role:slave\r\nmaster_host:%s\r\nmaster_port:%s\r\n", host, port)
		fmt.Fprintf(&b, "master_link_status:%s\r\n", status)
		fmt.Fprintf(&b, "slave_repl_offset:%d\r\n", srv.replOffset.Load())
	} else {
		srv.writeMu.Lock()
		fmt.Fprintf(&b, "role:master\r\nconnected_slaves:%d\r\n", len(srv.replicas))
		i := 0
		for r := range srv.replicas {
			host, port, _ := net.SplitHostPort(r.addr)
			lag := int(time.Since(time.Unix(0, r.lastAck.Load())).Seconds())
			fmt.Fprintf(&b, "slave%d:ip=%s,port=%s,offset=%d,lag=%d\r\n", i, host, port, r.ackOffset.Load(), lag)
			i++
		}
		quorum := srv.hasQuorumLocked()
		srv.writeMu.Unlock()
		if len(srv.peers) > 0 {
			fmt.Fprintf(&b, "cluster_nodes:%d\r\nwrites_allowed:%v\r\n", len(srv.peers)+1, quorum)
		}
		fmt.Fprintf(&b, "sync_full:%d\r\nsync_partial_ok:%d\r\n", srv.syncFull.Load(), srv.syncPartial.Load())
	}
	fmt.Fprintf(&b, "master_replid:%s\r\n", srv.getReplID())
	srv.writeMu.Lock()
	id2 := srv.replID2
	off2 := srv.secondOffset
	srv.writeMu.Unlock()
	if id2 != "" {
		fmt.Fprintf(&b, "master_replid2:%s\r\nsecond_repl_offset:%d\r\n", id2, off2)
	}
	fmt.Fprintf(&b, "master_repl_offset:%d\r\n", srv.replOffset.Load())
	fmt.Fprintf(&b, "failover_epoch:%d\r\n", srv.epoch.Load())
	return b.String()
}
