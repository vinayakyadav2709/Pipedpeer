package internet

import (
	"context"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/nodestore"
)

// TestReplacedLinksLeaveOneNodeRow (FIX-NAT5). A link that dies while OnPeer
// is still registering it had its OnPeerGone run first: the leave removed
// nothing, the join then wrote a row for a forwarder port nobody listens on,
// and manual rows are never pruned. Ten such rows were seen on one machine.
// The callbacks here are the daemon's, against a real node store.
func TestReplacedLinksLeaveOneNodeRow(t *testing.T) {
	store, err := nodestore.NewAt(filepath.Join(t.TempDir(), "nodes.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	split := func(addr string) (string, int) {
		host, p, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatal(err)
		}
		port, _ := strconv.Atoi(p)
		return host, port
	}
	joining := make(chan struct{})
	release := make(chan struct{})
	m := New(Config{
		OnPeer: func(node, addr string) {
			joining <- struct{}{}
			<-release
			// What AddManual writes when the health check through the
			// forwarder fails: a placeholder row at the forwarder address.
			host, port := split(addr)
			if err := store.UpsertNode(nodestore.Node{NodeID: "manual-" + addr, Host: host,
				Port: port, Source: "manual", IsManual: true}); err != nil {
				t.Error(err)
			}
		},
		OnPeerGone: func(node, addr string) {
			host, port := split(addr)
			if _, err := store.RemoveManualAt(host, port); err != nil {
				t.Error(err)
			}
		},
	})

	defer m.closeAll()

	const replaced = 5
	ctx := context.Background()
	for i := 0; i <= replaced; i++ {
		up := make(chan error, 1)
		go func() { up <- m.linkUp(ctx, "peer-a", nil, "punched") }()
		<-joining
		dropped := make(chan struct{})
		if i < replaced {
			// The link dies before its join has been recorded.
			go func() { m.dropLink("peer-a"); close(dropped) }()
			time.Sleep(20 * time.Millisecond) // let the leave run first if it can
		} else {
			close(dropped) // the last link stays up
		}
		release <- struct{}{}
		if err := <-up; err != nil {
			t.Fatal(err)
		}
		<-dropped
	}

	rows, err := store.ListAll()
	if err != nil {
		t.Fatal(err)
	}
	want := m.Peers()["peer-a"]
	if len(rows) != 1 || rows[0].NodeID != "manual-"+want {
		var ids []string
		for _, r := range rows {
			ids = append(ids, r.NodeID)
		}
		t.Fatalf("after %d replaced links want one row for the live forwarder %s, got %v",
			replaced, want, ids)
	}
}
