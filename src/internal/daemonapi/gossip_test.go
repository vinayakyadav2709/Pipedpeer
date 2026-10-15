package daemonapi

import (
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/identity"
	"github.com/pipedpeer/pipedpeer/internal/nodestore"
)

func testKey(t *testing.T) identity.KeyPair {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return identity.KeyPair{Public: pub, Private: priv}
}

func signedRecord(k identity.KeyPair, seq int64, hosts ...string) addrRecord {
	rec := addrRecord{
		NodeID: k.Fingerprint(), Seq: seq, Hosts: hosts,
		DaemonPort: 4242, DirectPort: directPort, IntroducerPort: 4244,
		Pub: identity.EncodePublic(k.Public),
	}
	rec.Sig = k.Sign(addrSigContext, rec.canonical())
	return rec
}

type hintLog map[string][]string

func (h hintLog) hint(node string, c []string) { h[node] = append(h[node], c...) }

func gossipPaths(t *testing.T, s *Server, id string) []nodestore.Path {
	t.Helper()
	ps, err := s.store.Paths(id)
	if err != nil {
		t.Fatal(err)
	}
	return ps
}

// A valid record's addresses become candidates: a gossip path (never active
// until the poller verifies it) and a hint to internet mode. Loopback hosts
// are skipped: another machine's loopback is ours. The host is public
// (TEST-NET-3): a private one off this machine's networks gets no hint (E6),
// and which ranges those are depends on the machine running the test.
func TestGossipRecordBecomesCandidates(t *testing.T) {
	s := nodeTestServer(t, "me")
	hints := hintLog{}
	s.SetHinter(hints.hint)
	k := testKey(t)

	s.learnAddresses("peer", addressesResp{Self: ptr(signedRecord(k, 1, "203.0.113.7", "127.0.0.1"))})

	ps := gossipPaths(t, s, k.Fingerprint())
	if len(ps) != 1 || ps[0].Kind != nodestore.PathGossip || ps[0].Host != "203.0.113.7" || ps[0].Port != 4242 {
		t.Fatalf("paths = %+v, want one gossip candidate 203.0.113.7:4242", ps)
	}
	if got := hints[k.Fingerprint()]; len(got) != 1 || got[0] != "203.0.113.7:4243" {
		t.Fatalf("hints = %v, want [203.0.113.7:4243]", got)
	}

	// An older or equal seq changes nothing.
	s.learnAddresses("peer", addressesResp{Peers: []addrRecord{signedRecord(k, 1, "10.9.9.9")}})
	if ps := gossipPaths(t, s, k.Fingerprint()); len(ps) != 1 {
		t.Fatalf("stale record applied: %+v", ps)
	}
}

// A record whose signature does not verify, or that claims a node id its key
// does not have, is dropped: no member can move another's address.
func TestGossipRecordWithBadSignatureIsDropped(t *testing.T) {
	s := nodeTestServer(t, "me")
	hints := hintLog{}
	s.SetHinter(hints.hint)
	victim := testKey(t)
	attacker := testKey(t)

	tampered := signedRecord(victim, 5, "10.0.0.1")
	tampered.Hosts = []string{"203.0.113.66"}

	forged := signedRecord(attacker, 5, "203.0.113.66")
	forged.NodeID = victim.Fingerprint()
	forged.Sig = attacker.Sign(addrSigContext, forged.canonical())

	s.learnAddresses("peer", addressesResp{Peers: []addrRecord{tampered, forged}})

	if ps := gossipPaths(t, s, victim.Fingerprint()); len(ps) != 0 {
		t.Fatalf("bad record became paths %+v", ps)
	}
	if len(hints) != 0 {
		t.Fatalf("bad record hinted %v", hints)
	}
}

// GET /v1/addresses serves this node's own record, signed so that a peer
// verifies it, plus the records it learned, verbatim.
func TestAddressesServesSignedRecords(t *testing.T) {
	key, err := identity.Key()
	if err != nil {
		t.Fatal(err)
	}
	s := nodeTestServer(t, key.Fingerprint())
	s.SetSelfInfo("192.168.1.20", 4242, map[string]string{})
	other := testKey(t)
	s.learnAddresses("peer", addressesResp{Self: ptr(signedRecord(other, 7, "10.4.4.4"))})

	rec := httptest.NewRecorder()
	s.handleAddresses(rec, httptest.NewRequest(http.MethodGet, "/v1/addresses", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var got addressesResp
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Self == nil || got.Self.verify() != nil || got.Self.NodeID != key.Fingerprint() ||
		len(got.Self.Hosts) != 1 || got.Self.Hosts[0] != "192.168.1.20" {
		t.Fatalf("self record %+v: want verified, hosts [192.168.1.20]", got.Self)
	}
	if len(got.Peers) != 1 || got.Peers[0].verify() != nil || got.Peers[0].NodeID != other.Fingerprint() {
		t.Fatalf("peers %+v: want the learned record, still verifying", got.Peers)
	}
}

// GET /v1/introducers is [] without internet mode and the report with it.
func TestIntroducersServed(t *testing.T) {
	s := New("me")
	get := func() []IntroducerState {
		rec := httptest.NewRecorder()
		s.handleIntroducers(rec, httptest.NewRequest(http.MethodGet, "/v1/introducers", nil))
		var out []IntroducerState
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if got := get(); got == nil || len(got) != 0 {
		t.Fatalf("no internet mode: %#v, want []", got)
	}
	s.SetIntroducerStatus(func() []IntroducerState {
		return []IntroducerState{{Addr: "203.0.113.1:4244", Members: 3}}
	})
	if got := get(); len(got) != 1 || got[0].Members != 3 {
		t.Fatalf("got %+v", got)
	}
}

func ptr[T any](v T) *T { return &v }
