package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pipedpeer/pipedpeer/internal/authtoken"
)

// fakeDaemon serves the three lists connect reads.
func fakeConnDaemon(t *testing.T, nodes, intros, unreachable string) string {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/nodes":
			w.Write([]byte(nodes))
		case "/v1/introducers":
			w.Write([]byte(intros))
		case "/v1/peers/unreachable":
			w.Write([]byte(unreachable))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(s.Close)
	return s.URL
}

const selfRow = `{"node_id":"aaaaaaaaaaaaaaaa","state":"healthy","source":"self","capabilities":{"hostname":"me"}}`

func TestConnectClassifiesThreeOutcomes(t *testing.T) {
	replied := `[{"addr":"10.0.0.1:4244","last_reply":"2026-10-15T10:00:00Z","members":3}]`
	cases := []struct {
		name, nodes, intros, unreach string
		want                         connOutcome
		mention                      string
	}{
		{"connected",
			`[` + selfRow + `,{"node_id":"bbbbbbbbbbbbbbbb","state":"healthy","source":"lan","capabilities":{"hostname":"pc1"}}]`,
			replied, `[]`, connConnected, "pc1"},
		{"partly",
			`[` + selfRow + `,{"node_id":"bbbbbbbbbbbbbbbb","state":"healthy","source":"lan","capabilities":{"hostname":"pc1"}},` +
				`{"node_id":"cccccccccccccccc","state":"unreachable","source":"internet","reason":"context deadline exceeded","capabilities":{"hostname":"pc2"}}]`,
			replied, `[]`, connPartly, "4242/tcp"},
		{"nobody answered",
			`[` + selfRow + `]`,
			`[{"addr":"10.0.0.1:4244","last_reply":"0001-01-01T00:00:00Z","last_err":"no reply"}]`, `[]`,
			connFailed, "no introducer answered"},
		// The real laptops: no introducer reachable, and a candidate's token
		// check timed out. Nothing refused the secret.
		{"no introducer, token check timed out",
			`[` + selfRow + `,{"node_id":"cccccccccccccccc","state":"unreachable","source":"gossip",` +
				`"reason":"Get \"http://192.168.1.5:4242/health\": 192.168.1.5:4242 could not prove it holds the cluster token; not sending it: context deadline exceeded"}]`,
			`[{"addr":"192.168.1.5:4244","last_reply":"0001-01-01T00:00:00Z","last_err":"no reply"},` +
				`{"addr":"203.0.113.7:4244","last_reply":"0001-01-01T00:00:00Z"}]`, `[]`,
			connFailed, "Tailscale"},
		{"no introducer, peer did not answer the token check",
			`[` + selfRow + `]`,
			`[{"addr":"192.168.1.5:4244","last_reply":"0001-01-01T00:00:00Z"}]`,
			`[{"peer":"dddddddddddddddd","reason":"192.168.1.5:4242 did not answer the cluster token check; not sending it: EOF"}]`,
			connFailed, "no introducer answered (tried 192.168.1.5:4244)"},
		{"answered without proof",
			`[` + selfRow + `]`, replied,
			`[{"peer":"dddddddddddddddd","reason":"10.0.0.9:4242 answered but could not prove it holds the cluster token; not sending it: its proof does not match"}]`,
			connFailed, "rejected this machine's secret"},
		{"secret rejected",
			`[` + selfRow + `]`, replied,
			`[{"peer":"dddddddddddddddd","reason":"could not prove it holds the cluster token"}]`,
			connFailed, "secret"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := snapshot(fakeConnDaemon(t, c.nodes, c.intros, c.unreach))
			if err != nil {
				t.Fatal(err)
			}
			r := classify(s)
			if r.outcome != c.want {
				t.Fatalf("outcome %d, want %d: %+v", r.outcome, c.want, r)
			}
			var b bytes.Buffer
			printConnResult(&b, r)
			if !strings.Contains(b.String(), c.mention) {
				t.Fatalf("output lacks %q:\n%s", c.mention, b.String())
			}
			if !strings.Contains(c.mention, "secret") && strings.Contains(b.String(), "rejected") {
				t.Fatalf("blames the secret with nothing refusing it:\n%s", b.String())
			}
		})
	}
}

func TestDuplicateHostnamesGetIDs(t *testing.T) {
	r := classify(connSnapshot{nodes: []connNode{
		{NodeID: "1111111111111111", State: "healthy", Capabilities: map[string]string{"hostname": "psipl"}},
		{NodeID: "2222222222222222", State: "healthy", Capabilities: map[string]string{"hostname": "psipl"}},
	}})
	if strings.Join(r.healthy, ",") != "psipl (11111111),psipl (22222222)" {
		t.Fatal(r.healthy)
	}
}

func TestInvitePrintsOneLine(t *testing.T) {
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv(authtoken.EnvVar, "")
	if err := authtoken.Set("sekrit"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = authtoken.Set("") })
	cmd := newInviteCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs(nil)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "pipedpeer connect pp1-") {
		t.Fatalf("invite printed %q", out.String())
	}
}
