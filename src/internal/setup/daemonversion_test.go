package setup

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Cases (daemonVersion, httptest):
// | Input | Expected |
// |---|---|
// | 200 {"capabilities":{"version":"v1"}} | "v1", nil |
// | 200 {} | "", nil (older build) |
// | 500 with a JSON body | "", error (the finding) |
// | 200 with bad JSON | error |
// | 401 | error; staleDaemon reports "unknown", never a build it did not read |
func TestDaemonVersion_CaseTable(t *testing.T) {
	cases := []struct {
		name        string
		statusCode  int
		body        string
		wantVersion string
		wantErr     bool
	}{
		{
			name:        "200 with capabilities version",
			statusCode:  http.StatusOK,
			body:        `{"capabilities":{"version":"v1"}}`,
			wantVersion: "v1",
			wantErr:     false,
		},
		{
			name:        "200 empty JSON (older build)",
			statusCode:  http.StatusOK,
			body:        `{}`,
			wantVersion: "",
			wantErr:     false,
		},
		{
			name:        "500 with a JSON body",
			statusCode:  http.StatusInternalServerError,
			body:        `{"version":"v1","error":"internal error"}`,
			wantVersion: "",
			wantErr:     true,
		},
		{
			name:        "200 with bad JSON",
			statusCode:  http.StatusOK,
			body:        `{invalid json`,
			wantVersion: "",
			wantErr:     true,
		},
		{
			name:        "401 unauthorized",
			statusCode:  http.StatusUnauthorized,
			body:        `{"error":"unauthorized"}`,
			wantVersion: "",
			wantErr:     true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(c.statusCode)
				_, _ = w.Write([]byte(c.body))
			}))
			defer srv.Close()

			port := srv.Listener.Addr().(*net.TCPAddr).Port
			gotV, err := daemonVersion(port)
			if (err != nil) != c.wantErr {
				t.Fatalf("daemonVersion() err = %v, wantErr = %v", err, c.wantErr)
			}
			if gotV != c.wantVersion {
				t.Fatalf("daemonVersion() version = %q, want %q", gotV, c.wantVersion)
			}
			if c.statusCode == http.StatusUnauthorized {
				// The answer carries no version, so none may be reported for it.
				oldV := Version
				Version = "1.2.0"
				defer func() { Version = oldV }()
				stale, from := staleDaemon(port)
				if !stale || from != "unknown" {
					t.Fatalf("staleDaemon on 401 = %v, %q; want true, \"unknown\"", stale, from)
				}
			}
		})
	}
}
