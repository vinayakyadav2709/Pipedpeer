package service

import (
	"os"
	"strings"
	"testing"
)

func TestUnitGolden(t *testing.T) {
	want := `[Unit]
Description=pipedpeer daemon
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/pipedpeer __daemon__
Environment=PATH=/usr/bin:/bin
Restart=on-failure
RestartSec=5
Delegate=yes

[Install]
WantedBy=default.target
`
	if got := Unit("/usr/local/bin/pipedpeer", "/usr/bin:/bin"); got != want {
		t.Fatalf("unit text changed:\n%s", got)
	}
}

func TestInstallEnableRemove(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	var calls []string
	systemctl = func(args ...string) (string, error) {
		calls = append(calls, "systemctl "+strings.Join(args, " "))
		return "", nil
	}
	loginctl = func(args ...string) (string, error) {
		calls = append(calls, "loginctl "+args[0])
		return "", nil
	}
	if Installed() {
		t.Fatal("installed before Install")
	}
	if err := Install(true); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(UnitPath())
	if err != nil || !strings.Contains(string(body), "__daemon__") {
		t.Fatalf("unit not written: %v %q", err, body)
	}
	if err := Remove(); err != nil {
		t.Fatal(err)
	}
	if Installed() {
		t.Fatal("still installed after Remove")
	}
	got := strings.Join(calls, "; ")
	want := "systemctl daemon-reload; systemctl enable pipedpeer.service; loginctl enable-linger; " +
		"systemctl disable --now pipedpeer.service; systemctl daemon-reload"
	if got != want {
		t.Fatalf("calls:\n got %s\nwant %s", got, want)
	}
}
