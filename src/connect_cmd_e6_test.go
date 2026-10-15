package main

import (
	"slices"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/internet"
)

// E6: an invite's private LAN address that never answered from here is not
// kept as an introducer; one that answered, a public address and a name are.
func TestConnectKeepsOnlyIntroducersThatAnsweredOrArePublic(t *testing.T) {
	addrs := []string{"192.168.0.2:4244", "10.1.2.3:4244", "203.0.113.7:4244", "intro.example.org:4244"}
	states := []internet.IntroducerState{
		{Addr: "192.168.0.2:4244", LastErr: "no answer for 1m0s"},
		{Addr: "10.1.2.3:4244", LastReply: time.Now()},
		{Addr: "203.0.113.7:4244", LastErr: "no answer"},
	}
	got := keptIntroducers(addrs, states)
	if want := addrs[1:]; !slices.Equal(got, want) {
		t.Errorf("kept %v, want %v", got, want)
	}
	if got := keptIntroducers(addrs[:1], states); !slices.Equal(got, addrs[:1]) {
		t.Errorf("nothing else to keep: got %v, want the invite's list unchanged", got)
	}
}

// E6: `jobs last` said "ran on 127.0.0.1" for a local run; the recorded node
// name wins over the host, and old records keep the host.
func TestJobsShowTheNodeNameNotTheLoopbackHost(t *testing.T) {
	if got := whereRan("", "127.0.0.1", "this machine"); got != "this machine" {
		t.Errorf("local run: %q", got)
	}
	if got := whereRan("", "127.0.0.1", "fedora"); got != "fedora" {
		t.Errorf("internet peer: %q", got)
	}
	if got := whereRan("", "10.0.0.4", ""); got != "10.0.0.4" {
		t.Errorf("record without a name: %q", got)
	}
}
