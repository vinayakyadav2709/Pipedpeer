package netaddr

import (
	"net"
	"slices"
	"testing"
)

// The Android fallback must see what net sees on a host where net works.
func TestNetlinkDumpMatchesNet(t *testing.T) {
	want, err := net.InterfaceAddrs()
	if err != nil {
		t.Skipf("net cannot list addresses here: %v", err)
	}
	ifs, byIndex, err := dumpAddrs()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, a := range byIndex {
		for _, x := range a {
			got = append(got, x.String())
		}
	}
	var ws []string
	for _, a := range want {
		ws = append(ws, a.String())
	}
	slices.Sort(got)
	slices.Sort(ws)
	if !slices.Equal(got, ws) {
		t.Fatalf("dump %v, net %v", got, ws)
	}
	for _, ifc := range ifs {
		real, err := net.InterfaceByIndex(ifc.Index)
		if err != nil {
			t.Fatal(err)
		}
		if ifc.Name != real.Name || ifc.Flags != real.Flags {
			t.Fatalf("dump %d = %s %v, net %s %v", ifc.Index, ifc.Name, ifc.Flags, real.Name, real.Flags)
		}
	}
}
