package heartbeat

import (
	"os"
	"strings"
	"sync"
)

// MachineCapability is the key under which a node advertises which physical
// machine it is running on.
const MachineCapability = "machine"

var machineOnce struct {
	sync.Once
	id string
}

// Machine identifies the physical machine this daemon runs on, so two daemons
// that share one can find out that they do.
//
// It matters because free memory is a property of the machine, not of the
// daemon. Two daemons on one host each read the host's free memory, each
// subtract only their own reservations, and each conclude they have the whole
// of it - so both admit work that between them the machine cannot hold. That
// is not a hypothetical: forcing a large matmul spill drove a 14 GB test
// machine to 194 MB free and the kernel killed its desktop shell. A container
// worker beside its host's daemon is an ordinary arrangement, and the lab
// scripts here create exactly it.
//
// The kernel's boot id is the right identifier. Containers share the host
// kernel, so a container and its host read the same value, which is precisely
// the co-location this needs to detect; two real machines never share one; and
// it needs no configuration and no privilege. /etc/machine-id is not a
// substitute - a container usually has its own, or none at all.
//
// Empty when it cannot be read, which callers must treat as "unknown", never
// as "same machine": guessing co-location where there is none would shrink
// every node's usable memory for no reason.
func Machine() string {
	machineOnce.Do(func() {
		b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
		if err != nil {
			return
		}
		machineOnce.id = strings.TrimSpace(string(b))
	})
	return machineOnce.id
}

// SameMachine reports whether two advertised machine ids are the same physical
// machine. Unknown on either side is not the same machine.
func SameMachine(a, b string) bool {
	return a != "" && a == b
}
