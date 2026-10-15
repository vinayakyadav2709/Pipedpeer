package heartbeat

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"
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
// The key is a hash of /etc/machine-id and the kernel's boot id, whichever
// exist. Boot id alone made every container on one host one machine (they
// share the kernel), which skewed the co-machine memory logic and the DDP
// decisions in the emulated lab; machine-id alone is not safe either, since
// cloned PC images share it. Together: containers with their own machine-id
// differ, and cloned PCs still differ by boot id. A container with no
// machine-id of its own no longer matches its host.
//
// Empty when neither can be read, which callers must treat as "unknown",
// never as "same machine": guessing co-location where there is none would
// shrink every node's usable memory for no reason.
func Machine() string {
	machineOnce.Do(func() {
		machineOnce.id = machineKey(machineIDPath, bootIDPath)
	})
	return machineOnce.id
}

// Where Machine reads its two parts; variables so a test can point them at
// fake files.
var (
	machineIDPath = "/etc/machine-id"
	bootIDPath    = "/proc/sys/kernel/random/boot_id"
)

// machineKey hashes the contents of whichever of the two files exist; "" when
// neither does. A missing machine-id is normal in a container, so it is not
// logged; a missing boot id is (Linux always has one).
func machineKey(machineIDFile, bootIDFile string) string {
	read := func(path string) string {
		b, err := os.ReadFile(path)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(b))
	}
	mid, boot := read(machineIDFile), read(bootIDFile)
	if boot == "" {
		log.Warn().Str("path", bootIDFile).Msg("no boot id; machine key from machine-id alone")
	}
	if mid == "" && boot == "" {
		return ""
	}
	sum := sha256.Sum256([]byte("pipedpeer-machine/1\x00" + mid + "\x00" + boot))
	return hex.EncodeToString(sum[:16])
}

// SameMachine reports whether two advertised machine ids are the same physical
// machine. Unknown on either side is not the same machine.
func SameMachine(a, b string) bool {
	return a != "" && a == b
}
