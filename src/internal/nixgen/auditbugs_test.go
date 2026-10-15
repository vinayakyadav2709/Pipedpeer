package nixgen

import "testing"

// B1: PIPEDPEER_DDP_SYNC_EVERY=auto (any case) must not crash int() at DDP
// install. DataLoader._pp_shard is set at the end of _install_ddp.
func TestSyncEveryAutoInstallsDDP(t *testing.T) {
	src := `
import torch
import torch.utils.data
assert hasattr(torch.utils.data.DataLoader, "_pp_shard"), "DDP did not install"
print("AUTO-OK")
`
	for _, v := range []string{"auto", "AUTO", " Auto "} {
		runShimPythonEnv(t, "sync_auto", src, "AUTO-OK",
			"PIPEDPEER_SHIM=1", "PIPEDPEER_DDP=1", "PIPEDPEER_DDP_SYNC_EVERY="+v,
			"PIPEDPEER_DAEMON_URL=http://127.0.0.1:1", "PIPEDPEER_CLOSURE_NODES=0")
	}
}
