#!/usr/bin/env bash
# Build the campaign's one Python environment (flake.nix beside this script)
# and link it at $HOME/.local/share/pipedpeer-lab/env, so every system in the
# campaign runs the same interpreter and libraries:
#   $HOME/.local/share/pipedpeer-lab/env/bin/{python3,ray,dask,torchrun}
#
#   scripts/campaign/env/install.sh          # build (idempotent); prints the store path
#   scripts/campaign/env/install.sh remove   # drop the link and the store paths only it kept
#
# Needs nix (any install: multi-user or single-user). Rerunning with the link
# in place rebuilds nothing and prints the same path. The last line of a build
# is `LAB-ENV <store path>`; the runner and the setup scan parse it.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
state="$HOME/.local/share/pipedpeer-lab"
link="$state/env"
closure="$state/env.closure"   # the closure this script added, for `remove`

die() { echo "[lab-env] ERROR: $*" >&2; exit 1; }
log() { echo "[lab-env] $*" >&2; }

nix_bin() {
	command -v nix 2>/dev/null && return
	for c in /nix/var/nix/profiles/default/bin/nix "$HOME/.nix-profile/bin/nix"; do
		[[ -x $c ]] && { echo "$c"; return; }
	done
	die "nix not found; install it first (the setup step does)"
}

install_env() {
	local nix; nix="$(nix_bin)"
	[[ -f $here/flake.lock ]] || die "missing $here/flake.lock: the pin would float"
	mkdir -p "$state"
	log "building $here (first build downloads about 2.5 GiB from cache.nixos.org)"
	"$nix" --extra-experimental-features 'nix-command flakes' build \
		--no-write-lock-file --out-link "$link" "path:$here" >&2
	local out; out="$(readlink -f "$link")"
	for b in python3 ray dask torchrun; do
		[[ -x $link/bin/$b ]] || die "$link/bin/$b missing from $out"
	done
	"$nix" --extra-experimental-features nix-command path-info -r "$out" >"$closure"
	echo "LAB-ENV $out"
}

remove_env() {
	[[ -L $link || -f $closure ]] || { log "nothing installed"; return 0; }
	rm -f "$link"
	if [[ -f $closure ]]; then
		local nix; nix="$(nix_bin)"
		# Paths something else still roots (a pipedpeer closure, another
		# profile) stay alive and nix refuses them; that is expected. Passes
		# repeat because deleting a referrer is what frees its dependencies.
		local alive prev=-1
		while :; do
			alive=0
			while IFS= read -r p; do
				[[ -e $p ]] || continue
				"$nix" --extra-experimental-features nix-command store delete "$p" >/dev/null 2>&1 ||
					alive=$((alive + 1))
			done <"$closure"
			((alive == 0 || alive == prev)) && break
			prev=$alive
		done
		log "store paths still alive (kept by other roots): $alive"
		rm -f "$closure"
	fi
	rmdir "$state" 2>/dev/null || true
	log "removed"
}

case "${1:-install}" in
	install) install_env ;;
	remove) remove_env ;;
	*) die "usage: install.sh [install|remove]" ;;
esac
