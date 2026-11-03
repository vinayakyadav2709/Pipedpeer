#!/usr/bin/env bash
# Rehearsal stand-in for scripts/campaign/env/install.sh: the env directory
# with the system python3 instead of the 2.5 GiB Nix closure.
set -euo pipefail
state=$HOME/.local/share/pipedpeer-lab
case "${1:-install}" in
  install)
    mkdir -p "$state/env/bin"
    ln -sfn "$(command -v python3)" "$state/env/bin/python3"
    for b in ray dask torchrun; do printf '#!/bin/sh\necho fake %s "$@"\n' "$b" >"$state/env/bin/$b"; chmod +x "$state/env/bin/$b"; done
    echo "LAB-ENV /nix/store/rehearsal-python3-env" ;;
  remove) rm -rf "$state/env"; rmdir "$state" 2>/dev/null || true ;;
esac
