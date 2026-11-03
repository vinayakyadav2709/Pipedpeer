#!/usr/bin/env bash
# Rehearsal stand-in for scripts/install-pipedpeer.sh (headless-test.sh puts it
# in the code tarball). A real install needs crun and Nix in the container; this
# one installs a fake pipedpeer, and a fake nix as pipedpeer setup would.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
stubs=$here/campaign/test/headless-stubs
sudo -n install -m 755 "$stubs/pipedpeer" /usr/local/bin/pipedpeer
command -v nix >/dev/null || sudo -n install -m 755 "$stubs/nix" /usr/local/bin/nix
echo "installed fake pipedpeer $*"
pipedpeer setup -y
