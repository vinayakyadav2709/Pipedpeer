#!/usr/bin/env bash
# install-otelcol.sh [install|remove]: the pinned otelcol-contrib (lib.sh) in
# ~/.local/share/pipedpeer-lab/bin, checksum-verified, no root. remove also
# stops a node collector and deletes its state. Linux amd64 and arm64.
# OTELCOL_TARBALL=<path> installs from an already downloaded release tarball.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$here/lib.sh"

case "${1:-install}" in
install) lab_install_otelcol ;;
remove)
	# The collector, pp-poll.py and tcpdump: node.sh stop knows all three.
	bash "$here/node.sh" stop >/dev/null 2>&1 || lab_stop_pid "$LAB_OTEL/node.pid"
	rm -rf "$LAB_OTEL" "$LAB_BIN/otelcol-contrib" "$LAB_HOME/dl"
	rmdir "$LAB_BIN" "$LAB_HOME" 2>/dev/null || true
	echo "removed otelcol-contrib and $LAB_OTEL"
	;;
*)
	echo "usage: $0 [install|remove]" >&2
	exit 2
	;;
esac
