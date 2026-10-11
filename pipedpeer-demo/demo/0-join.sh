#!/usr/bin/env bash
# Live join - a machine left out at prep joins on stage (run on PC-3).
#
#   ./demo/0-join.sh <LAPTOP-A LAN IP> [cluster-token]
#
# The token is needed only if this machine never ran 'pipedpeer auth set'
# (print it on LAPTOP-A with: pipedpeer auth show).
source "$(dirname "$0")/_lib.sh"

ip="${1:-}"
token="${2:-}"
if [[ -z "$ip" ]]; then
	echo "usage: ./demo/0-join.sh <LAPTOP-A LAN IP> [cluster-token]" >&2
	exit 2
fi

banner "A new machine joins"
if [[ -n "$token" ]]; then
	show "$PIPE" auth set "$token" || { echo "${R}  auth set failed - check the token (pipedpeer auth show on LAPTOP-A)${N}"; exit 1; }
fi
note "Watch LAPTOP-A's dashboard: a new row appears and the status line says this PC is up."
pause
show "$PIPE" join "$ip"
rc=$?
echo
if [[ "$rc" -ne 0 ]]; then
	echo "${R}  join failed (exit $rc). Read the reasons above, then: pipedpeer net-check${N}"
	exit "$rc"
fi
show "$PIPE" nodes
echo
note "It found the others by itself; LAPTOP-A only introduced them."
