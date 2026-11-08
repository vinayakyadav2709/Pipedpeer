#!/usr/bin/env bash
# Port of docs/benchmarks/workloads/trivial.py (fixed cost) to plain ssh: GNU
# parallel runs one `echo` on the cluster. The sshlogin list is written bottom
# up, so the single job lands on the *last* listed PC (not the driver) and the
# measured fixed cost includes the real remote hop.
# Distributed: the job itself. Local: nothing (the line comes back from it).
# Follows: https://www.gnu.org/software/parallel/parallel_tutorial.html
# Result line, byte-for-byte as the original: TRIVIAL-OK
set -euo pipefail

: "${LAB_HOSTS_FILE:?LAB_HOSTS_FILE: a file with one ip per line}"
user=${LAB_USER:-$(id -un)}
tmpd=$(mktemp -d "${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/trivial-ssh.XXXXXX")
trap 'rm -rf "$tmpd"' EXIT

tac "$LAB_HOSTS_FILE" | while IFS= read -r ip; do printf '1/%s@%s\n' "$user" "$ip"; done >"$tmpd/login"

parallel -j1 --sshloginfile "$tmpd/login" "printf 'TRIVIAL-OK\\n'" ::: ''
