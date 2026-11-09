#!/usr/bin/env bash
# shellcheck disable=SC2317  # rows run in subshells
# Case table for the CPU count Ray is started with (cluster.sh
# _CLUSTER_CPUS_PROBE and _cluster_ray_cpus_flag). Ray counts the host's CPUs
# and ignores a cgroup CPU quota it cannot see (on the laptop's container lab
# it counted 16 under a 3-CPU cap), so every Ray start gets --num-cpus with
# the count the PC can really use: the affinity mask (cpuset, AllowedCPUs)
# lowered to the CPU quota of the PC's own cgroup or any ancestor, rounded
# down, never below 1. A --cap still wins. The probe reads PP_CG_ROOT,
# PP_PROC_CGROUP and PP_NPROC, so these rows feed it fixture trees.
#
#   case                                   nproc  cgroup files                       want
#   no quota anywhere (v2)                 8      cpu.max "max 100000"               8
#   v2 quota on an ancestor (docker --cpus 3) 16  /docker/x/cpu.max "300000 100000"  3
#   v2 fractional quota rounds down        8      "250000 100000"                    2
#   v2 quota below one CPU gives 1         8      "50000 100000"                     1
#   cpuset smaller than the quota          2      "400000 100000"                    2
#   v1 quota (cfs_quota_us/period)         16     cpu/docker/x quota 300000/100000   3
#   v1 no quota (-1)                       4      cpu quota -1                       4
#   v1 combined cpu,cpuacct mount          16     cpu,cpuacct/... 200000/100000      2
#   hybrid: v2 line and v1 line, min wins  16     v2 max, v1 300000/100000           3
#   unreadable cgroup file                 6      no files at all                    6
#   flag: --cap wins over the probe                                               --num-cpus=<cap>
#   flag: uncapped PC gets the probed count                                       --num-cpus=<probe>
#   flag: probe fails -> no flag, logged with the PC                              "" + log line
#   e19 joiner cores: hosts.tsv 16, probe 3                                       3
#   e19 joiner cores: hosts.tsv 4, probe 8 (ordinary: the smaller wins)           4
#   e19 joiner cores: probe fails -> hosts.tsv value, logged                      16 + log line
#   e19 ray join script: the starter carries --num-cpus=<cores>                   --num-cpus=3
set -euo pipefail
REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"
pass=0
failed=0
TMP="$(mktemp -d "$STATE/cluster-cpus-test.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
ok() { pass=$((pass + 1)); printf 'ok   %s\n' "$1"; }
bad() { failed=$((failed + 1)); printf 'FAIL %s -- %s\n' "$1" "$2"; }

# The probe text, exactly as cluster.sh holds it.
probe="$(LAB_RESULTS="$TMP/lab" bash -c "source '$REPO/scripts/campaign/lib.sh' >/dev/null 2>&1; printf '%s' \"\$LAB_CPUS_PROBE\"")"
if [[ -z "$probe" ]]; then
	bad "lib.sh defines LAB_CPUS_PROBE" "empty"
	printf 'PASS %s/%s\n' "$pass" "$((pass + failed))"
	exit 1
fi

# put <file> <content>
put() { mkdir -p "$(dirname "$1")"; printf '%s\n' "$2" >"$1"; }

# row <name> <nproc> <proc_cgroup text> <want> ; the tree is under $TMP/<name>
row() {
	local name="$1" n="$2" pcg="$3" want="$4" got root="$TMP/$1"
	mkdir -p "$root/cg"
	printf '%s\n' "$pcg" >"$root/proc_cgroup"
	got="$(PP_CG_ROOT="$root/cg" PP_PROC_CGROUP="$root/proc_cgroup" PP_NPROC="$n" sh -c "$probe" 2>"$root/err")" || got="rc=$?"
	if [[ "$got" == "$want" ]]; then ok "probe: $name -> $want"; else bad "probe: $name" "got [$got] want [$want] $(tr '\n' ' ' <"$root/err")"; fi
}

put "$TMP/v2none/cg/user.slice/cpu.max" "max 100000"
row v2none 8 "0::/user.slice/user-1000.slice/session-1.scope" 8

put "$TMP/v2anc/cg/docker/x/cpu.max" "300000 100000"
put "$TMP/v2anc/cg/docker/x/user.slice/cpu.max" "max 100000"
row v2anc 16 "0::/docker/x/user.slice/user-1000.slice/session-1.scope" 3

put "$TMP/v2frac/cg/a/cpu.max" "250000 100000"
row v2frac 8 "0::/a/b" 2

put "$TMP/v2tiny/cg/a/cpu.max" "50000 100000"
row v2tiny 8 "0::/a" 1

put "$TMP/v2set/cg/a/cpu.max" "400000 100000"
row v2set 2 "0::/a" 2

put "$TMP/v1q/cg/cpu/docker/x/cpu.cfs_quota_us" "300000"
put "$TMP/v1q/cg/cpu/docker/x/cpu.cfs_period_us" "100000"
row v1q 16 "4:memory:/docker/x/user.slice
1:cpu:/docker/x/user.slice" 3

put "$TMP/v1none/cg/cpu/docker/x/cpu.cfs_quota_us" "-1"
put "$TMP/v1none/cg/cpu/docker/x/cpu.cfs_period_us" "100000"
row v1none 4 "1:cpu:/docker/x" 4

put "$TMP/v1comb/cg/cpu,cpuacct/lab/cpu.cfs_quota_us" "200000"
put "$TMP/v1comb/cg/cpu,cpuacct/lab/cpu.cfs_period_us" "100000"
row v1comb 16 "3:cpu,cpuacct:/lab/user.slice" 2

put "$TMP/hyb/cg/unified/x/cpu.max" "max 100000"
put "$TMP/hyb/cg/cpu/docker/x/cpu.cfs_quota_us" "300000"
put "$TMP/hyb/cg/cpu/docker/x/cpu.cfs_period_us" "100000"
row hyb 16 "1:cpu:/docker/x/user.slice
0::/x" 3

row nofiles 6 "0::/nowhere/at/all" 6

# --- the flag, on the fake lab ------------------------------------------------
# shellcheck source=/dev/null
source "$REPO/scripts/campaign/test/fakelab.sh"
flag_row() { # <name> <want flag> <want log regex or -> [env...]
	local name="$1" want="$2" logre="$3" d out
	shift 3
	d="$TMP/flag-${name//[^a-z]/_}"
	out="$(
		fakelab_init "$d" 2 >/dev/null 2>&1 || exit 90
		export LAB_RESULTS="$d" "$@"
		# shellcheck source=/dev/null
		source "$REPO/scripts/campaign/cluster.sh"
		CLUSTER_CAP_PC=()
		CLUSTER_CAP_CPU=()
		if [[ -n "${T_CAP:-}" ]]; then CLUSTER_CAP_PC=(pc00); CLUSTER_CAP_CPU=("$T_CAP"); fi
		_cluster_ray_cpus_flag pc00 2>/dev/null
	)" || out="rc=$?"
	if [[ "$out" != "$want" ]]; then
		bad "flag: $name" "got [$out] want [$want]"
	elif [[ "$logre" != - ]] && ! grep -qE "$logre" "$d/campaign.log" 2>/dev/null; then
		bad "flag: $name" "no log line /$logre/: $(tail -2 "$d/campaign.log" 2>/dev/null | tr '\n' ' ')"
	else
		ok "flag: $name -> [$want]"
	fi
}
mkdir -p "$TMP/fcg/cg/a"
printf '300000 100000\n' >"$TMP/fcg/cg/a/cpu.max"
printf '0::/a\n' >"$TMP/fcg/proc_cgroup"
flag_row "cap wins over the probe" " --num-cpus=2" - T_CAP=2 PP_CG_ROOT="$TMP/fcg/cg" PP_PROC_CGROUP="$TMP/fcg/proc_cgroup" PP_NPROC=16
flag_row "uncapped PC gets the probed count" " --num-cpus=3" "ray on pc00: --num-cpus=3" PP_CG_ROOT="$TMP/fcg/cg" PP_PROC_CGROUP="$TMP/fcg/proc_cgroup" PP_NPROC=16
flag_row "probe fails: no flag, logged with the PC" "" "pc00: could not read its usable CPU count" FAKELAB_FAIL=pc00 LAB_RETRY_SLEEP=0

# --- e19's joiner -------------------------------------------------------------
e19_row() { # <name> <hosts.tsv cores> <want> <want log regex or -> [env...]
	local name="$1" hc="$2" want="$3" logre="$4" d out
	shift 4
	d="$TMP/e19-${name//[^a-z0-9]/_}"
	out="$(
		fakelab_init "$d" 2 >/dev/null 2>&1 || exit 90
		export LAB_RESULTS="$d" "$@"
		awk -F'\t' -v c="$hc" 'BEGIN{OFS="\t"} /^#/ {print; next} {$5 = c; print}' "$d/hosts.tsv" >"$d/h" && mv "$d/h" "$d/hosts.tsv"
		# shellcheck source=/dev/null
		source "$REPO/scripts/campaign/e19.sh" >/dev/null 2>&1
		e19_joiner_cores pc01 2>/dev/null
	)" || out="rc=$?"
	if [[ "$out" != "$want" ]]; then
		bad "e19 joiner cores: $name" "got [$out] want [$want]"
	elif [[ "$logre" != - ]] && ! grep -qE "$logre" "$d/campaign.log" 2>/dev/null; then
		bad "e19 joiner cores: $name" "no log line /$logre/"
	else
		ok "e19 joiner cores: $name -> $want"
	fi
}
e19_row "hosts.tsv 16, probe 3" 16 3 - PP_CG_ROOT="$TMP/fcg/cg" PP_PROC_CGROUP="$TMP/fcg/proc_cgroup" PP_NPROC=16
e19_row "hosts.tsv 4, probe 8: the smaller wins" 4 4 - PP_CG_ROOT="$TMP/nofiles/cg" PP_PROC_CGROUP="$TMP/nofiles/proc_cgroup" PP_NPROC=8
e19_row "probe fails: hosts.tsv value, logged" 16 16 "pc01: could not read its usable CPU count" FAKELAB_FAIL=pc01 LAB_RETRY_SLEEP=0

out="$(
	export LAB_RESULTS="$TMP/e19script"
	# shellcheck source=/dev/null
	source "$REPO/scripts/campaign/e19.sh" >/dev/null 2>&1
	e19_join_script ray E19-x 10.0.0.2 10.0.0.17 3 CODE
)"
starter="$(printf '%s\n' "$out" | awk '/base64 -d/ {print $NF}' | base64 -d 2>/dev/null || true)"
if [[ "$starter" == *"ray start --address=10.0.0.2:6379 --num-cpus=3 --block"* ]]; then
	ok "e19 ray join script: the starter carries --num-cpus=3"
else
	bad "e19 ray join script: the starter carries --num-cpus=3" "starter [$starter]"
fi

printf 'PASS %s/%s\n' "$pass" "$((pass + failed))"
((failed == 0))
