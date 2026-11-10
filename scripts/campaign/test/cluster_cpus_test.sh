#!/usr/bin/env bash
# shellcheck disable=SC2317  # rows run in subshells
# shellcheck disable=SC2034  # the cap arrays are read by the sourced cluster.sh
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
#
# Memory for Ray (_cluster_ray_mem_check <pc> head|worker; lib.sh's
# LAB_MEM_PROBE: MemTotal, lowered to a cgroup memory limit on the PC's own
# cgroup or an ancestor). Emulated lab (one VM, cgroup v1): the pc00
# container peaked at 1.24 GB RSS and 3.0-3.3 GB with page cache while Ray
# ran. Refuse under CLUSTER_RAY_HEAD_MIN_MIB (2048) / CLUSTER_RAY_WORKER_MIN_MIB
# (1024), warn under CLUSTER_RAY_HEAD_WARN_MIB (4096) / ..._WORKER_WARN_MIB (2048):
#   head 1024 MiB                     rc 1, RAY-MEMORY pc00 ... refused
#   head 3072 MiB                     rc 0, warning logged
#   head 8192 MiB (ordinary)          rc 0, nothing logged
#   worker 900 MiB                    rc 1
#   worker 1536 MiB                   rc 0, warning
#   16 GiB MemTotal, 1.5 GiB cgroup limit on an ancestor, head   rc 1 (the limit counts)
#   probe fails                       rc 0, "memory unknown" logged with the PC
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
		export LAB_RESULTS="$d"
		local kv
		for kv in "$@"; do export "${kv?}"; done
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
flag_row "probe fails: the ssh error is in the log, not just []" "" "pc00: could not read its usable CPU count .*fakelab ssh: pc00 is down" FAKELAB_FAIL=pc00 LAB_RETRY_SLEEP=0
flag_row "opposite: a working probe still logs the count" " --num-cpus=8" "ray on pc00: --num-cpus=8" PP_NPROC=8 LAB_RETRY_SLEEP=0

# --- e19's joiner -------------------------------------------------------------
e19_row() { # <name> <hosts.tsv cores> <want> <want log regex or -> [env...]
	local name="$1" hc="$2" want="$3" logre="$4" d out
	shift 4
	d="$TMP/e19-${name//[^a-z0-9]/_}"
	out="$(
		fakelab_init "$d" 2 >/dev/null 2>&1 || exit 90
		export LAB_RESULTS="$d"
		local kv
		for kv in "$@"; do export "${kv?}"; done
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

# e19's joiner Ray start carries the memory flags from the joiner's own probe
d="$TMP/e19mem"
out="$(
	fakelab_init "$d" 2 >/dev/null 2>&1 || exit 90
	mkdir -p "$d/nocg"
	export LAB_RESULTS="$d" PP_CG_ROOT="$d/nocg" PP_PROC_CGROUP=/dev/null PP_MEMTOTAL_KIB=4194304
	# shellcheck source=/dev/null
	source "$REPO/scripts/campaign/e19.sh" >/dev/null 2>&1
	e19_join_script ray E19-x 10.0.0.2 10.0.0.17 3 CODE pc01
)"
starter="$(printf '%s\n' "$out" | awk '/base64 -d/ {print $NF}' | base64 -d 2>/dev/null || true)"
if [[ "$starter" == *"--num-cpus=3 --memory=4294967296 --object-store-memory=1288490188 --block"* ]]; then
	ok "e19 ray join script: the starter carries --memory and --object-store-memory"
else
	bad "e19 ray join script: memory flags" "starter [$starter]"
fi

# --- memory for Ray -------------------------------------------------------------
mem_row() { # <name> <pc> <role> <want rc> <log regex or - for none expected> [env...]
	local name="$1" pc="$2" role="$3" want="$4" logre="$5" d rc
	shift 5
	d="$TMP/mem-${name//[^a-z0-9]/_}"
	rc="$(
		fakelab_init "$d" 2 >/dev/null 2>&1 || exit 90
		mkdir -p "$d/nocg"
		export LAB_RESULTS="$d" PP_CG_ROOT="$d/nocg" PP_PROC_CGROUP=/dev/null
		local kv
		for kv in "$@"; do export "${kv?}"; done
		# shellcheck source=/dev/null
		source "$REPO/scripts/campaign/cluster.sh"
		r=0
		_cluster_ray_mem_check "$pc" "$role" >/dev/null 2>&1 || r=$?
		echo "$r"
	)" || rc="sub=$?"
	if [[ "$rc" != "$want" ]]; then
		bad "ray memory: $name" "rc [$rc] want [$want]; log: $(tail -2 "$d/campaign.log" 2>/dev/null | tr '\n' ' ')"
	elif [[ "$logre" == - ]] && grep -q 'RAY-MEMORY' "$d/campaign.log" 2>/dev/null; then
		bad "ray memory: $name" "unexpected log: $(grep RAY-MEMORY "$d/campaign.log")"
	elif [[ "$logre" != - ]] && ! grep -qE "$logre" "$d/campaign.log" 2>/dev/null; then
		bad "ray memory: $name" "no log /$logre/: $(tail -2 "$d/campaign.log" 2>/dev/null | tr '\n' ' ')"
	else
		ok "ray memory: $name -> rc $want"
	fi
}
mem_row "head 1024 MiB refused" pc00 head 1 'RAY-MEMORY pc00 have=1024 MiB.*refused' PP_MEMTOTAL_KIB=1048576
mem_row "head 3072 MiB warned" pc00 head 0 'RAY-MEMORY pc00 have=3072 MiB.*warning' PP_MEMTOTAL_KIB=3145728
mem_row "head 8192 MiB fine" pc00 head 0 - PP_MEMTOTAL_KIB=8388608
mem_row "worker 900 MiB refused" pc01 worker 1 'RAY-MEMORY pc01 have=900 MiB.*refused' PP_MEMTOTAL_KIB=921600
mem_row "a nominal 2 GB head reports 1900 MiB: not refused" pc00 head 0 'RAY-MEMORY pc00 have=1900 MiB.*warning' PP_MEMTOTAL_KIB=1945600
mem_row "opposite: a real 1.75 GB head is still refused" pc00 head 1 'RAY-MEMORY pc00 have=1792 MiB.*refused' PP_MEMTOTAL_KIB=1835008
mem_row "a nominal 4 GB head reports 3900 MiB: no warning" pc00 head 0 - PP_MEMTOTAL_KIB=3993600
mem_row "opposite: a 3 GB head (3000 MiB) is still warned" pc00 head 0 'RAY-MEMORY pc00 have=3000 MiB.*warning' PP_MEMTOTAL_KIB=3072000
mem_row "worker 1536 MiB warned" pc01 worker 0 'RAY-MEMORY pc01 have=1536 MiB.*warning' PP_MEMTOTAL_KIB=1572864
mkdir -p "$TMP/memcg/cg/docker/x"
printf '1610612736\n' >"$TMP/memcg/cg/docker/x/memory.max"
printf '0::/docker/x/user.slice\n' >"$TMP/memcg/proc_cgroup"
mem_row "a cgroup limit under MemTotal counts" pc00 head 1 'RAY-MEMORY pc00 have=1536 MiB' PP_MEMTOTAL_KIB=16777216 PP_CG_ROOT="$TMP/memcg/cg" PP_PROC_CGROUP="$TMP/memcg/proc_cgroup"
mem_row "probe fails: memory unknown, not refused" pc01 worker 0 'pc01: memory unknown' FAKELAB_FAIL=pc01 LAB_RETRY_SLEEP=0
mem_row "probe fails: the ssh error is in the log, not just []" pc01 worker 0 'pc01: memory unknown .*fakelab ssh: pc01 is down' FAKELAB_FAIL=pc01 LAB_RETRY_SLEEP=0
mem_row "opposite: a memory probe that answers is still used" pc01 worker 0 'RAY-MEMORY pc01 have=1536 MiB.*warning' PP_MEMTOTAL_KIB=1572864
mkdir -p "$TMP/memcg4/cg/docker/x"
printf '4294967296\n' >"$TMP/memcg4/cg/docker/x/memory.max"
printf '0::/docker/x/user.slice\n' >"$TMP/memcg4/proc_cgroup"

# --- the memory flags for Ray ---------------------------------------------------
mflag_row() { # <name> <want flag> <want log regex or -> [env...]
	local name="$1" want="$2" logre="$3" d out
	shift 3
	d="$TMP/mflag-${name//[^a-z]/_}"
	out="$(
		fakelab_init "$d" 2 >/dev/null 2>&1 || exit 90
		mkdir -p "$d/nocg"
		export LAB_RESULTS="$d" PP_CG_ROOT="$d/nocg" PP_PROC_CGROUP=/dev/null
		local kv
		for kv in "$@"; do export "${kv?}"; done
		# shellcheck source=/dev/null
		source "$REPO/scripts/campaign/cluster.sh"
		_cluster_ray_mem_flag pc00 2>/dev/null
	)" || out="rc=$?"
	if [[ "$out" != "$want" ]]; then
		bad "mem flag: $name" "got [$out] want [$want]"
	elif [[ "$logre" != - ]] && ! grep -qE "$logre" "$d/campaign.log" 2>/dev/null; then
		bad "mem flag: $name" "no log /$logre/"
	else
		ok "mem flag: $name -> [$want]"
	fi
}
# 4 GiB = 4294967296 bytes, 30% = 1288490188; 16 GiB = 17179869184, 30% = 5153960755
mflag_row "4 GiB cgroup on a 16 GiB host" " --memory=4294967296 --object-store-memory=1288490188" "ray on pc00: --memory=4294967296" \
	PP_MEMTOTAL_KIB=16777216 PP_CG_ROOT="$TMP/memcg4/cg" PP_PROC_CGROUP="$TMP/memcg4/proc_cgroup"
mflag_row "no cgroup limit: MemTotal" " --memory=17179869184 --object-store-memory=5153960755" - PP_MEMTOTAL_KIB=16777216
mflag_row "probe fails: no flag, logged" "" "ray on pc00: no usable memory figure" FAKELAB_FAIL=pc00 LAB_RETRY_SLEEP=0
mflag_row "explicit object store wins" " --memory=17179869184 --object-store-memory=1000" - PP_MEMTOTAL_KIB=16777216 CLUSTER_RAY_OBJECT_STORE_BYTES=1000

printf 'PASS %s/%s\n' "$pass" "$((pass + failed))"
((failed == 0))
