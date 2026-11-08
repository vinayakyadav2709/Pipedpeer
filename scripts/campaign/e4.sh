#!/usr/bin/env bash
# e4.sh (brief S12): uneven machines. The first four PCs of hosts.tsv are
# capped to 8/4/2/1 CPUs, then W1 runs on pipedpeer, ray, dask and pipedpeer
# with the headcount split. Writes analysis-input/e4.csv:
#   system,repeat,wall_s,result_ok,share_pc00..share_pc03
# A share is the part of the W1 items one PC computed, read from the run's
# receipt (by_where); it is empty for ray/dask and when the receipt is
# missing (the reason is logged). A run without a record gets no row.
#
#   e4.sh [--dry-run] [--repeats k] [--systems a,b] [-h]
#
# Every exit path, Ctrl-C and SIGTERM included, runs `cluster.sh down`
# (caps reset) and puts a daemon that was switched to the headcount split
# back to the measured split.
# shellcheck disable=SC2153  # CLUSTER_SH, METRICS_SH, UPLOAD_SH come from run.sh
set -euo pipefail
D="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
. "$D/lib.sh"
# shellcheck source=/dev/null
. "$D/run.sh"
# shellcheck source=/dev/null
. "$D/cluster.sh"

usage() { echo "usage: e4.sh [--dry-run] [--repeats k] [--systems a,b] [-h]"; }
DRY=0
REPEATS=5
cap_n=(8 4 2 1)
sys_list=(pipedpeer ray dask pipedpeer-headcount)

while (($#)); do
  case "$1" in
    --dry-run) DRY=1 ;;
    --repeats) REPEATS="${2:?--repeats needs a number}"; shift ;;
    --systems) IFS=',' read -r -a sys_list <<<"${2:?--systems needs a list}"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; echo "e4.sh: unknown arg $1" >&2; exit 2 ;;
  esac; shift
done

mkdir -p "$LAB/analysis-input"

if ((DRY)); then
  for i in "${!cap_n[@]}"; do echo "CAP pc0$i=${cap_n[$i]}"; done
  echo "cluster.sh up 4 --cap pc00=8 --cap pc01=4 --cap pc02=2 --cap pc03=1"
  for s in "${sys_list[@]}"; do
    for ((r = 1; r <= REPEATS; r++)); do echo "RUN E4-W1-$s-n4-r$r"; done
  done
  echo "E4-DRY-RUN $((${#sys_list[@]} * REPEATS)) runs"
  exit 0
fi

# The first four PCs of the lab, each with at least as many CPUs as its cap.
hfile="$(lab_hosts 4)" || { echo "E4-NEEDS 4 PCs in hosts.tsv"; exit 1; }
mapfile -t ids <"$hfile"
for i in "${!ids[@]}"; do
  have="$(awk -F'\t' -v id="${ids[$i]}" '$1 == id {print $5; exit}' "$LAB/hosts.tsv")"
  if [[ ! "$have" =~ ^[0-9]+$ ]] || ((have < cap_n[i])); then
    echo "E4-NEEDS ${ids[$i]} with ${cap_n[$i]}+ CPUs (hosts.tsv says '${have:-none}')"
    lab_log "E4-NEEDS ${ids[$i]} with ${cap_n[$i]}+ CPUs (hosts.tsv says '${have:-none}')"
    exit 1
  fi
done
cap_sum=0
cluster_args=()
for i in "${!ids[@]}"; do
  cluster_args+=(--cap "${ids[$i]}=${cap_n[$i]}")
  cap_sum=$((cap_sum + cap_n[i]))
done

pc0="${ids[0]}"
CLUSTER_UP=0 HC_SET=0 METRICS_ON=0

hc_restore() { # the measured split again: service environment and daemon
  lab_ssh "$pc0" "systemctl --user unset-environment PIPEDPEER_POOL_SPLIT; env -u PIPEDPEER_POOL_SPLIT pipedpeer start --introducer-only=false" ||
    lab_log "E4: could not put the measured pool split back on $pc0; restart its daemon by hand"
  HC_SET=0
}

cleanup() {
  local rc=$?
  trap - EXIT
  ((HC_SET)) && hc_restore
  if ((METRICS_ON)); then
    "$METRICS_SH" stop E4-n4 "$hfile" || lab_log "E4: metrics stop failed"
  fi
  if ((CLUSTER_UP)); then
    "$CLUSTER_SH" down || lab_log "E4: cluster down failed; caps may still be on (see caps.txt)"
  fi
  "$UPLOAD_SH" || lab_log "E4: upload failed; results stay in $LAB"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

_load_sizes || exit 1

CLUSTER_UP=1
"$CLUSTER_SH" up 4 "${cluster_args[@]}" || { lab_log "E4: cluster up failed"; exit 1; }

# The cores each system sees must be the sum of the caps.
cores_file="$LAB/logs/cluster-4/cores.txt"
bad=0
if [[ ! -s "$cores_file" ]]; then
  echo "E4 caps not followed: no $cores_file" >>"$LAB/BLOCKERS.md"
  bad=1
else
  while IFS='=' read -r csys v; do
    if [[ "$v" != "$cap_sum" ]]; then
      echo "E4 caps not followed: $csys sees $v cores, caps sum to $cap_sum" >>"$LAB/BLOCKERS.md"
      lab_log "E4 caps not followed: $csys sees $v cores, expected $cap_sum"
      bad=1
    fi
  done <"$cores_file"
fi
if ((bad)); then exit 1; fi

if "$METRICS_SH" start E4-n4 "$hfile"; then
  METRICS_ON=1
else
  lab_log "E4: metrics start failed; runs go on without host metrics"
fi

out="$LAB/analysis-input/e4.csv"
printf 'system,repeat,wall_s,result_ok,share_pc00,share_pc01,share_pc02,share_pc03\n' >"$out"

# e4_row <system> <repeat>: one CSV row from the run's runs.jsonl record.
e4_row() {
  local s="$1" r="$2" id="E4-W1-$1-n4-r$2" rec line
  rec="$(grep -F -- "\"run_id\":\"$id\"" "$LAB/runs.jsonl" 2>/dev/null | tail -1 || true)"
  if [[ -z "$rec" ]]; then
    lab_log "E4 $id: no record in runs.jsonl; no CSV row"
    return 0
  fi
  if ! line="$(E4_REC="$rec" E4_HOSTS="$LAB/hosts.tsv" E4_IDS="${ids[*]}" E4_SYS="$s" python3 - <<'PY'
import json, os, sys
d = json.loads(os.environ["E4_REC"])
ids = os.environ["E4_IDS"].split()
wall = d.get("wall_s")
if not wall:
    sys.stderr.write("wall_s unknown (no start/end stamp)\n")
    sys.exit(3)
ok = 1 if d.get("result_ok") is True else 0
shares = [""] * len(ids)
if os.environ["E4_SYS"].startswith("pipedpeer"):
    rc = d.get("receipt")
    bw = rc.get("by_where") if isinstance(rc, dict) else None
    if not isinstance(bw, dict) or not bw:
        sys.stderr.write("no receipt by_where in the record\n")
    else:
        ip2pc = {}
        for l in open(os.environ["E4_HOSTS"]):
            f = l.rstrip("\n").split("\t")
            if len(f) > 1 and not l.startswith("#"):
                ip2pc[f[1]] = f[0]
        total = sum(bw.values())
        per = {}
        bad = []
        for k, v in bw.items():
            if k == "local":
                pc = ids[0]
            elif k.startswith("peer:"):
                pc = ip2pc.get(k.split(":")[1])
            else:
                pc = None
            if pc in ids:
                per[pc] = per.get(pc, 0) + v
            else:
                bad.append(k)
        if bad or total <= 0:
            sys.stderr.write("receipt by_where has unmapped keys %s or no items\n" % bad)
        else:
            shares = [str(round(per.get(pc, 0) / total, 4)) for pc in ids]
print(",".join([str(wall), str(ok)] + shares))
PY
  )"; then
    lab_log "E4 $id: no CSV row (record unusable)"
    return 0
  fi
  if [[ "$s" == pipedpeer* && "$line" == *,,,* ]]; then
    lab_log "E4 $id: no receipt shares (no receipt by_where in the record); shares left empty"
  fi
  echo "$s,$r,$line" >>"$out"
}

# hc_lines: how many headcount plan lines the pc0 daemon has logged so far.
hc_lines() {
  lab_ssh "$pc0" "{ grep -F '[pool] plan (headcount)' \"\${PIPEDPEER_DAEMON_STATE_DIR:-\$HOME/.local/state/pipedpeer}/daemon.log\" 2>/dev/null; journalctl --user -u pipedpeer.service --no-pager 2>/dev/null | grep -F '[pool] plan (headcount)'; } | wc -l" |
    tail -1 | tr -d '[:space:]'
}

# hc_switch: daemon on pc0 restarted with PIPEDPEER_POOL_SPLIT=headcount; true
# when 4 daemons are healthy again.
hc_switch() {
  local st="$LAB/logs/e4-status.txt" got=0 waited=0 limit="${E4_JOIN_S:-120}"
  HC_SET=1
  lab_ssh "$pc0" "systemctl --user set-environment PIPEDPEER_POOL_SPLIT=headcount; PIPEDPEER_POOL_SPLIT=headcount pipedpeer start --introducer-only=false" ||
    { lab_log "E4: could not restart the $pc0 daemon with the headcount split"; return 1; }
  while :; do
    if lab_ssh "$pc0" pipedpeer status >"$st" 2>/dev/null; then
      got="$(status_healthy_count "$st" 2>/dev/null || echo 0)"
      ((got >= 4)) && return 0
    fi
    ((waited >= limit)) && break
    sleep "$(lab_poll_sleep)"
    waited=$((waited + $(lab_poll_sleep)))
  done
  lab_log "E4: only $got of 4 daemons healthy ${waited}s after the headcount restart"
  return 1
}

for s in "${sys_list[@]}"; do
  if [[ "$s" == pipedpeer-headcount ]]; then
    if ! hc_switch; then
      echo "pipedpeer-headcount unavailable (daemon restart or rejoin failed)" >>"$LAB/BLOCKERS.md"
      hc_restore
      continue
    fi
    before="$(hc_lines)"
    campaign_run_one E4 W1 "$s" 4 1 0 || lab_log "E4: probe run for $s did not finish cleanly"
    after="$before" waited=0
    while ((waited < ${E4_LOG_WAIT_S:-10})); do
      after="$(hc_lines)"
      [[ "$after" =~ ^[0-9]+$ && "$before" =~ ^[0-9]+$ ]] && ((after > before)) && break
      sleep 1
      waited=$((waited + 1))
    done
    if ! [[ "$after" =~ ^[0-9]+$ && "$before" =~ ^[0-9]+$ ]] || ((after <= before)); then
      lab_log "E4: no '[pool] plan (headcount)' line in the $pc0 daemon log after the probe run; arm unavailable"
      echo "pipedpeer-headcount unavailable (daemon log has no [pool] plan (headcount) line)" >>"$LAB/BLOCKERS.md"
      hc_restore
      continue
    fi
    e4_row "$s" 1
    for ((r = 2; r <= REPEATS; r++)); do
      campaign_run_one E4 W1 "$s" 4 "$r" 0 || lab_log "E4: run $s r$r did not finish cleanly"
      e4_row "$s" "$r"
    done
    hc_restore
    continue
  fi
  for ((r = 1; r <= REPEATS; r++)); do
    campaign_run_one E4 W1 "$s" 4 "$r" 0 || lab_log "E4: run $s r$r did not finish cleanly"
    e4_row "$s" "$r"
  done
done
