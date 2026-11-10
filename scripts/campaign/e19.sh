#!/usr/bin/env bash
# e19.sh: E19 elasticity campaign.
#
# Usage:
#   e19.sh [--n 16] [--add 16] [--repeats 3] [--systems pipedpeer,ray,dask] [--dry-run]
#
# Steps:
# 1. Bring up the cluster with N + ADD nodes once.
# 2. Before the baseline and before every repeat from the second on: stop the
#    daemon and workers on the joiners, on Pipedpeer move nodes.db (with its WAL
#    files) aside, and bring the cluster to exactly N. The head restart clears
#    any node the previous repeat left registered.
# 3. Baseline: campaign_run_one E19 EL $sys $N 0 0 with no join. Its own job
#    record on pc00 names the closure used by this run.
# 4. Join runs: per repeat, purge that closure and the closure caches on the
#    joiners and test the store on disk: gone means joiner_warm=0, still there
#    means 1, no answer means empty. Then start cpu_sampler.py, start the run and
#    at 30% of the baseline wall join on each joiner.
# 5. The join runs detached on the joiner and runs once only. Python imports and
#    the Dask client connect happen before t0, so join_s holds the join itself.
# 6. first_work_s comes from the streak of >= 5 samples with busy cores >= 50%.
# 7. Checksums across all runs at this size: majority wins, minority 0,
#    all differ -> empty + BLOCKERS.md line.
#
# Results survive an interrupted campaign: every finished row goes to
# $LAB/e19.rows, $LAB/analysis-input/e19.csv is regenerated from it after each
# repeat, and a rerun skips the repeats already recorded.
#
# Output $LAB/analysis-input/e19.csv:
#   system,kind,i,pc,join_s,first_work_s,wall_s,result_ok,seed_s,joiner_warm,join_status,purged_bytes,closure_bytes
# kind is baseline, join, or join_aborted for a repeat that gave no join rows.
# join_status is ok, partial (exit 2, confirmed by the cluster), timeout, failed, no_result or no_cores on a join row, and
# why the repeat stopped on a join_aborted row: cluster_failed, no_baseline,
# run_not_seen, invite_failed, run_failed or interrupted. join_s is empty unless
# the status is ok or partial, so read medians together with the counts per status.

set -euo pipefail

REPO="${REPO:-$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)}"
CAMP="$REPO/scripts/campaign"
WORKLOADS="$REPO/docs/benchmarks/workloads"

# shellcheck source=/dev/null
source "$CAMP/lib.sh"
# shellcheck source=/dev/null
source "$CAMP/run.sh"

CLUSTER_SH="${CLUSTER_SH:-$CAMP/cluster.sh}"

E19_CSV_HEADER='system,kind,i,pc,join_s,first_work_s,wall_s,result_ok,seed_s,joiner_warm,join_status,purged_bytes,closure_bytes'
E19_ROWS_HEAD='e19-rows-v2 rid|sys|kind|i|pc|join_s|first_work_s|wall_s|seed_s|jwarm|status|csum|purged|closure'
E19_STORE_PATH_RE='^/nix/store/[a-z0-9]{32}-[A-Za-z0-9._+-]+$'

e19_usage() {
  echo "usage: e19.sh [--n 16] [--add 16] [--repeats 3] [--systems pipedpeer,ray,dask] [--dry-run] [-h]"
}

# e19_resolve_checksums <"run_id:csum ..."> [interim]
# Resolves majority checksum and outputs "run_id=1" (majority), "run_id=0" (minority),
# or "run_id=" (no majority/all differ), and logs / writes BLOCKERS.md when all differ.
# With "interim" (the CSV rendered after each repeat) it only prints the map.
e19_resolve_checksums() {
  local pairs="$1" mode="${2:-final}"
  python3 - "$pairs" "${LAB:-${LAB_RESULTS:-}}" "$mode" <<'PY'
import sys, os

pairs_str = sys.argv[1]
lab = sys.argv[2]
interim = sys.argv[3] == "interim"
data = []
for item in pairs_str.split():
    if ":" in item:
        rid, csum = item.split(":", 1)
        data.append((rid, csum))

if not data:
    sys.exit(0)

counts = {}
for rid, csum in data:
    if csum:
        counts[csum] = counts.get(csum, 0) + 1

total = sum(counts.values())
winner = None
if total > 0:
    top_csum, top_count = max(counts.items(), key=lambda kv: kv[1])
    if top_count > total / 2:
        winner = top_csum

if winner is not None:
    for rid, csum in data:
        if not csum:
            print(f"{rid}=0")
        else:
            ok = "1" if csum == winner else "0"
            print(f"{rid}={ok}")
            if ok == "0" and not interim:
                sys.stderr.write(f"e19: checksum mismatch {rid}: got {csum} want {winner}\n")
else:
    for rid, csum in data:
        print(f"{rid}=")
    msg = "e19: checksum mismatch, no majority across systems"
    if not interim:
        sys.stderr.write(f"{msg}\n")
        if lab:
            blockers = os.path.join(lab, "BLOCKERS.md")
            try:
                seen = ""
                if os.path.exists(blockers):
                    with open(blockers) as f:
                        seen = f.read()
                if msg not in seen.splitlines():
                    with open(blockers, "a") as f:
                        f.write(f"{msg}\n")
            except OSError as e:
                sys.stderr.write(f"e19: cannot write {blockers}: {e}\n")
PY
}

e19_running_now() {
  local id="$1" out
  out="$(lab_ssh pc00 'R=~/pipedpeer-lab/runs/'"$id"'; t=$(cat $R/t0 2>/dev/null); if [ -n "$t" ]; then if [ ! -e $R/exit ]; then echo "$t $(date +%s%N) live"; else echo "$t $(date +%s%N) done"; fi; fi' 2>/dev/null)" || out=""
  if [[ "$out" =~ ^([0-9]+)\ ([0-9]+)\ (live|done)$ ]]; then
    RUN_ELAPSED_NS=$((BASH_REMATCH[2] - BASH_REMATCH[1]))
    return 0
  fi
  return 1
}

e19_wait_running() {
  local id="$1" deadline=$((SECONDS + ${E19_START_WAIT_S:-120}))
  while ((SECONDS < deadline)); do
    e19_running_now "$id" && return 0
    sleep 0.2
  done
  lab_log "e19 $id: never seen running within ${E19_START_WAIT_S:-120}s"
  return 1
}

e19_is_number() {
  [[ "$1" =~ ^[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$ ]]
}

# e19_extract_run_info <run_id>: "wall|csum|start_ns|exit_code" from runs.jsonl,
# each field empty when unknown.
e19_extract_run_info() {
  local id="$1"
  python3 - "$LAB/runs.jsonl" "$id" <<'PY'
import sys, json, re
runs_path = sys.argv[1]
run_id = sys.argv[2]
wall_s = csum = start_ns = exit_code = ""
try:
    with open(runs_path) as f:
        for line in f:
            d = json.loads(line)
            if d.get("run_id") == run_id:
                w = d.get("wall_s")
                if w is not None:
                    wall_s = str(w)
                if d.get("start_ns") is not None:
                    start_ns = str(d["start_ns"])
                if d.get("exit_code") is not None:
                    exit_code = str(d["exit_code"])
                for ln in d.get("result_lines") or []:
                    m = re.search(r"checksum=([0-9.]+)", ln)
                    if m:
                        csum = m.group(1)
                        break
                break
except FileNotFoundError:
    pass
print(f"{wall_s}|{csum}|{start_ns}|{exit_code}")
PY
}

# e19_extract_seed_s <pc> <pc_ip>
e19_extract_seed_s() {
  local pc="$1" pc_ip="$2"
  local log_content secs=""
  log_content="$(lab_ssh pc00 '
    {
      cat "${PIPEDPEER_DAEMON_STATE_DIR:-$HOME/.local/state}/pipedpeer/daemon.log" 2>/dev/null || true
      journalctl --user -u pipedpeer.service --no-pager 2>/dev/null || true
    } | grep "peer took the closure" | tail -10
  ' 2>/dev/null || true)"
  if [[ -n "$log_content" ]]; then
    secs="$(python3 - "$log_content" "$pc_ip" "$pc" <<'PY'
import sys, re
content, pc_ip, pc = sys.argv[1], sys.argv[2], sys.argv[3]
matched_sec = ""
for line in content.splitlines():
    if pc_ip in line or pc in line:
        m = re.search(r'["\']secs["\']:\s*([0-9.]+)|secs=([0-9.]+)', line)
        if m:
            matched_sec = m.group(1) or m.group(2)
if matched_sec:
    print(matched_sec)
PY
)"
  fi
  echo "$secs"
}

# e19_join_poll_py: python run on the joiner, in the campaign venv, for Ray and
# Dask. Imports and the Dask client connect come first, then it records t0, runs
# the start command and polls the driver's view until this joiner's own node
# (Ray, with CPUs to schedule on) or all its workers (Dask) are listed. The Dask
# poll asks the scheduler for one number, never for the serialised worker list.
# args: system, driver ip, joiner ip, workers expected, timeout s, run dir,
# start command (base64). Writes join.stamp and join.res ("t0 t1 rc"), rc 0 when
# ready, 124 on timeout, 3 when the start command fails.
e19_join_poll_py() {
  cat <<'PY'
import base64, os, subprocess, sys, time

system, driver, me = sys.argv[1], sys.argv[2], sys.argv[3]
want, limit = int(sys.argv[4]), float(sys.argv[5])
rundir, starter = sys.argv[6], sys.argv[7]


def write_file(name, text):
    path = os.path.join(rundir, name)
    with open(path + ".tmp", "w") as f:
        f.write(text)
    os.replace(path + ".tmp", path)


def finish(t0, rc):
    write_file("join.res", "%d %d %d\n" % (t0, time.time_ns(), rc))
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(rc)


def own_workers(dask_scheduler, host):
    return sum(1 for a in dask_scheduler.workers if a.split("://")[-1].rsplit(":", 1)[0] == host)


try:
    if system == "ray":
        import ray
    else:
        from distributed import Client
except Exception as e:
    sys.stderr.write("join poll: import failed: %s\n" % e)
    finish(time.time_ns(), 1)

client = None
if system != "ray":
    try:
        client = Client(driver + ":8786", timeout=10)
    except Exception as e:
        sys.stderr.write("join poll: scheduler not reachable yet: %s\n" % e)


def count():
    global client
    if system == "ray":
        if not ray.is_initialized():
            ray.init(address=driver + ":6379", logging_level="ERROR", log_to_driver=False)
        return sum(
            1
            for n in ray.nodes()
            if n.get("Alive") and n.get("NodeManagerAddress") == me and (n.get("Resources") or {}).get("CPU", 0) > 0
        )
    if client is None:
        client = Client(driver + ":8786", timeout=10)
    return client.run_on_scheduler(own_workers, host=me)


t0 = time.time_ns()
write_file("join.stamp", "%d\n" % t0)
rc = subprocess.call(["bash", "-c", base64.b64decode(starter).decode()])
if rc != 0:
    sys.stderr.write("join poll: start command exited %d\n" % rc)
    finish(t0, 3)

deadline = time.time() + limit
last_error = None
while time.time() < deadline:
    try:
        got = count()
    except Exception as e:
        got = 0
        if str(e) != last_error:
            last_error = str(e)
            sys.stderr.write("join poll: %s\n" % e)
    if got >= want:
        finish(t0, 0)
    time.sleep(1.0)
sys.stderr.write("join poll: %s on %s not ready after %ss\n" % (system, me, limit))
finish(t0, 124)
PY
}

# e19_store_path_py: python run on pc00. The store path of this run's own job:
# the earliest job record with a store path started at or after the given
# nanosecond time (the baseline's start, less a second of slack).
e19_store_path_py() {
  cat <<'PY'
import datetime, glob, json, os, re, sys

cutoff = int(sys.argv[1]) / 1e9 - 1
base = os.environ.get("XDG_DATA_HOME") or os.path.expanduser("~/.local/share")
best = None
for path in glob.glob(os.path.join(base, "pipedpeer", "jobs", "*", "metadata.json")):
    try:
        with open(path) as f:
            job = json.load(f)
        store, started = job.get("store_path") or "", job.get("started_at") or ""
        if not store or not started:
            continue
        started = re.sub(r"(\.\d{6})\d+", r"\1", started).replace("Z", "+00:00")
        when = datetime.datetime.fromisoformat(started).timestamp()
    except (OSError, ValueError, AttributeError) as e:
        sys.stderr.write("e19: skipping job record %s: %s\n" % (path, e))
        continue
    if when >= cutoff and (best is None or when < best[0]):
        best = (when, store)
if best:
    print(best[1])
PY
}

# e19_valid_store_path <path>: a full store path with a hash and a name.
e19_valid_store_path() {
  [[ "$1" =~ $E19_STORE_PATH_RE ]]
}

# e19_closure_path <baseline start_ns>: the closure to purge from the joiners.
# E19_STORE_PATH is an operator override. Empty (logged) when not known, so
# nothing is purged and joiner_warm stays empty.
e19_closure_path() {
  local start_ns="$1" b64 path
  if [[ -n "${E19_STORE_PATH:-}" ]]; then
    if e19_valid_store_path "$E19_STORE_PATH"; then
      printf '%s' "$E19_STORE_PATH"
    else
      lab_log "e19: E19_STORE_PATH '$E19_STORE_PATH' is not a store path; closure not purged, joiner_warm left empty"
    fi
    return 0
  fi
  if [[ ! "$start_ns" =~ ^[0-9]+$ ]]; then
    lab_log "e19: baseline has no start time; closure not purged, joiner_warm left empty"
    return 0
  fi
  b64="$(e19_store_path_py | base64 -w0)"
  path="$(lab_ssh pc00 "echo $b64 | base64 -d | python3 - $start_ns")" || {
    lab_log "e19: cannot read job records on pc00 (exit $?); closure not purged, joiner_warm left empty"
    return 0
  }
  if [[ -z "$path" ]]; then
    lab_log "e19: no job of this baseline on pc00 names a store path; closure not purged, joiner_warm left empty"
  elif e19_valid_store_path "$path"; then
    printf '%s' "$path"
  else
    lab_log "e19: job record store path '$path' is not a store path; closure not purged, joiner_warm left empty"
  fi
}

# e19_ssh_or_log <pc> <what> <command>: run on pc, log a failure, never fail.
e19_ssh_or_log() {
  local pc="$1" what="$2" cmd="$3"
  lab_ssh "$pc" "$cmd" >/dev/null 2>&1 || lab_log "e19: $what on $pc failed (ssh exit $?)"
}

# Joiners whose nodes.db went aside, so cleanup restores them.
declare -a E19_JOINERS_ASIDE=()
declare -A E19_ASIDE_SEEN=()

e19_cleanup() {
  local pc
  for pc in "${E19_JOINERS_ASIDE[@]}"; do
    lab_ssh "$pc" '
      db="${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer/nodes.db"
      [ -e "$db.aside" ] || exit 0
      pipedpeer stop 2>/dev/null || true
      systemctl --user stop pipedpeer.service 2>/dev/null || true
      rm -f "$db" "$db-wal" "$db-shm"
      mv -f "$db.aside" "$db" || exit 1
      for f in wal shm; do
        if [ -e "$db-$f.aside" ]; then mv -f "$db-$f.aside" "$db-$f" || exit 1; fi
      done
      pipedpeer start >/dev/null 2>&1
    ' >/dev/null 2>&1 || lab_log "e19: restoring nodes.db on $pc failed (ssh exit $?); see $pc:.local/share/pipedpeer/nodes.db.aside"
  done
}

# e19_on_signal <exit code>: stop background work, restore the joiners, exit.
e19_on_signal() {
  local code="$1" pids
  trap - EXIT INT TERM
  lab_log "e19: interrupted, exiting $code"
  pids="$(jobs -p)" || pids=""
  if [[ -n "$pids" ]]; then
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null || lab_log "e19: some background jobs had already ended"
  fi
  e19_cleanup
  exit "$code"
}

# e19_joiner_cores <pc>: its worker count, as cluster.sh computes for Dask:
# hosts.tsv cores capped by CLUSTER_CAP_CPU and by lab_usable_cpus. Empty
# (logged) when unknown.
e19_joiner_cores() {
  local pc="$1" cores usable
  cores="$(awk -F'\t' -v p="$pc" '$1 == p { print $5; exit }' "$LAB/hosts.tsv" 2>/dev/null || true)"
  if [[ ! "$cores" =~ ^[0-9]+$ ]] || ((cores < 1)); then
    lab_log "e19: no core count for $pc in hosts.tsv"
    return 0
  fi
  if [[ "${CLUSTER_CAP_CPU:-}" =~ ^[0-9]+$ ]] && ((CLUSTER_CAP_CPU >= 1 && CLUSTER_CAP_CPU < cores)); then
    cores="$CLUSTER_CAP_CPU"
  fi
  # what the joiner can really use (cpuset and cgroup quota); hosts.tsv is nproc
  if usable="$(lab_usable_cpus "$pc")"; then
    ((usable < cores)) && cores="$usable"
  else
    lab_log "e19: $pc's usable CPU count unknown; using hosts.tsv/cap value $cores"
  fi
  printf '%s' "$cores"
}

# e19_journal_add <row>: one finished row, kept across interrupted campaigns.
e19_journal_add() {
  printf '%s\n' "$1" >>"$E19_JOURNAL"
}

# e19_abort_repeat <run_id> <sys> <rep> <status> [wall]: count a repeat that gave
# no join rows, so it shows in the CSV and not as a gap.
e19_abort_repeat() {
  local id="$1" sys="$2" rep="$3" status="$4" wall="${5:-}"
  e19_journal_add "$id|$sys|join_aborted|$rep||||$wall|||$status|||"
}

# e19_recorded <run_id>: true when the journal holds a finished result for the
# run. A repeat stopped by a cluster failure or a missing baseline is retried.
e19_recorded() {
  local id="$1" line rid sys kind i pc js fw wall seed warm status csum pbytes cbytes
  while IFS= read -r line; do
    IFS='|' read -r rid sys kind i pc js fw wall seed warm status csum pbytes cbytes <<<"$line"
    [[ "$rid" == "$id" ]] || continue
    case "$kind/$status" in
      join_aborted/cluster_failed | join_aborted/no_baseline) continue ;;
    esac
    return 0
  done <"$E19_JOURNAL"
  return 1
}

# e19_init_outputs: set E19_CSV and E19_JOURNAL. A CSV or journal of another
# format is moved aside and logged, never mixed with new rows.
e19_init_outputs() {
  local first="" ts
  mkdir -p "$LAB/analysis-input"
  E19_CSV="$LAB/analysis-input/e19.csv"
  E19_JOURNAL="$LAB/e19.rows"
  ts="$(date +%Y%m%dT%H%M%S)"
  if [[ -f "$E19_JOURNAL" ]]; then
    IFS= read -r first <"$E19_JOURNAL" || first=""
    if [[ "$first" != "$E19_ROWS_HEAD" ]]; then
      mv -f "$E19_JOURNAL" "$E19_JOURNAL.old-$ts"
      lab_log "e19: $E19_JOURNAL has another format; moved to $E19_JOURNAL.old-$ts"
    fi
  fi
  if [[ -f "$E19_CSV" ]]; then
    first=""
    IFS= read -r first <"$E19_CSV" || first=""
    if [[ "$first" != "$E19_CSV_HEADER" || ! -f "$E19_JOURNAL" ]]; then
      mv -f "$E19_CSV" "$E19_CSV.old-$ts"
      lab_log "e19: $E19_CSV is from another format or run; moved to $E19_CSV.old-$ts"
    fi
  fi
  [[ -f "$E19_JOURNAL" ]] || printf '%s\n' "$E19_ROWS_HEAD" >"$E19_JOURNAL"
  [[ -f "$E19_CSV" ]] || printf '%s\n' "$E19_CSV_HEADER" >"$E19_CSV"
}

# e19_render_csv <interim|final>: rewrite the CSV from the journal: last row wins
# per run, kind and PC; a repeat that has join rows drops an earlier abort row.
e19_render_csv() {
  local mode="$1" line rid sys kind i pc js fw wall seed warm status csum pbytes cbytes key tmp res_map ok_val
  local -A last_line=() key_seen=() has_join=() ok_map=() pair_seen=()
  local -a keys=() pairs=()
  while IFS= read -r line; do
    [[ -n "$line" && "$line" != "$E19_ROWS_HEAD" ]] || continue
    IFS='|' read -r rid sys kind i pc js fw wall seed warm status csum pbytes cbytes <<<"$line"
    key="$rid|$kind|$pc"
    if [[ -z "${key_seen[$key]:-}" ]]; then
      keys+=("$key")
      key_seen["$key"]=1
    fi
    last_line["$key"]="$line"
    if [[ "$kind" == join ]]; then has_join["$rid"]=1; fi
    if [[ "$kind" != join_aborted && -z "${pair_seen[$rid]:-}" ]]; then
      pairs+=("$rid:$csum")
      pair_seen["$rid"]=1
    fi
  done <"$E19_JOURNAL"

  res_map="$(e19_resolve_checksums "${pairs[*]:-}" "$mode")" || {
    lab_log "e19: checksum resolution failed (exit $?); result_ok left empty"
    res_map=""
  }
  while IFS='=' read -r rid ok_val; do
    [[ -n "$rid" ]] || continue
    ok_map["$rid"]="$ok_val"
  done <<<"$res_map"

  tmp="$E19_CSV.tmp.$$"
  {
    printf '%s\n' "$E19_CSV_HEADER"
    for key in "${keys[@]}"; do
      IFS='|' read -r rid sys kind i pc js fw wall seed warm status csum pbytes cbytes <<<"${last_line[$key]}"
      if [[ "$kind" == join_aborted && -n "${has_join[$rid]:-}" ]]; then continue; fi
      printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$sys" "$kind" "$i" "$pc" "$js" "$fw" "$wall" "${ok_map[$rid]:-}" "$seed" "$warm" "$status" "$pbytes" "$cbytes"
    done
  } >"$tmp"
  mv -f "$tmp" "$E19_CSV"
}

# e19_joiner_out <sys> <pc>: stop the joiner's daemon and workers; on Pipedpeer
# move its database aside. An .aside already there is the original (left by an
# interrupted run): it stays, and the current files are removed. Side files go
# first, nodes.db last, so nodes.db.aside marks a complete move.
e19_joiner_out() {
  local sys="$1" pc="$2"
  e19_ssh_or_log "$pc" "stopping the joiner" "
    systemctl --user stop pipedpeer.service 2>/dev/null || true
    pipedpeer stop 2>/dev/null || true
    ~/pipedpeer-lab/venv/bin/ray stop --force 2>/dev/null || true
    systemctl --user stop lab-ray 2>/dev/null || true
    systemctl --user stop lab-dask-worker 2>/dev/null || true
  "
  if [[ "$sys" == pipedpeer* ]]; then
    if [[ -z "${E19_ASIDE_SEEN[$pc]:-}" ]]; then
      E19_ASIDE_SEEN["$pc"]=1
      E19_JOINERS_ASIDE+=("$pc")
    fi
    e19_ssh_or_log "$pc" "moving nodes.db aside" '
      db="${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer/nodes.db"
      if [ -e "$db.aside" ]; then
        rm -f "$db" "$db-wal" "$db-shm"
      else
        for f in wal shm; do
          if [ -e "$db-$f" ]; then mv -f "$db-$f" "$db-$f.aside"; fi
        done
        if [ -e "$db" ]; then mv -f "$db" "$db.aside"; fi
      fi
    '
  fi
}

# e19_prepare <sys>: joiners out, cluster at exactly N (uses joiner_hosts, N).
e19_prepare() {
  local sys="$1" jpc
  for jpc in "${joiner_hosts[@]}"; do
    e19_joiner_out "$sys" "$jpc"
  done
  "$CLUSTER_SH" up "$N" || {
    lab_log "e19: cluster up $N failed for $sys (exit $?)"
    return 1
  }
}

# e19_purge_script: the body run on a joiner (after sys=, rel= are set): remove
# the closure caches, then the closure's requisites with every store path that
# refers to them (the common environment shares python and its libraries),
# each --delete on its own and in passes so a refusal (a GC-rooted referrer)
# is logged and the rest still go. Prints NOTE lines, "BYTES <purged> <closure>" (nar bytes of the
# requisites present before the purge, and of those gone after), then
# present|absent for the top path.
e19_purge_script() {
  cat <<'EOS'
d="${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer"
rm -rf "$d/work/nar-cache" "$d/closure-cache" || echo "NOTE cache removal failed"
# The warm-up's GC root (our own link, kept by `pipedpeer run`) would hold the
# common environment and so the closure's shared paths: remove it first.
root="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/warmup-env"
if [ -L "$root" ] || [ -e "$root" ]; then
  if rm -f "$root"; then echo "NOTE removed the warm-up GC root $root"; else echo "NOTE could not remove the warm-up GC root $root"; fi
fi
purged=0; total=0
if [ -n "$rel" ]; then
  # private store: plain rm of the closure's top path
  had_private=0
  if [ -e "$d/nix/store/$rel" ]; then
    had_private=1
    sz="$(du -sb "$d/nix/store/$rel" 2>/dev/null | cut -f1)"
    case "$sz" in ''|*[!0-9]*) echo "NOTE private store size unknown for $rel"; sz=0 ;; esac
    total=$((total + sz))
    rm -rf "$d/nix/store/$rel" 2>&1 | sed 's/^/NOTE private store rm: /'
    [ -e "$d/nix/store/$rel" ] || purged=$((purged + sz))
  fi
  # system store (root-owned): requisites through the Nix daemon. A top path
  # an earlier repeat removed leaves its dependencies here: pc00's list
  # ($pc00reqs, base64) names them.
  pc00list="$(printf '%s' "${pc00reqs:-}" | base64 -d 2>/dev/null)"
  if [ -d "$sys" ] && command -v nix-store >/dev/null 2>&1 && [ ! -e "$sys/$rel" ] && [ -z "$pc00list" ] && [ "$had_private" = 0 ]; then echo NEED-PC00-REQS; fi
  if [ -e "$sys/$rel" ] || [ -n "$pc00list" ]; then
    if command -v nix-store >/dev/null 2>&1; then
      if [ -e "$sys/$rel" ]; then
        reqs="$(nix-store -qR "$sys/$rel" 2>&1)" || { echo "NOTE nix-store -qR failed, only the top path is deleted: $reqs"; reqs=; }
      else
        reqs="$pc00list"
        echo "NOTE top path $rel already gone here; requisites taken from pc00's closure"
      fi
      list="$sys/$rel"
      for p in $(printf '%s\n' "$reqs" | tac); do
        [ "$p" = "$sys/$rel" ] || list="$list $p"
      done
      # sizes first: the byte counts are of the closure as it was
      sizes=""
      for p in $list; do
        case "$p" in
          "$sys"/?*) ;;
          *) echo "NOTE skipped requisite outside the store: $p"; continue ;;
        esac
        [ -e "$p" ] || continue
        sz="$(nix-store -q --size "$p" 2>/dev/null)"
        case "$sz" in ''|*[!0-9]*) echo "NOTE size unknown for $p"; sz=0 ;; esac
        total=$((total + sz))
        sizes="$sizes $p:$sz"
      done
      # Paths outside the closure that refer into it go too: on a PC whose
      # install built the common environment, python and its libraries are
      # also referenced by that environment, and Nix refuses to delete a path
      # while a referrer exists. Referrers first, then the closure, dependents
      # first; passes repeat until nothing more goes (a GC-rooted referrer
      # keeps its part of the closure and is logged).
      # Only paths that exist here go to nix-store: a top path an earlier
      # repeat removed would otherwise end the whole query, and its referrers
      # would never be found.
      have=""
      for p in $list; do
        case "$p" in
          "$sys"/?*) [ -e "$p" ] && have="$have $p" ;;
        esac
      done
      extra=""
      if [ -n "$have" ]; then
        refs="$(nix-store -q --referrers-closure $have 2>&1)" || { echo "NOTE nix-store -q --referrers-closure failed, only the closure is deleted: $refs"; refs=; }
        for p in $refs; do
          case " $list " in *" $p "*) ;; *) case "$p" in "$sys"/?*) extra="$extra $p" ;; esac ;; esac
        done
      else
        echo "NOTE no closure path exists here; there are no referrers to delete"
      fi
      # Outside-closure paths may be deleted only on a PC whose Nix the campaign
      # installed (lab-pc.sh records "nix-campaign ..."). When Nix predated the
      # campaign, do not delete outside paths: log and let byte count decide warm.
      nix_campaign=0
      lab_state="${state:-${E19_LAB_STATE:-/var/lib/pipedpeer-lab/state}}"
      if [ -f "$lab_state" ] && grep -q '^nix-campaign ' "$lab_state" 2>/dev/null; then
        nix_campaign=1
      fi
      if [ "$nix_campaign" = 0 ]; then
        n_extra=0
        for p in $extra; do [ -e "$p" ] && n_extra=$((n_extra + 1)); done
        [ "$n_extra" = 0 ] || echo "NOTE Nix predates the campaign on ${pc:-unknown}: $n_extra outside path(s) kept"
        extra=""
      fi
      # These were not all created by the harness: name each one before it goes.
      for p in $extra; do
        [ -e "$p" ] && echo "NOTE outside closure, will delete: $p"
      done
      todo="$extra $list"
      pass=0
      while [ -n "$(echo $todo)" ] && [ "$pass" -lt 50 ]; do
        pass=$((pass + 1)); left=""; moved=0
        for p in $todo; do
          [ -e "$p" ] || continue
          nix-store --delete "$p" >/dev/null 2>&1 || true
          if [ -e "$p" ]; then left="$left $p"; else moved=1; fi
        done
        todo="$left"
        [ "$moved" = 1 ] || break
      done
      gone=0
      for p in $extra; do [ -e "$p" ] || gone=$((gone + 1)); done
      [ "$gone" = 0 ] || echo "NOTE deleted $gone path(s) outside the closure that referred to it (the common environment and build inputs)"
      for p in $todo; do
        case " $list " in *" $p "*) ;; *) continue ;; esac
        [ -e "$p" ] || continue
        o="$(nix-store --delete "$p" 2>&1)" || echo "NOTE nix-store --delete refused $p: $(printf '%s' "$o" | grep -m1 -iE 'error|cannot' || printf '%s' "$o" | tail -1)"
      done
      for e in $sizes; do
        p="${e%:*}"; sz="${e##*:}"
        [ -e "$p" ] || purged=$((purged + sz))
      done
    else
      echo 'NOTE nix-store not found; the system-store closure cannot be deleted'
    fi
  fi
  echo "BYTES $purged $total"
  if [ -e "$d/nix/store/$rel" ] || [ -e "$sys/$rel" ]; then echo present; else echo absent; fi
fi
EOS
}

# e19_purge_and_probe <pc> <closure path or ""> <run_id>: purge, then print
# "warm|purged_bytes|closure_bytes". warm is 0 when at least 90% of the closure
# bytes went, else 1; with no byte count it is what the disk says for the top
# path. Prints nothing when it cannot be told or no path is known.
e19_purge_and_probe() {
  local pc="$1" path="$2" id="$3" out rc=0 rel="" b64 note bytes="" purged="" total="" last warm=""
  local reqs_failed=0
  [[ -z "$path" ]] || rel="${path#/nix/store/}"
  b64="$(e19_purge_script | base64 -w0)"
  out="$(lab_ssh "$pc" "pc='$pc'; state='${E19_LAB_STATE:-/var/lib/pipedpeer-lab/state}'; sys='${E19_SYS_STORE:-/nix/store}'; rel='$rel'; eval \"\$(echo $b64 | base64 -d)\"" 2>&1)" || rc=$?
  # the joiner had lost the top path (an earlier repeat's purge): its
  # dependencies are named by pc00's list, where the closure is whole
  if ((rc == 0)) && grep -q '^NEED-PC00-REQS$' <<<"$out"; then
    local pc00reqs="" reqs_raw="" reqs_rc=0
    reqs_raw="$(lab_ssh pc00 "nix-store -qR '${E19_SYS_STORE:-/nix/store}/$rel'" 2>&1)" || reqs_rc=$?
    if ((reqs_rc != 0)); then
      reqs_failed=1
      lab_log "e19 $id: could not list the closure's requisites on pc00 (ssh exit $reqs_rc): ${reqs_raw:-(no output)}; $pc's leftover dependencies are not purged"
    else
      pc00reqs="$(printf '%s' "$reqs_raw" | base64 -w0)"
    fi
    if [[ -n "$pc00reqs" ]]; then
      out="$(lab_ssh "$pc" "pc='$pc'; state='${E19_LAB_STATE:-/var/lib/pipedpeer-lab/state}'; sys='${E19_SYS_STORE:-/nix/store}'; rel='$rel'; pc00reqs='$pc00reqs'; eval \"\$(echo $b64 | base64 -d)\"" 2>&1)" || rc=$?
    fi
  fi
  if ((rc != 0)); then
    lab_log "e19 $id: closure purge on $pc failed (ssh exit $rc): ${out:-(no output)}; joiner_warm left empty"
    return 0
  fi
  while IFS= read -r note; do
    [[ "$note" == NOTE\ * ]] && lab_log "e19 $id: purge on $pc: ${note#NOTE }"
  done <<<"$out"
  [[ -n "$path" ]] || return 0
  bytes="$(grep '^BYTES ' <<<"$out" | tail -1 || true)"
  last="$(grep -v '^NOTE \|^BYTES ' <<<"$out" | tail -1 || true)"
  if [[ "$bytes" =~ ^BYTES\ ([0-9]+)\ ([0-9]+)$ ]]; then
    purged="${BASH_REMATCH[1]}"
    total="${BASH_REMATCH[2]}"
  fi
  if ((reqs_failed)); then
    warm=1
  elif [[ -n "$total" ]] && ((total > 0)); then
    if ((purged * 100 >= total * 90)); then
      warm=0
    else
      warm=1
      lab_log "e19 $id: only $purged of $total closure bytes removed on $pc; flagged joiner_warm=1"
    fi
  else
    case "$last" in
      absent) warm=0 ;;
      present)
        warm=1
        lab_log "e19 $id: closure still on $pc after the purge; flagged joiner_warm=1"
        ;;
      *) lab_log "e19 $id: no answer from $pc on the closure ('$last'); joiner_warm left empty"
         return 0 ;;
    esac
  fi
  printf '%s|%s|%s' "$warm" "$purged" "$total"
}

# e19_join_script <sys> <id> <driver ip> <joiner ip> <cores> <invite code> [pc]:
# the bash run detached on the joiner.
e19_join_script() {
  local sys="$1" id="$2" driver="$3" jip="$4" cores="$5" code="$6" jpc_="${7:-}" wait_s="${E19_JOIN_WAIT_S:-180}"
  case "$sys" in
    pipedpeer*)
      cat <<EOF
R=~/pipedpeer-lab/runs/$id
t0=\$(date +%s%N); echo "\$t0" > "\$R/join.stamp"
timeout $wait_s pipedpeer connect $(printf '%q' "$code")
rc=\$?
t1=\$(date +%s%N)
echo "\$t0 \$t1 \$rc" > "\$R/join.res.tmp" && mv -f "\$R/join.res.tmp" "\$R/join.res"
EOF
      ;;
    ray* | dask*)
      local pollsys="ray" poll_b64 starter want=1
      [[ "$sys" == dask* ]] && pollsys="dask"
      poll_b64="$(e19_join_poll_py | base64 -w0)"
      if [[ "$pollsys" == ray ]]; then
        starter="systemd-run --user --unit=lab-ray --collect ~/pipedpeer-lab/venv/bin/ray start --address=$driver:6379${cores:+ --num-cpus=$cores}${jpc_:+$(_cluster_ray_mem_flag "$jpc_")} --block"
      else
        starter="systemd-run --user --unit=lab-dask-worker --collect ~/pipedpeer-lab/venv/bin/dask worker tcp://$driver:8786 --nworkers $cores --nthreads 1"
        want="$cores"
      fi
      cat <<EOF
R=~/pipedpeer-lab/runs/$id
echo $poll_b64 | base64 -d | ~/pipedpeer-lab/venv/bin/python - $pollsys $driver $jip $want $wait_s "\$R" $(printf '%s' "$starter" | base64 -w0)
rc=\$?
if [ ! -e "\$R/join.res" ]; then
  echo "join poll: python exited \$rc before writing a result" >&2
  t=\$(date +%s%N); echo "\$t \$t 1" > "\$R/join.res.tmp" && mv -f "\$R/join.res.tmp" "\$R/join.res"
fi
EOF
      ;;
  esac
}

# e19_confirm_joined <joiners already in>: succeed when the driver's own
# `pipedpeer status` lists at least N + that + 1 healthy nodes (polled up to
# E19_CONFIRM_WAIT_S seconds).
e19_confirm_joined() {
  local want=$((N + $1 + 1)) end=$((SECONDS + ${E19_CONFIRM_WAIT_S:-20})) tab healthy=0
  while :; do
    tab="$(lab_ssh pc00 'pipedpeer status' 2>&1)" || tab=""
    healthy="$(awk '/^NAME[ \t]+STATE/{t=1;next} t&&NF==0{exit} t&&$2=="healthy"{c++} END{print c+0}' <<<"$tab")"
    if ((healthy >= want)); then return 0; fi
    ((SECONDS < end)) || break
    sleep 2
  done
  lab_log "e19: driver lists $healthy healthy nodes, want $want to confirm the partial join"
  return 1
}

# e19_await_join_res <pc> <run_id> <deadline in SECONDS>: print join.res once it
# exists, or nothing at the deadline.
e19_await_join_res() {
  local pc="$1" id="$2" deadline="$3" out
  while :; do
    out="$(lab_ssh "$pc" "cat ~/pipedpeer-lab/runs/$id/join.res 2>/dev/null || true" 2>/dev/null)" || out=""
    if [[ -n "$out" ]]; then
      printf '%s' "$out"
      return 0
    fi
    ((SECONDS < deadline)) || return 0
    sleep "${E19_POLL_S:-1}"
  done
}

# e19_stop_sampler <pc> <run_id>
e19_stop_sampler() {
  local pc="$1" id="$2"
  e19_ssh_or_log "$pc" "stopping the sampler" "
    pid=\$(cat ~/pipedpeer-lab/runs/$id/sampler.pid 2>/dev/null || true)
    if [ -n \"\$pid\" ]; then
      kill \"\$pid\" 2>/dev/null || true
    fi
  "
}

# e19_one_repeat <sys> <rep> <run_id> <baseline wall> <closure path>: run the
# repeat, journal its rows. Uses joiner_hosts, N, E19_JOURNAL.
e19_one_repeat() {
  local sys="$1" rep="$2" run_id="$3" base_wall="$4" closure="$5" jpc jcores
  local -A warm_of=() status_of=() cores_of=() launched=()

  if [[ "$sys" == pipedpeer* ]]; then
    for jpc in "${joiner_hosts[@]}"; do
      warm_of["$jpc"]="$(e19_purge_and_probe "$jpc" "$closure" "$run_id")"
    done
  fi

  for jpc in "${joiner_hosts[@]}"; do
    jcores="$(e19_joiner_cores "$jpc")"
    cores_of["$jpc"]="$jcores"
    e19_ssh_or_log "$jpc" "starting the sampler" "
      R=~/pipedpeer-lab/runs/$run_id
      mkdir -p \"\$R\"
      rm -rf \"\$R/sampler.txt\" \"\$R/sampler.pid\" \"\$R/join.stamp\" \"\$R/join.res\" \"\$R/join.log\" \"\$R/join.once\"
      ${jcores:+export LAB_CORES=$jcores}
      python3 ~/pipedpeer-lab/work/cpu_sampler.py \"\$R/sampler.txt\" ${E19_TIMEOUT_S:-900} >/dev/null 2>&1 &
      echo \$! > \"\$R/sampler.pid\"
    "
  done

  campaign_run_one E19 EL "$sys" "$N" "$rep" 0 "${E19_TIMEOUT_S:-900}" strong &
  local run_pid=$!

  local aborted="" remain
  if e19_wait_running "$run_id"; then
    remain="$(python3 -c "print(max(0.0, 0.3 * $base_wall - $RUN_ELAPSED_NS / 1e9))")"
    sleep "$remain" &
    wait $! || lab_log "e19 $run_id: wait before the join ended early"
  else
    aborted="run_not_seen"
  fi

  local pp_code="" driver_ip
  driver_ip="$(lab_ip pc00)"
  if [[ -z "$aborted" && "$sys" == pipedpeer* ]]; then
    pp_code="$(lab_ssh pc00 'pipedpeer invite 2>/dev/null' | awk '{print $NF}' | tail -1 || true)"
    if [[ -z "$pp_code" ]]; then
      lab_log "e19 $run_id: pipedpeer invite on pc00 printed no code; repeat aborted, no join row"
      aborted="invite_failed"
    fi
  fi

  local join_started=$SECONDS
  if [[ -z "$aborted" ]]; then
    local -a launch_pids=() launch_hosts=()
    local script b64 launch i
    for jpc in "${joiner_hosts[@]}"; do
      if [[ "$sys" == dask* && -z "${cores_of[$jpc]}" ]]; then
        status_of["$jpc"]="no_cores"
        continue
      fi
      script="$(e19_join_script "$sys" "$run_id" "$driver_ip" "$(lab_ip "$jpc")" "${cores_of[$jpc]}" "$pp_code" "$jpc")"
      b64="$(printf '%s' "$script" | base64 -w0)"
      launch="R=~/pipedpeer-lab/runs/$run_id; mkdir \"\$R/join.once\" 2>/dev/null || exit 0; setsid nohup bash -c \"\$(echo $b64 | base64 -d)\" >\"\$R/join.log\" 2>&1 </dev/null &"
      lab_ssh "$jpc" "$launch" >/dev/null 2>&1 &
      launch_pids+=("$!")
      launch_hosts+=("$jpc")
    done
    for i in "${!launch_pids[@]}"; do
      if wait "${launch_pids[$i]}"; then
        launched["${launch_hosts[$i]}"]=1
      else
        lab_log "e19 $run_id: could not start the join on ${launch_hosts[$i]} (ssh exit $?)"
      fi
    done
  fi

  local run_rc=0
  wait "$run_pid" || run_rc=$?
  if ((run_rc != 0)); then
    lab_log "e19 $run_id: campaign_run_one exited $run_rc"
  fi

  local run_wall run_csum run_exit
  IFS='|' read -r run_wall run_csum _ run_exit <<<"$(e19_extract_run_info "$run_id")"
  if [[ -z "$aborted" ]] && { ((run_rc != 0)) || [[ "$run_exit" != 0 ]] || ! e19_is_number "$run_wall"; }; then
    lab_log "e19 $run_id: run failed (rc=$run_rc exit=$run_exit wall=$run_wall); no join rows"
    aborted="run_failed"
  fi

  if [[ -n "$aborted" ]]; then
    for jpc in "${joiner_hosts[@]}"; do
      e19_stop_sampler "$jpc" "$run_id"
    done
    e19_abort_repeat "$run_id" "$sys" "$rep" "$aborted" "$run_wall"
    return 0
  fi

  local deadline=$((join_started + ${E19_JOIN_WAIT_S:-180} + 15))
  local joined_so_far=0
  for jpc in "${joiner_hosts[@]}"; do
    local jres_line="" join_s="" status="no_result" first_work_s="" join_ns seed_s="" jout t0 t1 rc
    jcores="${cores_of[$jpc]}"
    if [[ -n "${status_of[$jpc]:-}" ]]; then
      status="${status_of[$jpc]}"
      lab_log "e19 $run_id: $jpc has no core count; dask worker not started, status $status"
    elif [[ -n "${launched[$jpc]:-}" ]]; then
      jres_line="$(e19_await_join_res "$jpc" "$run_id" "$deadline")"
      if [[ "$jres_line" =~ ^([0-9]+)\ ([0-9]+)\ ([0-9]+)$ ]]; then
        t0="${BASH_REMATCH[1]}" t1="${BASH_REMATCH[2]}" rc="${BASH_REMATCH[3]}"
        case "$rc" in
          0)
            status="ok"
            join_s="$(python3 -c "print(round(($t1 - $t0) / 1e9, 3))")"
            ;;
          2)
            if [[ "$sys" == pipedpeer* ]] && e19_confirm_joined "$joined_so_far"; then
              status="partial"
              join_s="$(python3 -c "print(round(($t1 - $t0) / 1e9, 3))")"
              lab_log "e19 $run_id: $jpc connect exited 2 (partly connected) but the cluster lists it; join_s kept, status partial; the join log below names the unreachable address"
            else
              status="failed"
              lab_log "e19 $run_id: join command failed on $jpc (exit 2, not confirmed by the cluster)"
            fi
            ;;
          124)
            status="timeout"
            lab_log "e19 $run_id: join timed out on $jpc after ${E19_JOIN_WAIT_S:-180}s"
            ;;
          *)
            status="failed"
            lab_log "e19 $run_id: join command failed on $jpc (exit $rc)"
            ;;
        esac
      else
        lab_log "e19 $run_id: no join.res from $jpc"
      fi
      if [[ "$status" == ok || "$status" == partial ]]; then joined_so_far=$((joined_so_far + 1)); fi
      if [[ "$status" != ok ]]; then
        jout="$(lab_ssh "$jpc" "tail -c 600 ~/pipedpeer-lab/runs/$run_id/join.log 2>/dev/null || true" 2>/dev/null | tr '\n' ' ' || true)"
        lab_log "e19 $run_id: $jpc join log: ${jout:-(empty)}"
      fi
    fi

    join_ns="$(lab_ssh "$jpc" "cat ~/pipedpeer-lab/runs/$run_id/join.stamp 2>/dev/null || true" 2>/dev/null || true)"
    if [[ -n "$join_ns" && -n "$jcores" ]]; then
      first_work_s="$(lab_ssh "$jpc" "python3 ~/pipedpeer-lab/work/cpu_sampler.py --streak ~/pipedpeer-lab/runs/$run_id/sampler.txt $join_ns $jcores 2>/dev/null || true" 2>/dev/null || true)"
    fi
    if [[ -z "$first_work_s" ]]; then
      lab_log "e19 $run_id: $jpc never reached work; first_work_s left empty"
    fi

    e19_stop_sampler "$jpc" "$run_id"

    if [[ "$sys" == pipedpeer* ]]; then
      seed_s="$(e19_extract_seed_s "$jpc" "$(lab_ip "$jpc")")"
    fi

    local jwarm="" jpurged="" jclosure=""
    IFS="|" read -r jwarm jpurged jclosure <<<"${warm_of[$jpc]:-}"
    e19_journal_add "$run_id|$sys|join|$rep|$jpc|$join_s|$first_work_s|$run_wall|$seed_s|$jwarm|$status|$run_csum|$jpurged|$jclosure"
  done
}

# e19_one_system <sys>: baseline and repeats (uses N, REPEATS, joiner_hosts).
e19_one_system() {
  local sys="$1"
  local prepared=0 base_id="E19-EL-$sys-n$N-r0" rep run_id
  local base_wall base_csum base_start base_exit closure="" usable=0

  if ! _run_id_in_runs "$base_id"; then
    if e19_prepare "$sys"; then
      prepared=1
      campaign_run_one E19 EL "$sys" "$N" 0 0 "${E19_TIMEOUT_S:-900}" strong || lab_log "e19 $base_id: campaign_run_one exited $?"
    else
      for ((rep = 1; rep <= REPEATS; rep++)); do
        e19_abort_repeat "E19-EL-$sys-n$N-r$rep" "$sys" "$rep" cluster_failed
      done
      e19_render_csv interim
      return 0
    fi
  fi

  IFS='|' read -r base_wall base_csum base_start base_exit <<<"$(e19_extract_run_info "$base_id")"
  if ! e19_recorded "$base_id"; then
    e19_journal_add "$base_id|$sys|baseline|0||||$base_wall||||$base_csum||"
  fi
  if [[ "$base_exit" == 0 ]] && e19_is_number "$base_wall"; then
    usable=1
  else
    lab_log "e19 $base_id: baseline failed (exit=$base_exit wall=$base_wall); repeats counted as no_baseline, none joined"
  fi

  if [[ "$sys" == pipedpeer* ]] && ((usable)); then
    closure="$(e19_closure_path "$base_start")"
  fi

  for ((rep = 1; rep <= REPEATS; rep++)); do
    run_id="E19-EL-$sys-n$N-r$rep"
    if e19_recorded "$run_id"; then
      continue
    fi
    if ((!usable)); then
      e19_abort_repeat "$run_id" "$sys" "$rep" no_baseline
    elif _run_id_in_runs "$run_id"; then
      lab_log "e19 $run_id: run recorded but its join rows are not; counted as interrupted"
      e19_abort_repeat "$run_id" "$sys" "$rep" interrupted
    elif ((!prepared)) && ! e19_prepare "$sys"; then
      lab_log "e19 $run_id: not run, cluster could not be reset to $N"
      e19_abort_repeat "$run_id" "$sys" "$rep" cluster_failed
    else
      prepared=0
      e19_one_repeat "$sys" "$rep" "$run_id" "$base_wall" "$closure"
    fi
    e19_render_csv interim
  done
}

e19_main() {
  local N=16 ADD=16 REPEATS=3 DRY=0
  local -a systems=(pipedpeer ray dask)

  while (($#)); do
    case "$1" in
      --n) N="${2:?--n needs a count}"; shift 2 ;;
      --add) ADD="${2:?--add needs a count}"; shift 2 ;;
      --repeats) REPEATS="${2:?--repeats needs a count}"; shift 2 ;;
      --systems) IFS=',' read -r -a systems <<<"${2:?--systems needs a list}"; shift 2 ;;
      --dry-run) DRY=1; shift ;;
      -h | --help) e19_usage; return 0 ;;
      *) e19_usage >&2; echo "e19.sh: unknown arg $1" >&2; return 2 ;;
    esac
  done

  if ((DRY)); then
    echo "E19-DRY-RUN n=$N add=$ADD repeats=$REPEATS systems=$(IFS=,; echo "${systems[*]}")"
    return 0
  fi

  local total_nodes=$((N + ADD))
  local -a all_hosts=()
  mapfile -t all_hosts <"$(lab_hosts "$total_nodes")"
  if ((${#all_hosts[@]} < total_nodes)); then
    lab_log "e19: not enough nodes in hosts.tsv (${#all_hosts[@]} < $total_nodes)"
    return 1
  fi

  local -a joiner_hosts=("${all_hosts[@]:$N:$ADD}")

  e19_init_outputs

  trap e19_cleanup EXIT
  trap 'e19_on_signal 130' INT
  trap 'e19_on_signal 143' TERM

  # Step 1: bring up cluster at N+ADD
  "$CLUSTER_SH" up "$total_nodes" || {
    lab_log "e19: cluster up failed"
    return 1
  }

  _load_sizes >/dev/null 2>&1 || lab_log "e19: _load_sizes failed; runs use their own defaults"

  # cpu_sampler.py on every joiner
  local jpc sys
  for jpc in "${joiner_hosts[@]}"; do
    e19_ssh_or_log "$jpc" "creating the work dir" "mkdir -p ~/pipedpeer-lab/work"
    if [[ -f "$WORKLOADS/cpu_sampler.py" ]]; then
      lab_rsync "$WORKLOADS/cpu_sampler.py" "$jpc:pipedpeer-lab/work/cpu_sampler.py" >/dev/null 2>&1 ||
        lab_log "e19: cpu_sampler.py not copied to $jpc (exit $?)"
    fi
  done

  for sys in "${systems[@]}"; do
    e19_one_system "$sys"
  done

  e19_render_csv final
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  e19_main "$@"
fi
