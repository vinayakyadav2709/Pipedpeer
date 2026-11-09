#!/usr/bin/env bash
# e19.sh: E19 elasticity campaign.
#
# Usage:
#   e19.sh [--n 16] [--add 16] [--repeats 3] [--systems pipedpeer,ray,dask] [--dry-run]
#
# Steps:
# 1. Bring up cluster with N + ADD nodes.
# 2. Joiners out: stop daemon and workers on joiners; on Pipedpeer, move nodes.db
#    aside (restored by trap) and delete E19 job closure from nix store and nar cache;
#    check /v1/store?path=...&runnable=1 says absent, flag joiner_warm=1 if present.
# 3. Baseline: campaign_run_one E19 EL $sys $N 0 0 ... with no join.
# 4. Join runs: per repeat, start cpu_sampler.py on each joiner, start run,
#    at 30% baseline wall join with join command (recording date +%s%N on joiner).
# 5. Compute first_work_s from streak of >= 5 samples with busy cores >= 50% cores.
# 6. Compare checksum across all runs at this size: majority wins, minority 0,
#    all differ -> empty + BLOCKERS.md line.
# 7. Outputs $LAB/analysis-input/e19.csv:
#    system,kind,i,pc,join_s,first_work_s,wall_s,result_ok,seed_s,joiner_warm

set -euo pipefail

REPO="${REPO:-$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)}"
CAMP="$REPO/scripts/campaign"
WORKLOADS="$REPO/docs/benchmarks/workloads"

# shellcheck source=/dev/null
source "$CAMP/lib.sh"
# shellcheck source=/dev/null
source "$CAMP/run.sh"

CLUSTER_SH="${CLUSTER_SH:-$CAMP/cluster.sh}"

e19_usage() {
  echo "usage: e19.sh [--n 16] [--add 16] [--repeats 3] [--systems pipedpeer,ray,dask] [--dry-run] [-h]"
}

# e19_resolve_checksums <"run_id:csum ...">
# Resolves majority checksum and outputs "run_id=1" (majority), "run_id=0" (minority),
# or "run_id=" (no majority/all differ), and logs / writes BLOCKERS.md when all differ.
e19_resolve_checksums() {
  local pairs="$1"
  python3 - "$pairs" "${LAB:-${LAB_RESULTS:-}}" <<'PY'
import sys, os

pairs_str = sys.argv[1]
lab = sys.argv[2]
data = []
for item in pairs_str.split():
    if ":" in item:
        rid, csum = item.split(":", 1)
        data.append((rid, csum))

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
            if ok == "0":
                sys.stderr.write(f"e19: checksum mismatch {rid}: got {csum} want {winner}\n")
else:
    for rid, csum in data:
        print(f"{rid}=")
    msg = "e19: checksum mismatch, no majority across systems"
    sys.stderr.write(f"{msg}\n")
    if lab:
        blockers = os.path.join(lab, "BLOCKERS.md")
        try:
            with open(blockers, "a") as f:
                f.write(f"{msg}\n")
        except OSError:
            pass
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

e19_extract_run_info() {
  local id="$1"
  python3 - "$LAB/runs.jsonl" "$id" <<'PY'
import sys, json, re
runs_path = sys.argv[1]
run_id = sys.argv[2]
wall_s = ""
csum = ""
try:
    with open(runs_path) as f:
        for line in f:
            d = json.loads(line)
            if d.get("run_id") == run_id:
                w = d.get("wall_s")
                if w is not None:
                    wall_s = str(w)
                lines = d.get("result_lines") or []
                for ln in lines:
                    m = re.search(r"checksum=([0-9.]+)", ln)
                    if m:
                        csum = m.group(1)
                        break
                break
except FileNotFoundError:
    pass
print(f"{wall_s} {csum}")
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

# e19_join_poll_py: python run on the joiner, in the campaign venv. Polls the
# driver's cluster view until this joiner's own node (Ray) or all its workers
# (Dask) are listed, so t1 is "usable by the scheduler", not "start returned".
# args: system, driver ip, joiner ip, workers expected, timeout s. Exit 0 when
# ready, 1 on timeout (join_s is then left empty by the caller).
e19_join_poll_py() {
  cat <<'PY'
import sys, time
system, driver, me, want, limit = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), float(sys.argv[5])
deadline = time.time() + limit
def count():
    if system == "ray":
        import ray
        if not ray.is_initialized():
            ray.init(address=driver + ":6379", logging_level="ERROR", log_to_driver=False)
        return sum(1 for n in ray.nodes() if n.get("Alive") and n.get("NodeManagerAddress") == me)
    from distributed import Client
    global client
    if client is None:
        client = Client(driver + ":8786", timeout=10)
    return sum(1 for a in client.scheduler_info()["workers"] if a.split("://")[-1].rsplit(":", 1)[0] == me)
client = None
while time.time() < deadline:
    try:
        got = count()
    except Exception as e:
        sys.stderr.write("join poll: %s\n" % e)
        got = 0
    if got >= want:
        sys.exit(0)
    time.sleep(0.5)
sys.stderr.write("join poll: %s on %s not ready after %ss\n" % (system, me, limit))
sys.exit(1)
PY
}

# e19_derive_store_path <pc>: the closure's store path, from the newest
# "peer took the closure" line of the driver's daemon log (field store).
# Empty when there is none.
e19_derive_store_path() {
  local pc="$1" line path
  line="$(lab_ssh "$pc" '
    { cat "${PIPEDPEER_DAEMON_STATE_DIR:-$HOME/.local/state}/pipedpeer/daemon.log" 2>/dev/null || true
      journalctl --user -u pipedpeer.service --no-pager 2>/dev/null || true
    } | grep "peer took the closure" | tail -1' 2>/dev/null || true)"
  path="$(grep -oE '/nix/store/[a-z0-9]{32}-[A-Za-z0-9._+-]+' <<<"$line" | head -1 || true)"
  printf '%s' "$path"
}

# Track joiners where nodes.db was moved aside so trap restores them
declare -a E19_JOINERS_ASIDE=()
e19_cleanup() {
  local pc
  for pc in "${E19_JOINERS_ASIDE[@]}"; do
    lab_ssh "$pc" '
      db="${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer/nodes.db"
      if [ -f "$db.aside" ]; then
        mv -f "$db.aside" "$db"
      fi
    ' 2>/dev/null || true
  done
}
trap e19_cleanup EXIT INT TERM

e19_main() {
  local N=16 ADD=16 REPEATS=3 DRY=0
  local -a systems=(pipedpeer ray dask)

  while (( $# )); do
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

  mkdir -p "$LAB/analysis-input"
  local csv="$LAB/analysis-input/e19.csv"
  [[ -f "$csv" ]] || printf 'system,kind,i,pc,join_s,first_work_s,wall_s,result_ok,seed_s,joiner_warm\n' >"$csv"

  # Step 1: bring up cluster at N+ADD
  "$CLUSTER_SH" up "$total_nodes" || { lab_log "e19: cluster up failed"; return 1; }

  # Sizing
  _load_sizes >/dev/null 2>&1 || true

  # Ensure cpu_sampler.py is deployed to ~/pipedpeer-lab/work on all joiners
  for jpc in "${joiner_hosts[@]}"; do
    lab_ssh "$jpc" "mkdir -p ~/pipedpeer-lab/work"
    if [[ -f "$WORKLOADS/cpu_sampler.py" ]]; then
      lab_rsync "$WORKLOADS/cpu_sampler.py" "$jpc:pipedpeer-lab/work/cpu_sampler.py" 2>/dev/null || true
    fi
  done

  # In-memory buffer of rows before majority checksum resolution
  # Each entry: run_id | system | kind | i | pc | join_s | first_work_s | wall_s | seed_s | joiner_warm | csum
  local -a buffered_rows=()
  local -a checksum_pairs=()

  for sys in "${systems[@]}"; do
    # Step 2: Joiners out
    for jpc in "${joiner_hosts[@]}"; do
      lab_ssh "$jpc" "
        systemctl --user stop pipedpeer.service 2>/dev/null || true
        pipedpeer stop 2>/dev/null || true
        ~/pipedpeer-lab/venv/bin/ray stop --force 2>/dev/null || true
        systemctl --user stop lab-ray 2>/dev/null || true
        systemctl --user stop lab-dask-worker 2>/dev/null || true
      " 2>/dev/null || true

      if [[ "$sys" == pipedpeer* ]]; then
        lab_ssh "$jpc" '
          db="${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer/nodes.db"
          if [ -f "$db" ]; then
            mv "$db" "$db.aside"
          fi
        ' 2>/dev/null || true
        E19_JOINERS_ASIDE+=("$jpc")
      fi
    done

    # Step 3: Baseline run (repeat 0, warmup 0, no join)
    local base_id="E19-EL-$sys-n$N-r0"
    if ! _run_id_in_runs "$base_id"; then
      campaign_run_one E19 EL "$sys" "$N" 0 0 "${E19_TIMEOUT_S:-900}" strong || true
    fi
    read -r base_wall base_csum <<<"$(e19_extract_run_info "$base_id")"
    buffered_rows+=("$base_id|$sys|baseline|0||||$base_wall|||$base_csum")
    checksum_pairs+=("$base_id:$base_csum")

    if [[ "$sys" == pipedpeer* && -z "${E19_STORE_PATH:-}" ]]; then
      E19_STORE_PATH="$(e19_derive_store_path pc00)"
      export E19_STORE_PATH
      if [[ -z "$E19_STORE_PATH" ]]; then
        lab_log "e19: no closure store path in pc00's daemon log; joiner_warm left empty (unknown), closure not purged"
      fi
    fi

    local med_wall="${base_wall:-60}"
    if python3 -c "import sys; float(sys.argv[1])" "$med_wall" 2>/dev/null; then
      :
    else
      med_wall=60
    fi

    # Step 4 & 5: Join runs
    for ((rep = 1; rep <= REPEATS; rep++)); do
      local run_id="E19-EL-$sys-n$N-r$rep"

      # Before repeat on Pipedpeer: delete closure from nix store and nar cache on joiners
      local -A joiner_warm_map=()
      if [[ "$sys" == pipedpeer* ]]; then
        local closure_path="${E19_STORE_PATH:-}"
        for jpc in "${joiner_hosts[@]}"; do
          lab_ssh "$jpc" "
            rm -rf \"\${XDG_DATA_HOME:-\$HOME/.local/share}/pipedpeer/work/nar-cache\"
            if [ -n \"$closure_path\" ]; then
              rel=\"\${closure_path#/nix/store/}\"
              rm -rf \"\${XDG_DATA_HOME:-\$HOME/.local/share}/pipedpeer/nix/store/\$rel\"
              rm -rf \"/nix/store/\$rel\" 2>/dev/null || true
            fi
          " 2>/dev/null || true

          # Check /v1/store?path=...&runnable=1
          local warm=""
          if [[ -n "$closure_path" ]]; then
            warm=0
            local chk
            chk="$(lab_ssh "$jpc" "curl -s -m 2 'http://127.0.0.1:4242/v1/store?path=${closure_path}&runnable=1' 2>/dev/null || true")"
            if [[ "$chk" == *'"cached":true'* || "$chk" == *'"cached": true'* ]]; then
              warm=1
              lab_log "e19 $run_id: closure still present on $jpc; flagged joiner_warm=1"
            fi
          fi
          joiner_warm_map["$jpc"]="$warm"
        done
      fi

      # Start cpu_sampler.py on each joiner
      for jpc in "${joiner_hosts[@]}"; do
        lab_ssh "$jpc" "
          mkdir -p ~/pipedpeer-lab/runs/$run_id
          rm -f ~/pipedpeer-lab/runs/$run_id/sampler.txt ~/pipedpeer-lab/runs/$run_id/sampler.pid ~/pipedpeer-lab/runs/$run_id/join.stamp ~/pipedpeer-lab/runs/$run_id/join.res
          export LAB_CORES=\$(awk -v p=\"$jpc\" '\$1==p {print \$5}' ~/pipedpeer-lab/hosts-$total_nodes.txt 2>/dev/null || echo 8)
          python3 ~/pipedpeer-lab/work/cpu_sampler.py ~/pipedpeer-lab/runs/$run_id/sampler.txt ${E19_TIMEOUT_S:-900} >/dev/null 2>&1 &
          echo \$! > ~/pipedpeer-lab/runs/$run_id/sampler.pid
        " 2>/dev/null || true
      done

      # Start run on pc00 in background
      campaign_run_one E19 EL "$sys" "$N" "$rep" 0 "${E19_TIMEOUT_S:-900}" strong &
      local run_pid=$!

      # Wait until run is running, then wait until 30% baseline wall
      if e19_wait_running "$run_id"; then
        local remain
        remain="$(python3 -c "print(max(0.0, 0.3 * $med_wall - $RUN_ELAPSED_NS / 1e9))")"
        sleep "$remain"
      fi

      # At 30%: join on each joiner
      local pp_code="" pc00_ip join_skip=0
      local -a join_pids=()
      pc00_ip="$(lab_ip pc00)"
      if [[ "$sys" == pipedpeer* ]]; then
        pp_code="$(lab_ssh pc00 'pipedpeer invite 2>/dev/null' | awk '{print $NF}' | tail -1 || true)"
        if [[ -z "$pp_code" ]]; then
          lab_log "e19 $run_id: pipedpeer invite on pc00 printed no code; repeat aborted, no join row"
          join_skip=1
        fi
      fi

      for jpc in "${joiner_hosts[@]}"; do
        ((join_skip)) && break
        local jcores
        jcores="$(awk -v p="$jpc" '$1==p {print $5}' "$LAB/hosts-$total_nodes.txt" 2>/dev/null || echo 8)"
        local jcmd=""
        case "$sys" in
          pipedpeer*)
            jcmd="t0=\$(date +%s%N); echo \"\$t0\" > ~/pipedpeer-lab/runs/$run_id/join.stamp; pipedpeer connect $pp_code; rc=\$?; t1=\$(date +%s%N); echo \"\$t0 \$t1 \$rc\" > ~/pipedpeer-lab/runs/$run_id/join.res; exit \$rc"
            ;;
          ray* | dask*)
            local pollsys jip poll_b64 starter
            pollsys="ray"; [[ "$sys" == dask* ]] && pollsys="dask"
            jip="$(lab_ip "$jpc")"
            poll_b64="$(e19_join_poll_py | base64 -w0)"
            if [[ "$pollsys" == ray ]]; then
              starter="systemd-run --user --unit=lab-ray --collect ~/pipedpeer-lab/venv/bin/ray start --address=$pc00_ip:6379 --block"
            else
              starter="systemd-run --user --unit=lab-dask-worker --collect ~/pipedpeer-lab/venv/bin/dask worker tcp://$pc00_ip:8786 --nworkers $jcores --nthreads 1"
            fi
            jcmd="t0=\$(date +%s%N); echo \"\$t0\" > ~/pipedpeer-lab/runs/$run_id/join.stamp; $starter; rc=\$?; if [ \$rc -eq 0 ]; then echo $poll_b64 | base64 -d | ~/pipedpeer-lab/venv/bin/python - $pollsys $pc00_ip $jip $([[ "$pollsys" == ray ]] && echo 1 || echo "$jcores") ${E19_JOIN_WAIT_S:-180}; rc=\$?; fi; t1=\$(date +%s%N); echo \"\$t0 \$t1 \$rc\" > ~/pipedpeer-lab/runs/$run_id/join.res; exit \$rc"
            ;;
        esac

        (
          lab_ssh "$jpc" "$jcmd"
        ) >/dev/null 2>&1 &
        join_pids+=("$!")
      done

      # Wait for the join commands to start / finish, and the run to complete
      wait "$run_pid" || lab_log "e19 $run_id: campaign_run_one finished"
      # the join commands write join.res when their poll ends: read it only after
      local jp
      for jp in "${join_pids[@]}"; do
        wait "$jp" || true
      done

      # Read job wall & checksum
      read -r run_wall run_csum <<<"$(e19_extract_run_info "$run_id")"
      checksum_pairs+=("$run_id:$run_csum")

      # Collect each joiner's metrics
      for jpc in "${joiner_hosts[@]}"; do
        if ((join_skip)); then
          lab_ssh "$jpc" "kill \$(cat ~/pipedpeer-lab/runs/$run_id/sampler.pid 2>/dev/null) 2>/dev/null || true" 2>/dev/null || true
          continue
        fi
        local jcores
        jcores="$(awk -v p="$jpc" '$1==p {print $5}' "$LAB/hosts-$total_nodes.txt" 2>/dev/null || echo 8)"
        local jres_line
        jres_line="$(lab_ssh "$jpc" "cat ~/pipedpeer-lab/runs/$run_id/join.res 2>/dev/null || true")"

        local join_s=""
        if [[ "$jres_line" =~ ^([0-9]+)\ ([0-9]+)\ ([0-9]+)$ ]]; then
          local t0="${BASH_REMATCH[1]}" t1="${BASH_REMATCH[2]}" rc="${BASH_REMATCH[3]}"
          if ((rc == 0)); then
            join_s="$(python3 -c "print(round(($t1 - $t0) / 1e9, 3))")"
          else
            lab_log "e19 $run_id: join command failed on $jpc (exit $rc)"
          fi
        else
          lab_log "e19 $run_id: no join.res from $jpc"
        fi

        # Compute first_work_s
        local first_work_s="" join_ns
        join_ns="$(lab_ssh "$jpc" "cat ~/pipedpeer-lab/runs/$run_id/join.stamp 2>/dev/null || true")"
        if [[ -n "$join_ns" ]]; then
          first_work_s="$(lab_ssh "$jpc" "python3 ~/pipedpeer-lab/work/cpu_sampler.py --streak ~/pipedpeer-lab/runs/$run_id/sampler.txt $join_ns $jcores 2>/dev/null || true")"
        fi
        if [[ -z "$first_work_s" ]]; then
          lab_log "e19 $run_id: $jpc never reached work; first_work_s left empty"
        fi

        # Stop sampler on joiner
        lab_ssh "$jpc" "
          pid=\$(cat ~/pipedpeer-lab/runs/$run_id/sampler.pid 2>/dev/null || true)
          if [ -n \"\$pid\" ]; then
            kill \"\$pid\" 2>/dev/null || true
          fi
        " 2>/dev/null || true

        # Seed seconds for pipedpeer
        local seed_s=""
        if [[ "$sys" == pipedpeer* ]]; then
          local jip
          jip="$(lab_ip "$jpc")"
          seed_s="$(e19_extract_seed_s "$jpc" "$jip")"
        fi

        local jwarm="${joiner_warm_map[$jpc]:-}"
        buffered_rows+=("$run_id|$sys|join|$rep|$jpc|$join_s|$first_work_s|$run_wall|$seed_s|$jwarm|$run_csum")
      done

      # Cluster up again if needed between repeats
      "$CLUSTER_SH" up "$total_nodes" || true
    done
  done

  # Step 6: Majority checksum judgment across all runs at this size
  local res_map
  res_map="$(e19_resolve_checksums "${checksum_pairs[*]}")"
  local -A ok_map=()
  while IFS='=' read -r rid ok_val; do
    [[ -n "$rid" ]] || continue
    ok_map["$rid"]="$ok_val"
  done <<<"$res_map"

  # Write out final rows to e19.csv
  for row in "${buffered_rows[@]}"; do
    IFS='|' read -r rid sys kind i pc join_s first_work_s wall_s seed_s jwarm _csum <<<"$row"
    local result_ok="${ok_map[$rid]:-}"
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$sys" "$kind" "$i" "$pc" "$join_s" "$first_work_s" "$wall_s" "$result_ok" "$seed_s" "$jwarm" >>"$csv"
  done
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  e19_main "$@"
fi
