# Lab campaign: every measurement the paper and report need

One file, two readers. Part 1 is for the person going to the lab (the operator). Part 2 is for the operator's coding agent, which does all the work. Appendix A is the one script that runs on every lab PC.

The lab: 60+ Linux PCs, CPU only, root on all, anything may be installed. The campaign uses 33 PCs: pc00 is the head (submitter, Ray head, Dask scheduler) and pc01..pc32 are workers. E11 uses all available PCs.

## Part 1: for the operator

> **Superseded by the headless lab day** ([scripts/campaign/HEADLESS.md](../../scripts/campaign/HEADLESS.md)): no laptop; one `curl … | sudo bash` line on every PC, `… -s -- head` on pc00, and pc00 runs the whole day. The laptop flow below remains as the fallback (`scripts/campaign/serve.sh` serves `lab.sh` for worker setup).

Your agent does the code, the scripts, the runs and the analysis. You get lab time, sit at each PC once, and do the hand-timed tasks (E3, E10).

### What you need

- Your laptop (the driver): Linux, the Pipedpeer repo cloned and on `dev`, your coding agent, an SSH key (`ssh-keygen -t ed25519` if you have none). On the lab network (cable preferred) during every session.
- From the project lead: Cloudflare R2 account id, access key, secret.
- Lab time: three sessions if possible (about 1.5 h, 4 h, 3 h), 33+ PCs.
- Your phone (hotspot, for E9). Optional: 4-6 classmates for 1 h (E10).

### Before the lab (at home, 2-4 days)

1. Save this file as `docs/benchmarks/lab-campaign.md` in your clone.
2. Give your agent the prompt the project lead sent you.
3. Hand it the R2 keys when it asks.
4. Done when it says the rehearsal passed and gives you `results/lab/OPERATOR.md`. Read it; it is your exact script for the lab.

### Session 1 (about 1.5 h): setup

1. Your agent starts a small web server on your laptop and tells you one line.
2. On each of the 33 PCs, open a terminal and type that line. It looks like:

   ```sh
   curl -fsS http://<your-laptop-ip>:8000/lab-pc.sh | sudo bash -s -- setup
   ```

   It ends with a line starting `LAB-PC`. If it prints `ERROR`, photograph it for your agent.
3. Tell your agent "all PCs done". From here it drives every PC over SSH.
4. It checks network, clocks and telemetry and runs each experiment once.

### Sessions 2 and 3: measurements

Tell your agent "session 2" (then "session 3"); it follows OPERATOR.md.

Your hand jobs:

- **E3:** on two PCs, under a fresh account, set up PipedPeer, Ray and Dask yourself from their official quick starts, until the test script runs on both. The agent times you and counts commands.
- **E9:** put your laptop on your phone's hotspot when asked.
- **E10 (if time):** each classmate does the E3 task for PipedPeer and Dask. Ask their consent; record no names.

Do not leave until the agent says cleanup is done (it runs `undo` on every PC, which removes exactly what setup added).

### After the lab

Your agent writes the analysis and opens a PR to `dev`. Send the link to the project lead. Done.

## Part 2: for the agent

You run on the driver laptop. The operator is your hands in the lab. Your job: produce every number, table and figure the PipedPeer paper and report lack, store it in the repo and in Cloudflare R2, and leave the lab PCs as you found them. The decisions below are made; follow them. Product code changes are allowed where a step needs them (with tests, per AGENTS.md). If the same error happens three times in a row, or a step is impossible as written, write it in `results/lab/BLOCKERS.md` and tell the operator.

### 0. Read first, in this order

1. `AGENTS.md`, then `CONTEXT-MAP.md`.
2. `docs/benchmarks/RESULTS.md` (what is measured; "Not measured").
3. `docs/known-issues.md`, item 3.
4. `docs/paper/paper.tex`, sections Evaluation and Limitations.
5. `scripts/bench-pool.sh`, `scripts/bench-split.sh`, `scripts/bench-ddp.sh`, `scripts/lab-fail.sh`, `scripts/lib/cli.sh`: reuse, do not rewrite.

Work on branch `lab-campaign` from `dev`; commit each working step; one PR to `dev` at the end. Do not push to `dev` or `main` directly.

### 1. Before the lab (at home)

#### 1.1 Fix known issue 3

A job whose closure is already on the submitting node must still seed the peers, so pool spill has peers. Write the failing test first (the `lab-fail.sh` scenario), fix it; `scripts/lab-fail.sh` on `make lab-up` must then pass with `remote_items > 0`. E6 depends on this.

#### 1.2 Workloads (fixed for the whole campaign)

| Id | Script | Exercises |
|---|---|---|
| W1 | `docs/benchmarks/workloads/pool_map.py` | `multiprocessing.Pool` map, CPU bound |
| W2 | `pipedpeer-demo/cpu.py` | mixed: Pool, joblib, NumPy, pandas |
| W3 | `pipedpeer-demo/01_sklearn_rf.py` | joblib / scikit-learn |
| W4 | `docs/benchmarks/workloads/grid_search.py` | scikit-learn `GridSearchCV(n_jobs=-1)`, 256 fits (strong size 30 s on one PC) |
| T1 | `scripts/bench-train.py`, MLP-C and the transformer | data-parallel training on CPU |

Strong-scaling size: one lab PC alone takes 100-150 s. Weak-scaling size (W1 only): about 60 s of work per PC, items proportional to N. Record sizes in `results/lab/workloads.md`; never change a size between systems.

#### 1.3 Baseline ports

In `docs/benchmarks/workloads/baselines/`, a Ray and a Dask version of each W, the smallest idiomatic change:

- Ray: `ray.util.multiprocessing.Pool` for Pool code; `ray.util.joblib` (`register_ray`) for joblib code.
- Dask: `dask.distributed.Client` with `client.map`/`gather` for Pool code; `joblib.parallel_backend("dask")` for joblib code.
- Plain: the original with stock `python3` on one PC.

Changed-line counts (`diff -u orig port | grep -c '^[+-][^+-]'`) go to `results/lab/port-lines.csv`; PipedPeer's is 0.

One pinned Nix environment holds Python, NumPy, pandas, scikit-learn, joblib, torch (CPU), `ray[default]`, `dask[distributed]`; all four systems use it on every PC.

#### 1.4 Runner: `scripts/campaign/`

- Fan-out with Ansible (inventory built from the setup scan, ids pc00..pcNN). No hand-rolled ssh loops.
- `run.sh <experiment>` runs one experiment and appends one JSON line per run to `results/lab/runs.jsonl`: `run_id`, `experiment`, `workload`, `system`, `n`, `repeat`, `warmup`, `start_ns`, `end_ns`, `wall_s`, `result_ok` (output equals the plain run's), `hosts`, `git_commit`, `pipedpeer_version`, and the PipedPeer receipt (`--export json`) when the system is pipedpeer.
- Within each N the systems run in a fresh random order per repeat; one warm-up per (system, N) first, `warmup: true`, excluded from statistics.
- Restricting to N workers: PipedPeer stops the daemon on the rest (`pipedpeer stop`); Ray starts the head on pc00 plus N workers; Dask the scheduler on pc00 plus N workers. pc00 always submits and does no work for Ray/Dask; for PipedPeer it is counted as one of the N (state this in the write-up).
- `analyse.py`: one CSV per experiment and figures (matplotlib, PNG and PDF) in `results/lab/figures/`. Median and interquartile range over 5 repeats. Speedup = median wall of plain at N=1 / median at N.

#### 1.5 Telemetry to Cloudflare (OpenTelemetry)

- R2 bucket `pipedpeer-lab`, account id and keys from the operator, kept in `~/.config/pipedpeer-lab/r2.env` (mode 600), never in the repo.
- Every lab PC: `otelcol-contrib` (one pinned release), `hostmetrics` receiver (cpu, memory, network, load; 1 s), resource attribute `lab.pc_id`, the PipedPeer daemon log (filelog or journald, whichever the install uses), exporting OTLP/gRPC to the driver.
- Driver: `otelcol-contrib` receiving OTLP plus a filelog receiver on `results/lab/runs.jsonl`; exporters `file` (rotating, `results/lab/otel/`) and `awss3` to R2: `endpoint: https://<ACCOUNT_ID>.r2.cloudflarestorage.com`, `region: auto`, `s3_force_path_style: true`, `s3_bucket: pipedpeer-lab`, `s3_prefix: otel`, `marshaler: otlp_json`.
- After every experiment: `rclone sync results/lab r2:pipedpeer-lab/raw`.
- Clocks: every PC within 10 ms of the driver (`chronyc tracking` or `timedatectl`); fix with chrony if not; record in `results/lab/clocks.txt`.
- Test the whole pipeline against the container lab; confirm objects in R2.

#### 1.6 Setup script and rehearsal

- Put Appendix A at `scripts/campaign/lab-pc.sh` (now `scripts/campaign/lab.sh`). Fill in `DRIVER` and serve it with your public key from `results/lab/serve/` on port 8000 (`python3 -m http.server`).
- Find the PCs afterwards with `nmap -p22 --open <lab subnet>` plus a key login test; build the inventory.
- Rehearse on `make lab-up`: every experiment at N=1 and 2, one repeat, through the real runner and telemetry; all must pass.
- Write `results/lab/OPERATOR.md`: what to bring, the exact line to type on each PC, what each session does and prints, which hand jobs come when.

### 2. Session 1: setup and smoke (about 1.5 h)

1. Serve the script; the operator runs it on 33 PCs (more later for E11).
2. Scan, build inventory, `ansible all -m ping` all green.
3. Inventory each PC (`lscpu`, `nproc`, `free -g`, `/etc/os-release`, `uname -r`, NIC speed via `ethtool`, Python) → `results/lab/machines.csv`.
4. Network: `iperf3` bandwidth and ping RTT from pc00 to every PC → `results/lab/network.csv`; confirm 4242/tcp, 4242/udp, 4243/udp, 4244/udp.
5. Install over SSH with the pinned build: `scripts/install-pipedpeer.sh --version <v> -y`, `pipedpeer doctor`; join (`pipedpeer invite` on pc00, `pipedpeer connect <code>` elsewhere). Install otelcol and the Nix environment everywhere.
6. Clock check, telemetry check (objects in R2), each experiment once at N=2.

### 3. Sessions 2 and 3: measurements

Session 2 is E1/E2 (about 4 h, unattended runs; the operator stays). Session 3 is the rest.

| Id | What | Runs |
|---|---|---|
| E1/E2 | Strong scaling and baselines: W1, W2 × {plain (N=1), pipedpeer, ray, dask} × N = 1, 2, 4, 8, 16, 32. W3 at N = 1, 4, 16, 32 | 5 repeats + warm-up |
| E1b | Weak scaling, W1: pipedpeer, ray, dask at N = 1, 4, 16, 32 | 5 repeats |
| E3 | Setup effort: on 2 PCs, fresh user account, the operator follows each tool's official quick start (PipedPeer `pipedpeer-demo/KIT.md`, Ray cluster quick start, Dask distributed quick start) until W1 runs across both. Minutes, commands typed, script lines changed; terminal recorded with asciinema | Once per system |
| E4 | Uneven machines: N=4 with cores capped 8/4/2/1 (`systemd-run --scope -p AllowedCPUs=...` around each daemon/worker). PipedPeer measured split vs PipedPeer headcount split (the switch used in commit 8c9284c: `git show 8c9284c`) vs ray vs dask, W1 | 5 repeats |
| E5 | Shipping the environment to 32 PCs. Cold: first PipedPeer job on PCs without the closure (time, MB from receipts) vs `pip install -r` of the same pins into fresh venvs on all 32 at once (time, MB) vs Ray `runtime_env={"pip": ...}`. Update: add tqdm, measure again | Cold once, update 3 times |
| E6 | Faults: W1 at N=16, `kill -9` one worker's daemon (pipedpeer) or worker process (ray, dask) at about 30% progress; result correct, wall vs no kill. PipedPeer Ctrl-C to all-stopped latency | 3 kills per system, 3 Ctrl-C |
| E7 | Interception overhead on one lab PC: `docs/benchmarks/workloads/trivial.py` with and without interception, method as RESULTS.md R3 | 5 repeats |
| E8 | CPU training T1: PipedPeer exact and fast sync vs torchrun DDP (gloo), ranks 2, 4, 8, same model, batch and steps; wall, validation loss vs 1 rank, bytes sent per rank | 3 seeds |
| E9 | Across networks: the operator's laptop on a phone hotspot joins the lab cluster through the introducer and submits W1. Path type in `pipedpeer status`, wall, introducer bytes; `scripts/net-verify.sh` | Once |
| E10 | If time: 4-6 classmates new to both tools do the E3 task for PipedPeer and Dask, order alternated. Minutes, success, where stuck. Consent; IDs p1..p6 only | Once each |
| E11 | Cluster size: run the setup line on every available PC (target 60). Time until `pipedpeer status` on pc00 shows all nodes; membership and gossip bytes per node per minute at 8, 16, 32, 60 nodes (10 min idle each); introducer bytes; one W1 run at 60 | Once per size |
| E12 | Elastic join: W1 (13 x the strong size) starts on 4 PCs; 4 more join every 20 s up to 16 (daemons, Dask workers or `ray start` the runner had stopped). Items per second per PC over time, join events marked (`scripts/campaign/FIGURES.md`) | Once per system |
| E13 | Churn: W1 at N=16, E1b's weak size; every 20 s `kill -9` one random worker (daemon, raylet, Dask worker), restart it 10 s later. Result correct, wall vs E1b's no-churn wall, items re-run, throughput timeline with the kills | Once per system |
| E14 | Curl to cluster: when each PC printed LAB-PC, answered SSH, had pipedpeer, joined, and ran its first work in the day's first distributed job (W1 on all PCs, right after the join). No extra runs | Once (the day) |
| E15 | Network sensitivity: W1 at N=4 with `tc netem` adding 0, 20, 80 ms RTT between the PCs | 2 repeats per RTT |

Upload to R2 after each experiment. A failed experiment is a result: record what, which PC, the log path, and continue.

### 4. Before leaving the lab (last session)

- On every PC: `pipedpeer uninstall`, remove otelcol, venvs, Ray/Dask temp dirs, E3/E10 accounts; then `bash lab-pc.sh undo`.
- Ask the operator whether the lab wants Nix kept; remove it otherwise.
- List before deleting; remove only what the campaign created.
- Final `rclone sync`; object count in R2 matches local.

### 5. After the lab

- `analyse.py` → CSVs and figures: strong-scaling speedup vs N (one line per system, a panel per workload), weak-scaling efficiency, E4 bars, E5 bars, E8 table, E11 membership cost vs N, CPU utilisation per PC during one W1 run at N=32 (host metrics).
- Write `docs/benchmarks/lab-<YYYY-MM-DD>.md`: setup (machines, network, versions), each experiment's table with median and IQR, every failure, and what each result does and does not show. Container results are labelled emulated.
- Raw data in `docs/benchmarks/raw/lab-<date>/` (CSV, JSONL, receipts, logs); OTel files stay in R2.
- Do not edit the paper or report; the authors integrate the results.
- PR to `dev`: what was measured, what failed, R2 paths, product-code changes with their tests, and `Tried/failed:` (`<what> — <why>` per line, or none).

## Appendix A: lab-pc.sh

Runs as root on each lab PC (`curl ... | sudo bash -s -- setup`). Opens SSH for the driver, lets the lab user run sudo without a password for the campaign, opens the ports, prints one `LAB-PC` line. `undo` reverses exactly what setup recorded. The agent fills in `DRIVER` and may fix the script; keep `undo` exact.

The working copy is `scripts/campaign/lab.sh` (it was `lab-pc.sh`); the original as written is:

```bash
#!/usr/bin/env bash
# Lab PC setup for the PipedPeer campaign. Run as root:
#   curl -fsS http://DRIVER:8000/lab-pc.sh | sudo bash -s -- setup
#   sudo bash lab-pc.sh undo
set -euo pipefail
DRIVER="__DRIVER_IP__"
LAB_USER="${SUDO_USER:-$(logname 2>/dev/null || echo root)}"
HOME_DIR=$(getent passwd "$LAB_USER" | cut -d: -f6)
STATE=/var/lib/pipedpeer-lab/state
MARK="# pipedpeer-lab"
SUDOERS=/etc/sudoers.d/pipedpeer-lab

log()  { echo "[lab-pc] $*"; }
die()  { echo "[lab-pc] ERROR: $*" >&2; exit 1; }
record() { mkdir -p "$(dirname "$STATE")"; echo "$1" >>"$STATE"; }

[[ $EUID -eq 0 ]] || die "run with sudo"

pkg() {
  if command -v apt-get >/dev/null; then apt-get update -qq && apt-get install -y -qq "$@"
  elif command -v dnf >/dev/null; then dnf install -y -q "$@"
  elif command -v pacman >/dev/null; then pacman -S --noconfirm --needed "$@"
  else die "unknown package manager; install $* by hand"; fi
}

setup() {
  # SSH server
  if ! command -v sshd >/dev/null && [[ ! -x /usr/sbin/sshd ]]; then
    pkg openssh-server; record "pkg openssh-server"
  fi
  local unit=sshd; systemctl cat ssh.service >/dev/null 2>&1 && unit=ssh
  systemctl is-active --quiet "$unit" || { systemctl enable --now "$unit"; record "started $unit"; }

  # Driver key for the lab user
  local key; key=$(curl -fsS "http://$DRIVER:8000/driver.pub") || die "cannot fetch driver.pub from $DRIVER"
  install -d -m 700 -o "$LAB_USER" "$HOME_DIR/.ssh"
  touch "$HOME_DIR/.ssh/authorized_keys"
  grep -qF "$key" "$HOME_DIR/.ssh/authorized_keys" || echo "$key $MARK" >>"$HOME_DIR/.ssh/authorized_keys"
  chown "$LAB_USER" "$HOME_DIR/.ssh/authorized_keys"; chmod 600 "$HOME_DIR/.ssh/authorized_keys"

  # Passwordless sudo for the campaign only
  [[ -f $SUDOERS ]] || { echo "$LAB_USER ALL=(ALL) NOPASSWD: ALL" >$SUDOERS; chmod 440 $SUDOERS; record "sudoers"; }

  # Ports: 22/tcp, 4242/tcp, 4242-4244/udp
  if command -v ufw >/dev/null && ufw status | grep -q 'Status: active'; then
    for p in 22/tcp 4242/tcp 4242/udp 4243/udp 4244/udp; do ufw allow "$p" >/dev/null; record "ufw $p"; done
  elif command -v firewall-cmd >/dev/null && firewall-cmd --state >/dev/null 2>&1; then
    for p in 22/tcp 4242/tcp 4242/udp 4243/udp 4244/udp; do firewall-cmd -q --add-port="$p"; record "fwd $p"; done
  fi

  local ip; ip=$(ip -4 route get "$DRIVER" | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1);exit}}')
  echo "LAB-PC host=$(hostname) ip=$ip user=$LAB_USER cores=$(nproc) ram_gib=$(awk '/MemTotal/{printf "%.0f",$2/1048576}' /proc/meminfo)"
}

undo() {
  [[ -f $STATE ]] || { log "nothing recorded"; return 0; }
  while IFS= read -r l; do
    case "$l" in
      "ufw "*)     ufw delete allow "${l#ufw }" >/dev/null || echo "[lab-pc] WARNING: ufw ${l#ufw } not removed" >&2 ;;
      "fwd "*)     firewall-cmd -q --remove-port="${l#fwd }" || echo "[lab-pc] WARNING: firewalld ${l#fwd } not removed" >&2 ;;
      "started "*) systemctl disable --now "${l#started }" || echo "[lab-pc] WARNING: ${l#started } not stopped" >&2 ;;
      sudoers)     rm -f $SUDOERS ;;
      "pkg "*)     if command -v apt-get >/dev/null; then apt-get remove -y -qq "${l#pkg }"; else dnf remove -y -q "${l#pkg }"; fi ;;
    esac
  done <"$STATE"
  sed -i "/$MARK\$/d" "$HOME_DIR/.ssh/authorized_keys" 2>/dev/null || true
  rm -rf "$(dirname "$STATE")"
  log "undo complete"
}

case "${1:-}" in
  setup) setup ;;
  undo)  undo ;;
  *) die "usage: lab-pc.sh setup|undo" ;;
esac
```
