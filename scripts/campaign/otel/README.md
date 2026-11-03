# Campaign telemetry

Plan: [docs/benchmarks/lab-campaign.md](../../../docs/benchmarks/lab-campaign.md), section 1.5.

```
lab PC: node.sh ──OTLP/gRPC zstd :4317──▶ driver: telemetry.sh ──▶ results/lab/otel/{metrics,logs}.zjsonl
  otelcol: host metrics, process metrics,          file_log results/lab/runs.jsonl   └▶ awss3 gzip → R2 pipedpeer-lab/otel/
  Ray/Dask/self prometheus, daemon/Ray/Dask/                                          (only with r2.env)
  run logs, pp-poll.py JSON lines
  pp-poll.py: daemon API, sockets, kernel counters, processes, clock, facts, scrape targets
  tcpdump (--pcap): pcap/<pc_id>-*.pcap ──runner fetch──▶ results/lab/pcap/<pc_id>/
sync-r2.sh: rclone sync results/lab → r2:pipedpeer-lab/raw (pcap/ included; not otel/, serve/)
clocks.sh: offset of each PC from the driver → results/lab/clocks.txt
```

Everything installed lives under `~/.local/share/pipedpeer-lab/` (binaries in `bin/`, collector config, pids, logs and read offsets in `otel/`, the logs the collector reads in `logs/`, captures in `pcap/`); no root except for `--pcap`. `install-otelcol.sh remove` undoes a node.

## Pins

Set in `lib.sh`; the checksums are the releases' published sha256 values, checked on every download.

| Tool | Version | linux amd64 sha256 | linux arm64 sha256 |
|---|---|---|---|
| otelcol-contrib | 0.164.0 | `f0c1ce9f661399beb4a58b711924716e08ea70c3802db0715fcfde3cbbada2d4` | `8bd4a7f443405522a2175cc1a8675f9e99cc047c7181b73461efd08efbba38d3` |
| rclone | 1.75.2 | `349ac8fba6ff65d6247043f1750cdcb518ec5d500ef91463a10d37c0ccdf3702` | `7e1e8d69654941b7b7df84ee74c5f7fb09cce7d5496947fb7bf16b55ff427d10` |

otelcol 0.164 names components in snake_case (`host_metrics`, `file_log`, `otlp_grpc`); the configs use those names.

## Daemon log

`pipedpeer setup` (what `install-pipedpeer.sh -y` runs) starts the daemon detached, appending to `$XDG_STATE_HOME/pipedpeer/daemon.log` (default `~/.local/state/pipedpeer/daemon.log`; `internal/daemonctl`). `node.sh` reads that file with `file_log`. With `pipedpeer setup --boot` the daemon is the systemd user unit `pipedpeer.service` and logs to the user journal; `node.sh` picks `journald` (match `_SYSTEMD_USER_UNIT=pipedpeer.service`) when that unit file exists. Run `node.sh` as the user the daemon runs as. Both receivers start at the beginning and keep their offset in `file_storage`, so a restart neither repeats nor drops lines.

## Use

```sh
scripts/campaign/telemetry.sh up                            # driver; warns and runs file-only without r2.env
scripts/campaign/otel/install-otelcol.sh                    # on each PC
scripts/campaign/otel/node.sh start pc07 <driver-ip> [--pcap] [--session s2]   # on each PC
scripts/campaign/telemetry.sh check                         # counts per lab.pc_id and source, last 60 s; R2 objects
scripts/campaign/sync-r2.sh && scripts/campaign/sync-r2.sh count
scripts/campaign/clocks.sh                                  # hosts from results/lab/inventory.ini
```

R2 credentials: `~/.config/pipedpeer-lab/r2.env`, mode 600, lines `R2_ACCOUNT_ID=…`, `R2_ACCESS_KEY_ID=…`, `R2_SECRET_ACCESS_KEY=…`. The file is parsed, not sourced. The keys reach the collector as `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` in its environment and rclone as `RCLONE_CONFIG_R2_*`; no file in the repo or under `~/.local/share/pipedpeer-lab` holds them.

## Proving R2 end to end

Once `r2.env` exists, on the driver (it runs a node collector on itself as `drv`):

```sh
scripts/campaign/telemetry.sh down; scripts/campaign/telemetry.sh up && scripts/campaign/otel/install-otelcol.sh && scripts/campaign/otel/node.sh start drv 127.0.0.1 && sleep 45 && scripts/campaign/telemetry.sh check && scripts/campaign/sync-r2.sh && scripts/campaign/sync-r2.sh count; scripts/campaign/otel/node.sh stop
```

Pass: `check` lists `metrics_*.json` and `logs_*.json` objects under `pipedpeer-lab/otel/year=…/`, and `count` shows the same local and remote numbers.

## Testing without R2

`PIPEDPEER_LAB_S3_ENDPOINT=http://127.0.0.1:9000` points the collector and rclone at an S3 stand-in instead of R2, for example `rclone serve s3 --auth-key KEY,SECRET --addr 127.0.0.1:9000 <dir>` with a `pipedpeer-lab` directory in it. `PIPEDPEER_LAB_HOME` and `PIPEDPEER_LAB_R2_ENV` move the install directory and the credentials file.

For the container lab (`LAB_NAME=otel scripts/lab-up.sh`, rootless podman), containers reach the driver at `host.containers.internal`; copy `scripts/campaign` in with `docker cp` and run `node.sh start pc0N host.containers.internal`. The nix image has no `sed`, `setsid`, `ss` or `python3`; `node.sh` needs none of them once the campaign env is linked at `~/.local/share/pipedpeer-lab/env` (pp-poll.py then falls back to `/proc/net` for sockets). `PIPEDPEER_LAB_OTLP_PORT` moves the driver off 4317 when another driver already runs on the host; `LAB_RESULTS` moves results/lab.

## Sources

Every record carries the resource attributes `lab.pc_id`, `host.name`, `lab.session` and `lab.source`; `telemetry.sh check` counts by `lab.pc_id` and source. Records from pp-poll.py and from per-run files also carry `lab.run_id` (see [Joining a run](#joining-a-run)).

| Source (`check` name) | What | Period | Where it comes from |
|---|---|---|---|
| `host.cpu` | per-core time and utilisation, frequency, logical/physical counts | 1 s | host_metrics |
| `host.memory`, `host.paging`, `host.load` | memory with utilisation and limit; swap and faults; load | 1 s | host_metrics |
| `host.network` | per-interface bytes, packets, errors, drops; conntrack (TCP connection states are off: net/sockets has the campaign's) | 1 s | host_metrics |
| `host.disk`, `host.processes` | per-device io, ops, time; process counts and forks | 1 s | host_metrics |
| `host.filesystem` | usage and utilisation of real filesystems | 10 s | host_metrics |
| `host.process` | per process: cpu time and utilisation, memory, threads, disk io, context switches, fds, page faults, uptime. Only pipedpeer, python, `ray::*`, raylet, gcs_server, dask, crun, nix, torchrun, otelcol, tcpdump (by comm) | 1 s | host_metrics process scraper |
| `prom.ray` | Ray's per-node metrics (tasks, actors, object store, resources) | 5 s | prometheus, target from `logs/sd/ray.json` |
| `prom.dask` | Dask scheduler and per-worker metrics | 2 s | prometheus, targets from `logs/sd/dask.json` |
| `prom.otelcol-contrib` | the collector's own sent, refused, failed and queue counters: proves nothing was dropped | 10 s | prometheus |
| `daemon-log` | the PipedPeer daemon log | as written | file_log or journald |
| `ray-log` | Ray's session logs (raylet, gcs_server, workers, dashboard agent, events) and `logs/ray/*.log` | as written | file_log |
| `dask-log` | `logs/dask/*.log` | as written | file_log |
| `pipedpeer-run` | `logs/pipedpeer-run/<run_id>.log`, the CLI's output | as written | file_log; run id from the file name |
| `pp-poll/api` | the local daemon's `/health`, `/v1/pool/stats`, `/v1/jobs`, `/v1/peers/unreachable` (1 s), `/v1/nodes`, `/v1/introducers`, `/v1/reserved` (5 s), `/v1/addresses` (30 s); body as returned, or the error | 1-30 s | pp-poll.py → `logs/pp-poll.jsonl` |
| `net/sockets` | every TCP/UDP socket on the campaign ports (4242-4244, Ray 6379-6385, 8090, 8265, Dask 8786-8787, 9000-9199, Ray workers 10002-10999, torchrun 29500): state, queues, owner pid; with `ss`, also bytes sent/acked/received, segments, rtt, cwnd, retransmits, skmem drops | 1 s | pp-poll.py → `logs/net.jsonl` |
| `net/snmp` | kernel Ip, IpExt (octets), Tcp, TcpExt (retransmits, drops, timeouts) and all Udp counters | 1 s | pp-poll.py → `logs/net.jsonl` |
| `node-state/procs` | campaign processes with full command line, ppid, start time (join to `host.process` on pid) | on change, at least 60 s | pp-poll.py → `logs/node-state.jsonl` |
| `node-state/clock` | `chronyc -c tracking` (offset, frequency, stratum, root delay) and `timedatectl show` | 30 s | pp-poll.py |
| `node-state/facts` | `uname -a`, `lscpu`, cpu models, meminfo, NIC speed/duplex/MTU (and `ethtool` when present), os-release, kernel cmdline, clock, pipedpeer version | each `node.sh start` | pp-poll.py |
| `node-state/dask-sd`, `ray-sd` | the scrape targets, when they change | on change | pp-poll.py |
| `energy` | RAPL package energy: every top-level powercap zone's `energy_uj` (or hwmon `amd_energy`), as read; one `energy-start` line with the zones, or `available: false` and why (E16) | 1 s | `energy.sh` (root through `sudo -n`) → `logs/energy.jsonl` |
| pcap (not OTel) | packet headers (96 B) on 4242-4244, not loopback | continuous, 10 min files | `node.sh --pcap` |

Not available: the daemon exposes no gossip or membership byte counters (`src/internal/daemonapi` has none). For E11 take membership bytes from the pcaps (exact, per port and peer) or from `net/sockets` (TCP bytes per connection on 4242; UDP sockets have no byte counters in Linux). pp-poll.py never polls `/v1/stats`: that route asks every node, so 60 pollers would add 3,600 requests a second to the traffic E11 measures.

## Runner integration

What the runner (`run.sh`, `lib/`, Ansible) must do so every source fills. Paths are on each lab PC; `$LAB_HOME` is `~/.local/share/pipedpeer-lab`.

1. **Start telemetry** on every PC once per session, after `install-otelcol.sh` and `env/install.sh`, as the user the daemon runs as:
   `scripts/campaign/otel/node.sh start <pc_id> <driver_ip>[:port] --session <session> [--pcap]`
   `--session` (or `$PIPEDPEER_LAB_SESSION`, or the file `$LAB_HOME/session`) becomes `lab.session` on every record; use `s1`, `s2`, `s3`. `--pcap` needs `sudo -n` (lab-pc.sh gives it) and starts tcpdump. Stop with `node.sh stop`. Ansible: `ansible all -m shell -a "bash <repo>/scripts/campaign/otel/node.sh start {{ inventory_hostname }} <driver> --session s2 --pcap"`. The command returns at once, also over ssh.
2. **Run id on every PC**, before each run starts and again (to `idle` or empty) after it ends:
   `printf '%s\n' <run_id> > $LAB_HOME/current-run` on all PCs of the experiment (Ansible `copy content=`). pp-poll.py rereads it each second and stamps `run_id` on its lines.
3. **Log directories**, created by `node.sh start`:
   - pipedpeer: `pipedpeer run ... > $LAB_HOME/logs/pipedpeer-run/<run_id>.log 2>&1` on the submitting PC (pc00). One file per run; the name is the run id.
   - Ray: `ray start ... > $LAB_HOME/logs/ray/start-<run_or_group>.log 2>&1`, and the driver script's output to `$LAB_HOME/logs/ray/run-<run_id>.log`. Leave Ray's temp dir at `/tmp/ray`; if the runner moves it (`--temp-dir`), export `PIPEDPEER_LAB_RAY_TMP=<dir>` before `node.sh start`.
   - Dask: `dask scheduler ... >> $LAB_HOME/logs/dask/scheduler.log 2>&1`, `dask worker ... >> $LAB_HOME/logs/dask/worker.log 2>&1`, the client's output to `$LAB_HOME/logs/dask/run-<run_id>.log`.
4. **Ray flags**, on the head and every worker (fixed ports, so `net/sockets` sees Ray's traffic and the metrics agent is found):
   `ray start ... --metrics-export-port=8090 --node-manager-port=6380 --object-manager-port=6381 --dashboard-agent-listen-port=6382 --dashboard-agent-grpc-port=6383 --runtime-env-agent-port=6384 --min-worker-port=10002 --max-worker-port=10999`
   Head: `--head --port=6379 --num-cpus=0`. Another metrics port: `PIPEDPEER_LAB_RAY_METRICS_PORT=<p>` before `node.sh start`.
5. **Dask flags**: scheduler `dask scheduler --port 8786 --dashboard-address :8787` (8787 is the default; pp-poll.py asks the scheduler on it for the workers' metrics ports). Workers `dask worker tcp://<pc00>:8786 --nworkers $(nproc) --nthreads 1 --worker-port 9000:9099 --nanny-port 9100:9199`. Leave each worker's dashboard on its default random port; pp-poll.py discovers it.
6. **pcap fetch**, after `node.sh stop` (or `node.sh pcap-stop`) at the end of a session, before `sync-r2.sh`:
   `ansible all -m ansible.posix.synchronize -a "mode=pull src=~/.local/share/pipedpeer-lab/pcap/ dest=results/lab/pcap/{{ inventory_hostname }}/"`
   (or `-m fetch` per file). `sync-r2.sh` uploads `results/lab/pcap/`; git ignores it. Remove `$LAB_HOME/pcap` in cleanup.
7. **Cleanup** (lab-campaign.md section 4): `node.sh stop`, then `rm -rf ~/.local/share/pipedpeer-lab` removes collector, poller state, logs, pcaps and the tcpdump link (its store path goes with the env's garbage collection).

Nothing else needs the runner: scrape targets, socket ports and the process filter are fixed here.

## Joining a run

`results/lab/runs.jsonl` (also in the driver's logs as `service.name=lab-runner`) has, per run, `run_id`, `start_ns`, `end_ns` and `hosts`. A record belongs to run R when its `lab.pc_id` is one of R's hosts and its time is in `[start_ns, end_ns]`.

- `lab.run_id` is set on pp-poll.py records (`pp-poll`, `net`, `node-state`) and on `pipedpeer-run`. Prefer it there.
- Host metrics, process metrics, prometheus and Ray/Dask/daemon logs carry no run id: the collector has no way to read a file per record. Join them on time. Their timestamps are the PC's clock at collection, the same clock pp-poll.py uses. `clocks.sh` and `node-state/clock` give the offset to the driver, and the plan keeps it under 10 ms. At 1 s resolution, the join is exact to one sample at each end of a run.
- `otelread.py` reads the files: `otelread.points(dir)` and `otelread.records(dir)` yield flat dicts with the resource attributes merged in.

## Volume

Measured on the container lab (3 PCs that see 20 cores each, Dask up with 2 workers per worker PC, Ray stopped), over 300 s:

| | per PC |
|---|---|
| Uncompressed OTLP JSON | 445-520 MiB/h (process metrics 57 %, disk 9 %, net.jsonl 8 %, network 5 %, cpu 5 %, Dask metrics 5 %, pp-poll 4 %) |
| On the driver's disk (zstd frames) | about 20 MiB/h (metrics ×34, logs ×8) |
| In R2 (gzip objects) | about 18 MiB/h (metrics ×34, logs ×12) |
| pcap, idle cluster plus small runs | about 4.5 MiB/h |

The minimal campaign is 17 PCs for about 3 h, so about 1 GiB on the driver and in R2, plus about 0.25 GiB of pcap. The full plan is 33 PCs over 8.5 h: about 5.6 GiB, plus up to 60 PCs during E11. Both fit R2's free 10 GB and any driver disk, whether the driver is the laptop or pc00 (with no laptop, pc00 runs `telemetry.sh` and holds results/lab). Neither disk nor R2 is at risk. Requests stay small too: the R2 batch is up to 200k items or 60 s, about 3 PUTs a minute, against a free 1M class A operations a month. Things that scale the number:

- Process metrics grow with the number of matching processes: Dask runs one process per core and Ray one worker per core, so a 16-core PC running either adds about 16 × 18 datapoints a second. If that ever threatens R2, drop `host.process` to 5 s first (a second `host_metrics/proc` receiver at 5 s), then turn off `process.context_switches` and `process.paging.faults`.
- Per-core cpu metrics grow with cores (×2 states).
- pcap grows with PipedPeer's own data traffic (pool items, closures); 96-byte headers keep it to about 100 bytes per packet.
