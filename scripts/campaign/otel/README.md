# Campaign telemetry

Plan: [docs/benchmarks/lab-campaign.md](../../../docs/benchmarks/lab-campaign.md), section 1.5.

```
lab PC: node.sh ──OTLP/gRPC :4317──▶ driver: telemetry.sh ──▶ results/lab/otel/{metrics,logs}.jsonl
  host_metrics 1 s (cpu, memory,              file_log results/lab/runs.jsonl   └▶ awss3 → R2 pipedpeer-lab/otel/
  network, load), daemon log,                                                   (only with r2.env)
  lab.pc_id=<pcNN>
sync-r2.sh: rclone sync results/lab → r2:pipedpeer-lab/raw (not otel/, serve/)
clocks.sh: offset of each PC from the driver → results/lab/clocks.txt
```

Everything installed lives under `~/.local/share/pipedpeer-lab/` (binaries in `bin/`, collector config, pid, log and read offsets in `otel/`); no root. `install-otelcol.sh remove` undoes a node.

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
scripts/campaign/otel/node.sh start pc07 <driver-ip>        # on each PC
scripts/campaign/telemetry.sh check                         # datapoints and log records per lab.pc_id, last 60 s; R2 objects
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

For the container lab (`LAB_NAME=otel scripts/lab-up.sh`, rootless podman), containers reach the driver at `host.containers.internal`; copy this directory in with `docker cp` and run `node.sh start pc0N host.containers.internal`. The nix image has no `sed` or `setsid`; `node.sh` needs neither.
