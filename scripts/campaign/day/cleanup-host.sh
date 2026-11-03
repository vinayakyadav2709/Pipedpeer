#!/usr/bin/env bash
# cleanup-host.sh <had-nix 0|1> <had-pipedpeer 0|1> [keep-day]: remove what
# the lab day put on this PC, as the lab user, before lab.sh undo (which then
# removes the firewall rules, packages, key, sudo rule and home leftovers).
# lab-day.sh copies it to each worker through Ansible and runs it on pc00
# directly. Idempotent. The last line is "CLEANUP ok" or "CLEANUP left: ...".
set -uo pipefail
had_nix=${1:-1} had_pp=${2:-0} keep_day=${3:-}
L=$HOME/.local/share/pipedpeer-lab
C=$HOME/pipedpeer-lab/repo/scripts/campaign
left=()
export PATH=$HOME/.local/bin:/usr/local/bin:/nix/var/nix/profiles/default/bin:$HOME/.nix-profile/bin:$PATH

# Ray and Dask (run.sh starts them; a cut-off day can leave them up).
[[ -x $L/env/bin/ray ]] && timeout 60 "$L/env/bin/ray" stop --force >/dev/null 2>&1
pkill -u "$USER" -f 'dask (scheduler|worker)|distributed\.cli\.dask_|raylet|gcs_server|torchrun|torch\.distributed' 2>/dev/null
pkill -u "$USER" -x iperf3 2>/dev/null

# Telemetry: the node collector (and the driver collector on pc00).
[[ -f $C/telemetry.sh ]] && bash "$C/telemetry.sh" down >/dev/null 2>&1
if [[ -f $C/otel/install-otelcol.sh ]]; then
  bash "$C/otel/install-otelcol.sh" remove >/dev/null 2>&1
else
  [[ -f $L/otel/node.pid ]] && kill "$(cat "$L/otel/node.pid")" 2>/dev/null
fi
pkill -u "$USER" -x otelcol-contrib 2>/dev/null

# The campaign env: its out-link, and the store paths only it kept.
[[ -f $C/env/install.sh ]] && bash "$C/env/install.sh" remove >/dev/null 2>&1

# pipedpeer (the daemon, its unit, its data), unless the PC had it before.
if command -v pipedpeer >/dev/null && [[ $had_pp != 1 ]]; then
  pipedpeer stop >/dev/null 2>&1
  pipedpeer uninstall -y >/dev/null 2>&1 || left+=("pipedpeer uninstall failed")
fi

# Nix, only when the day's pipedpeer install put it there.
if [[ $had_nix != 1 && -e /nix ]]; then
  if [[ -x /nix/nix-installer ]]; then
    sudo -n /nix/nix-installer uninstall --no-confirm >/dev/null 2>&1 || left+=("nix uninstall failed")
  else
    left+=("/nix exists without /nix/nix-installer; remove it by hand")
  fi
fi

# E15's netem qdisc: experiments/e15.py names the interface in $L/netem.dev
# (and removes it itself; this is for a cut-off day). Only that interface.
if [[ -f $L/netem.dev ]]; then
  sudo -n tc qdisc del dev "$(cat "$L/netem.dev")" root 2>/dev/null
  tc qdisc show dev "$(cat "$L/netem.dev")" 2>/dev/null | grep -q netem && left+=("netem qdisc still on $(cat "$L/netem.dev")")
fi

# Temp dirs Ray, Dask and torchrun make, owned by this user only.
find /tmp -maxdepth 1 -user "$USER" \( -name ray -o -name 'dask-*' -o -name 'dask-scratch-space' \
  -o -name 'torchelastic_*' -o -name 'pipedpeer-lab*' \) -exec rm -rf {} + 2>/dev/null

rm -rf "$L"
[[ $keep_day == keep-day ]] || rm -rf "$HOME/pipedpeer-lab"

# What must be gone now (forget the paths bash remembered).
hash -r
command -v pipedpeer >/dev/null && [[ $had_pp != 1 ]] && left+=("pipedpeer still on PATH ($(command -v pipedpeer))")
pgrep -u "$USER" -x otelcol-contrib >/dev/null && left+=("otelcol-contrib still running")
pgrep -u "$USER" -f 'raylet|dask (scheduler|worker)' >/dev/null && left+=("ray or dask still running")
[[ $had_nix != 1 && -e /nix ]] && left+=("/nix still there")
[[ -e $L ]] && left+=("$L still there")
if ((${#left[@]})); then
  printf 'CLEANUP left: %s\n' "$(IFS=';'; echo "${left[*]}")"
  exit 1
fi
echo "CLEANUP ok"
