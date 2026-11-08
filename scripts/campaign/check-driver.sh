#!/usr/bin/env bash
# Driver readiness check for the lab campaign: every tool the scripts call, the
# lab ssh key, and (unless --no-r2) the r2.env credentials plus a live R2 listing.
# Prints OK <tool> / MISSING: <tool> (<hint>) per check; exits 1 if any is missing.
set -euo pipefail

REPO=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
# lib.sh is shared with the other campaign scripts (S1) and is not needed here.
if [[ -r "$REPO/scripts/campaign/lib.sh" ]]; then
  # shellcheck source=/dev/null
  . "$REPO/scripts/campaign/lib.sh"
fi

usage() {
  cat <<'EOF'
usage: check-driver.sh [--e3] [--no-r2]

  --e3      accepted for the E3 step; E3 is a paper sheet (results/lab/e3-sheet.md),
            written in R2, so no extra machine check runs here.
  --no-r2   skip the r2.env and rclone listing checks.
EOF
}

no_r2=0
for arg in "$@"; do
  case $arg in
    --e3) printf 'NOTE: --e3: E3 is a paper sheet (results/lab/e3-sheet.md), no extra check\n' >&2 ;;
    --no-r2) no_r2=1 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
done

miss=0
ok() { printf 'OK %s\n' "$*"; }
missing() { printf 'MISSING: %s\n' "$*"; miss=1; }
have() { command -v "$1" >/dev/null 2>&1; }

# Fedora and Ubuntu hints differ; pick by whichever package manager is on the driver.
hint() {
  if have dnf; then
    printf 'sudo dnf install -y %s' "$1"
  else
    printf 'sudo apt install -y %s' "$1"
  fi
}

check_tool() {
  local tool=$1 pkg=${2:-$1}
  if have "$tool"; then ok "$tool"; else missing "$tool ($(hint "$pkg"))"; fi
}

# pssh is `parallel-ssh` on Ubuntu and `pssh` on Fedora.
if have pssh; then
  ok pssh
elif have parallel-ssh; then
  ok parallel-ssh
else
  missing "pssh ($(hint pssh); the Ubuntu binary is parallel-ssh)"
fi

check_tool parallel
check_tool rsync
check_tool rclone
check_tool nmap
check_tool iperf3
check_tool ssh openssh-clients

if ! have python3; then
  missing "python3 ($(hint python3))"
elif python3 -c 'import matplotlib, pandas, scipy' >/dev/null 2>&1; then
  ok python3
else
  missing "python3 ($(hint 'python3-matplotlib python3-pandas python3-scipy'))"
fi

check_tool shellcheck

if [[ -f $HOME/.ssh/pipedpeer-lab ]]; then
  ok "$HOME/.ssh/pipedpeer-lab"
else
  missing "$HOME/.ssh/pipedpeer-lab (ssh-keygen -t ed25519 -N '' -f ~/.ssh/pipedpeer-lab)"
fi

if (( ! no_r2 )); then
  r2_env=$HOME/.config/pipedpeer-lab/r2.env
  r2_mode=
  if [[ -f $r2_env ]]; then
    r2_mode=$(stat -c %a "$r2_env")
  fi
  if [[ $r2_mode == 600 ]]; then
    ok "r2.env mode 600"
  else
    missing "r2.env mode 600 ($r2_env must exist with mode 600: chmod 600 $r2_env)"
  fi
  # Never print the file's values: source it in a subshell, discard rclone's output.
  # shellcheck disable=SC1090
  if [[ $r2_mode == 600 ]] && ( set -a; . "$r2_env"; rclone lsd r2:pipedpeer-lab >/dev/null 2>&1 ); then
    ok "rclone r2:pipedpeer-lab"
  else
    missing "rclone r2:pipedpeer-lab (rclone lsd r2:pipedpeer-lab fails; check r2.env)"
  fi
fi

if (( miss )); then
  exit 1
fi
echo DRIVER-OK
