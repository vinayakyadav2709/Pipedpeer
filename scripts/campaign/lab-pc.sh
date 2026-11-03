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
