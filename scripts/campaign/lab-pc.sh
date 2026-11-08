#!/usr/bin/env bash
# Lab PC setup for the PipedPeer campaign. Run as root:
#   wget -qO /tmp/lab-pc.sh http://DRIVER:8000/lab-pc.sh &&
#     echo "<sha256>  /tmp/lab-pc.sh" | sha256sum -c && sudo bash /tmp/lab-pc.sh setup
#   sudo bash /tmp/lab-pc.sh ports    # re-check firewall ports (every session)
#   sudo systemd-run --unit=lab-undo bash /tmp/lab-pc.sh undo   # last session
# Ubuntu only (apt, ufw, systemd). serve.sh replaces the two placeholders below
# and prints the exact setup line with this file's sha256.
set -euo pipefail
DRIVER="__DRIVER_IP__"
LAB_CIDR="__LAB_CIDR__"
LAB_USER="${SUDO_USER:-$(logname 2>/dev/null || echo root)}"
HOME_DIR=$(getent passwd "$LAB_USER" | cut -d: -f6)
STATE=/var/lib/pipedpeer-lab/state
LINE_FILE=/var/lib/pipedpeer-lab/lab-pc.line
MARK="# pipedpeer-lab"
SUDOERS=/etc/sudoers.d/pipedpeer-lab

log()  { echo "[lab-pc] $*"; }
die()  { echo "[lab-pc] ERROR: $*" >&2; exit 1; }
record() { mkdir -p "$(dirname "$STATE")"; grep -qxF "$1" "$STATE" 2>/dev/null || echo "$1" >>"$STATE"; }

[[ $EUID -eq 0 ]] || die "run with sudo"

command -v apt-get >/dev/null 2>&1 || die "this script is for Ubuntu (apt) only"
[[ -n $HOME_DIR ]] || die "no home directory for $LAB_USER"

# Package names installed and running, to record only what apt actually added.
installed_pkgs() { dpkg --get-selections | awk '$2 == "install" {print $1}' | sort; }

# Install only what is missing, and record the full set apt added, so undo
# purges exactly that (removal alone would leave the lab's step-8 diff dirty).
pkg() {
  local p before after n
  DEBIAN_FRONTEND=noninteractive apt-get update -qq || die "apt-get update failed"
  for p in "$@"; do
    if dpkg -s "$p" 2>/dev/null | grep -q '^Status: install ok installed'; then continue; fi
    before=$(installed_pkgs)
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "$p" || die "cannot install $p"
    after=$(installed_pkgs)
    while IFS= read -r n; do record "pkg $n"; done \
      < <(comm -13 <(printf '%s\n' "$before") <(printf '%s\n' "$after"))
  done
}

# Ports: 22/tcp, 4242/tcp, 4242-4244/udp, and the whole lab subnet (Ray,
# Dask, torchrun and iperf3 use ports this list lacks; Ray picks random worker
# ports). ufw rules are permanent; an inactive ufw (the Ubuntu default) blocks
# nothing, so nothing is added. Safe to rerun every session; record only rules
# this script added, so undo never removes the lab's own.
ports() {
  command -v ufw >/dev/null 2>&1 || return 0
  ufw status 2>/dev/null | grep -q 'Status: active' || return 0
  local p
  for p in 22/tcp 4242/tcp 4242/udp 4243/udp 4244/udp; do
    ufw status | grep -qE "^$p +ALLOW" && continue
    if ufw allow "$p" >/dev/null; then record "ufw $p"; else log "WARNING: ufw allow $p failed"; fi
  done
  if ! ufw status | awk -v c="$LAB_CIDR" '$0 ~ /ALLOW/ && $NF == c {f = 1} END {exit !f}'; then
    if ufw allow from "$LAB_CIDR" >/dev/null; then record "ufw-from $LAB_CIDR"
    else log "WARNING: ufw allow from $LAB_CIDR failed"; fi
  fi
}

# gsettings as the lab user on their session bus (root's own dconf is useless).
gs() {
  local uid; uid=$(id -u "$LAB_USER")
  sudo -u "$LAB_USER" env "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/$uid/bus" gsettings "$@"
}

gset_try() {
  local schema=$1 key=$2 value=$3 old
  old=$(gs get "$schema" "$key" 2>/dev/null) || return 0
  grep -qF "gset $schema $key " "$STATE" 2>/dev/null || record "gset $schema $key $old"
  gs set "$schema" "$key" "$value" || log "WARNING: gsettings $schema $key not set"
}

# No suspend for the whole campaign; screen lock and idle sleep off for the lab
# user's desktop (GNOME; other desktops: log a warning, the mask suffices).
nosleep() {
  local t uid
  for t in sleep.target suspend.target hibernate.target hybrid-sleep.target; do
    if [[ $(systemctl is-enabled "$t" 2>/dev/null) == masked ]]; then continue; fi
    if systemctl mask "$t" >/dev/null 2>&1; then record "mask $t"; else log "WARNING: $t not masked"; fi
  done
  uid=$(id -u "$LAB_USER")
  if [[ -S /run/user/$uid/bus ]] && command -v gsettings >/dev/null 2>&1; then
    gset_try org.gnome.desktop.screensaver lock-enabled "false"
    gset_try org.gnome.desktop.session idle-delay "uint32 0"
    gset_try org.gnome.settings-daemon.plugins.power sleep-inactive-ac-type "'nothing'"
  else
    log "WARNING: no GNOME session for $LAB_USER; screen lock not changed (suspend is masked)"
  fi
}

# Detached runs (`systemd-run --user`) must survive an ssh drop.
linger() {
  if [[ $(loginctl show-user "$LAB_USER" --property=Linger --value 2>/dev/null) == yes ]]; then return 0; fi
  if loginctl enable-linger "$LAB_USER" 2>/dev/null; then
    record linger
  else
    log "WARNING: cannot enable lingering for $LAB_USER; detached runs may not survive an ssh drop"
  fi
}

setup() {
  pkg openssh-server sysstat curl iperf3 parallel   # sysstat: `sar`; curl: the product installer

  # Ubuntu 24.04 starts sshd by socket activation, 22.04 as a service.
  if ! { systemctl is-active --quiet ssh.service || systemctl is-active --quiet ssh.socket; }; then
    if systemctl enable --now ssh.service >/dev/null 2>&1 || systemctl enable --now ssh.socket >/dev/null 2>&1; then
      record "started ssh"
    else
      die "cannot start ssh"
    fi
  fi

  # Driver key for the lab user (append only the exact marked line; the same
  # key may already be there unmarked from an earlier setup).
  local key
  key=$(curl -fsS "http://$DRIVER:8000/driver.pub") || die "cannot fetch driver.pub from $DRIVER"
  [[ -n $key ]] || die "driver.pub from $DRIVER is empty"
  install -d -m 700 -o "$LAB_USER" "$HOME_DIR/.ssh"
  touch "$HOME_DIR/.ssh/authorized_keys"
  grep -qxF "$key $MARK" "$HOME_DIR/.ssh/authorized_keys" \
    || echo "$key $MARK" >>"$HOME_DIR/.ssh/authorized_keys"
  chown "$LAB_USER" "$HOME_DIR/.ssh/authorized_keys"
  chmod 600 "$HOME_DIR/.ssh/authorized_keys"

  # Same key for root: scan.sh reads the LAB-PC line as root on an unknown user.
  install -d -m 700 /root/.ssh
  touch /root/.ssh/authorized_keys
  grep -qxF "$key $MARK" /root/.ssh/authorized_keys || echo "$key $MARK" >>/root/.ssh/authorized_keys
  chmod 600 /root/.ssh/authorized_keys
  record rootkey

  # Passwordless sudo for the campaign only. Broad on purpose (installers, nix,
  # firewall, cgroups); removed by `undo`, which the last session runs on every PC.
  local tmp=$SUDOERS.tmp
  if [[ ! -f $SUDOERS ]]; then
    echo "$LAB_USER ALL=(ALL) NOPASSWD: ALL" >"$tmp"
    if command -v visudo >/dev/null 2>&1 && ! visudo -cf "$tmp" >/dev/null; then
      rm -f "$tmp"; die "invalid sudoers entry for $LAB_USER"
    fi
    chmod 440 "$tmp"
    mv "$tmp" "$SUDOERS"
    record sudoers
  fi

  linger
  ports
  nosleep

  local ip line
  ip=$(ip -4 route get "$DRIVER" | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1);exit}}')
  [[ -n $ip ]] || die "cannot find the local IP towards $DRIVER"
  line="LAB-PC host=$(hostname) ip=$ip user=$LAB_USER cores=$(nproc) ram_gib=$(awk '/MemTotal/{printf "%.0f",$2/1048576}' /proc/meminfo)"
  echo "$line"
  install -d /var/lib/pipedpeer-lab
  echo "$line" >"$LINE_FILE"
}

stop_service() {
  local u=$1
  systemctl disable --now "$u.service" 2>/dev/null || log "WARNING: $u.service not stopped"
  if systemctl list-unit-files --no-legend "$u.socket" 2>/dev/null | grep -q .; then
    systemctl disable --now "$u.socket" 2>/dev/null || log "WARNING: $u.socket not stopped"
  fi
}

undo_gset() {
  local schema=$1 key=$2 uid; shift 2
  uid=$(id -u "$LAB_USER")
  if [[ -S /run/user/$uid/bus ]] && command -v gsettings >/dev/null 2>&1; then
    gs set "$schema" "$key" "$*" \
      || log "WARNING: gsettings $schema $key not restored (log in as $LAB_USER and rerun undo)"
  else
    log "WARNING: no GNOME session for $LAB_USER; gsettings $schema $key not restored (log in and rerun undo)"
  fi
}

# Run over SSH, start it detached so stopping sshd cannot cut it off halfway:
#   sudo systemd-run --unit=lab-undo bash /tmp/lab-pc.sh undo
# then read `journalctl -u lab-undo` (or the PC's screen) for "undo complete".
undo() {
  [[ -f $STATE ]] || { log "nothing recorded"; return 0; }
  local l f pkgs=()
  # Read STATE on fd 3, newest first, so a rule that depends on another is
  # removed after it; apt/ufw cannot swallow the list from stdin.
  while IFS= read -r l <&3; do
    case "$l" in
      "ufw-from "*) ufw delete allow from "${l#ufw-from }" >/dev/null </dev/null \
                      || log "WARNING: ufw from ${l#ufw-from } not removed" ;;
      "ufw "*)      ufw delete allow "${l#ufw }" >/dev/null </dev/null \
                      || log "WARNING: ufw ${l#ufw } not removed" ;;
      "started "*)  stop_service "${l#started }" ;;
      sudoers)      rm -f "$SUDOERS" ;;
      "mask "*)     systemctl unmask "${l#mask }" >/dev/null \
                      || log "WARNING: ${l#mask } not unmasked" ;;
      "gset "*)     read -r -a f <<<"${l#gset }"
                    if [[ ${#f[@]} -ge 3 ]]; then undo_gset "${f[0]}" "${f[1]}" "${f[@]:2}"
                    else log "WARNING: malformed state line, left as is: $l"; fi ;;
      linger)       loginctl disable-linger "$LAB_USER" \
                      || log "WARNING: lingering for $LAB_USER not disabled" ;;
      rootkey)      if [[ -f /root/.ssh/authorized_keys ]]; then
                      sed -i "\| $MARK\$|d" /root/.ssh/authorized_keys \
                        || log "WARNING: driver key not removed from /root/.ssh/authorized_keys"
                    fi ;;
      "pkg "*)      pkgs+=("${l#pkg }") ;;
      *)            log "WARNING: unknown state line, left as is: $l" ;;
    esac
  done 3< <(tac "$STATE")

  # One purge for everything apt added, after ssh is stopped. No autoremove: it
  # would take the lab's own orphans with it.
  if [[ ${#pkgs[@]} -gt 0 ]]; then
    DEBIAN_FRONTEND=noninteractive apt-get purge -y -qq "${pkgs[@]}" </dev/null \
      || log "WARNING: not all packages purged: ${pkgs[*]}"
  fi

  if [[ -f $HOME_DIR/.ssh/authorized_keys ]]; then
    sed -i "\| $MARK\$|d" "$HOME_DIR/.ssh/authorized_keys" \
      || log "WARNING: driver key not removed from $HOME_DIR/.ssh/authorized_keys"
  fi
  rm -rf "$(dirname "$STATE")"
  log "undo complete"
}

case "${1:-}" in
  setup) setup ;;
  ports) ports ;;
  undo)  undo ;;
  *) die "usage: lab-pc.sh setup|ports|undo" ;;
esac
