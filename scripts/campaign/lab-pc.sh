#!/usr/bin/env bash
# Lab PC setup for the PipedPeer campaign. Run as root:
#   wget -qO /tmp/lab-pc.sh http://DRIVER:8000/lab-pc.sh &&
#     echo "<sha256>  /tmp/lab-pc.sh" | sha256sum -c && sudo bash /tmp/lab-pc.sh setup
#   sudo bash /var/lib/pipedpeer-lab/lab-pc.sh ports    # re-check firewall ports (every session)
#   sudo systemd-run --unit=lab-undo bash /var/lib/pipedpeer-lab/lab-pc.sh undo   # last session
# (setup keeps its own copy there: /tmp/lab-pc.sh is gone after a reboot)
# Ubuntu only (apt, ufw, systemd). serve.sh replaces the two placeholders below
# and prints the exact setup line with this file's sha256.
set -euo pipefail
DRIVER="__DRIVER_IP__"
LAB_CIDR="__LAB_CIDR__"
LAB_USER="${SUDO_USER:-$(logname 2>/dev/null || echo root)}"
HOME_DIR=$(getent passwd "$LAB_USER" | cut -d: -f6)
STATE=/var/lib/pipedpeer-lab/state
LINE_FILE=/var/lib/pipedpeer-lab/lab-pc.line
SELF=/var/lib/pipedpeer-lab/lab-pc.sh   # setup's copy of this script: install.sh runs `mark` from it
MARK="# pipedpeer-lab"
SUDOERS=/etc/sudoers.d/pipedpeer-lab
NIX_DIR=/nix
RUN_USER_DIR=/run/user

log()  { echo "[lab-pc] $*"; }
die()  { echo "[lab-pc] ERROR: $*" >&2; exit 1; }
record() { mkdir -p "$(dirname "$STATE")"; grep -qxF "$1" "$STATE" 2>/dev/null || echo "$1" >>"$STATE"; }

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

# Who installed Nix. Only when setup found none: a Nix that appears later was
# brought by the campaign's install and is recorded with the installer kind,
# once. Run by `ports` (every session) and `mark`, after the install step; a
# Nix installed by someone else between setup and undo is not recorded and so
# never removed.
mark_nix() {
  grep -qxF nix-absent "$STATE" 2>/dev/null || return 0
  grep -q '^nix-campaign ' "$STATE" 2>/dev/null && return 0
  [[ -e $NIX_DIR ]] || return 0
  if [[ -x $NIX_DIR/nix-installer ]]; then record "nix-campaign determinate"
  else record "nix-campaign stock"; fi
}

setup() {
  # Nix: if the campaign's installs bring it, undo removes it; if it was here
  # before, undo leaves it. Absence is recorded once, the first time setup
  # runs; mark_nix records who installed it once it appears.
  if [[ ! -e $NIX_DIR ]] && ! grep -qxF nix-absent "$STATE" 2>/dev/null; then record nix-absent; fi
  pkg openssh-server sysstat curl iperf3 parallel bzip2 rsync   # bzip2: GNU parallel --sshlogin execs it (absent on minimal images); sysstat: `sar`; curl: the product installer; rsync: every fetch and stage

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
  mark_nix
  nosleep

  local ip line
  ip=$(ip -4 route get "$DRIVER" | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1);exit}}')
  [[ -n $ip ]] || die "cannot find the local IP towards $DRIVER"
  line="LAB-PC host=$(hostname) ip=$ip user=$LAB_USER cores=$(nproc) ram_gib=$(awk '/MemTotal/{printf "%.0f",$2/1048576}' /proc/meminfo)"
  echo "$line"
  install -d /var/lib/pipedpeer-lab
  echo "$line" >"$LINE_FILE"
  # /tmp/lab-pc.sh can vanish on a reboot; the state dir's copy is removed with
  # the state, and kept while a failed undo keeps it.
  if [[ ! $SELF -ef ${BASH_SOURCE[0]} ]]; then install -m 0700 -o root "${BASH_SOURCE[0]}" "$SELF"; fi
}

stop_service() {
  local u=$1 rc=0
  systemctl disable --now "$u.service" 2>/dev/null || { log "WARNING: $u.service not stopped"; rc=1; }
  if systemctl list-unit-files --no-legend "$u.socket" 2>/dev/null | grep -q .; then
    systemctl disable --now "$u.socket" 2>/dev/null || { log "WARNING: $u.socket not stopped"; rc=1; }
  fi
  return $rc
}

undo_gset() {
  local schema=$1 key=$2 uid; shift 2
  uid=$(id -u "$LAB_USER")
  if [[ -S /run/user/$uid/bus ]] && command -v gsettings >/dev/null 2>&1; then
    gs set "$schema" "$key" "$*" \
      || { log "WARNING: gsettings $schema $key not restored (log in as $LAB_USER and rerun undo)"; return 1; }
  else
    log "WARNING: no GNOME session for $LAB_USER; gsettings $schema $key not restored (log in and rerun undo)"
    return 1
  fi
}

# A failed undo step: say so, and remember it so the state is kept for a rerun.
warn() { log "WARNING: $*"; }

# systemctl --user as the lab user, on their runtime dir.
user_sc() {
  local uid; uid=$(id -u "$LAB_USER")
  sudo -u "$LAB_USER" env "XDG_RUNTIME_DIR=$RUN_USER_DIR/$uid" \
    "DBUS_SESSION_BUS_ADDRESS=unix:path=$RUN_USER_DIR/$uid/bus" systemctl --user "$@"
}

# What the campaign left running or set on the PC, before anything under the
# venv goes: the lab-* units (ray, dask, detached runs), strays started outside
# a unit, and the AllowedCPUs cap. Nothing there, nothing done. Returns 1 when
# a step failed.
stop_lab_runtime() {
  local uid u rc=0 units cap
  uid=$(id -u "$LAB_USER")
  if [[ -d $RUN_USER_DIR/$uid ]]; then
    units=$(user_sc list-units --no-legend --plain 'lab-*' 2>/dev/null | awk '{print $1}') || units=""
    for u in $units; do
      log "stopping $u"
      user_sc stop "$u" </dev/null || { warn "unit $u not stopped"; rc=1; }
    done
  fi
  # Processes of the lab venv or work dir that no unit owns (nohup fallback).
  if pgrep -u "$LAB_USER" -f "$HOME_DIR/pipedpeer-lab/" >/dev/null 2>&1; then
    log "stopping leftover processes under $HOME_DIR/pipedpeer-lab"
    pkill -u "$LAB_USER" -f "$HOME_DIR/pipedpeer-lab/" || warn "leftover processes not stopped"
    sleep 1
    if pgrep -u "$LAB_USER" -f "$HOME_DIR/pipedpeer-lab/" >/dev/null 2>&1; then
      warn "processes still running under $HOME_DIR/pipedpeer-lab"; rc=1
    fi
  fi
  cap=$(systemctl show "user-$uid.slice" --property=AllowedCPUs --value 2>/dev/null || true)
  if [[ -n $cap ]]; then
    log "resetting the AllowedCPUs cap ($cap) on user-$uid.slice"
    systemctl set-property --runtime "user-$uid.slice" AllowedCPUs= </dev/null \
      || { warn "AllowedCPUs cap on user-$uid.slice not reset"; rc=1; }
  fi
  return $rc
}

# Run over SSH, start it detached so stopping sshd cannot cut it off halfway:
#   sudo systemd-run --unit=lab-undo bash /var/lib/pipedpeer-lab/lab-pc.sh undo
# then read `journalctl -u lab-undo` (or the PC's screen) for "undo complete".
# A step that fails keeps its state line (and "cleanup" for the file work), so
# a rerun retries exactly that; the state is removed only when all went through.
undo() {
  [[ -f $STATE ]] || { log "nothing recorded"; return 0; }
  local l f pkgs=() u nix_absent=0 nix_kind="" keep=() k
  # Started by "sudo systemd-run" there is no SUDO_USER, so LAB_USER above is
  # root; the user setup acted for is on the line it saved.
  if [[ -f $LINE_FILE ]]; then
    u=$(sed -n 's/.* user=\([^ ]*\).*/\1/p' "$LINE_FILE" | head -n1)
    if [[ -n $u ]] && getent passwd "$u" >/dev/null; then
      LAB_USER=$u
      HOME_DIR=$(getent passwd "$LAB_USER" | cut -d: -f6)
    else
      warn "cannot read the lab user from $LINE_FILE; acting for $LAB_USER"
    fi
  fi
  stop_lab_runtime || keep+=(cleanup)
  # Read STATE on fd 3, newest first, so a rule that depends on another is
  # removed after it; apt/ufw cannot swallow the list from stdin.
  while IFS= read -r l <&3; do
    case "$l" in
      "ufw-from "*) ufw delete allow from "${l#ufw-from }" >/dev/null </dev/null \
                      || { warn "ufw from ${l#ufw-from } not removed"; keep+=("$l"); } ;;
      "ufw "*)      ufw delete allow "${l#ufw }" >/dev/null </dev/null \
                      || { warn "ufw ${l#ufw } not removed"; keep+=("$l"); } ;;
      "started "*)  stop_service "${l#started }" || keep+=("$l") ;;
      sudoers)      rm -f "$SUDOERS" || { warn "$SUDOERS not removed"; keep+=("$l"); } ;;
      "mask "*)     systemctl unmask "${l#mask }" >/dev/null \
                      || { warn "${l#mask } not unmasked"; keep+=("$l"); } ;;
      "gset "*)     read -r -a f <<<"${l#gset }"
                    if [[ ${#f[@]} -ge 3 ]]; then undo_gset "${f[0]}" "${f[1]}" "${f[@]:2}" || keep+=("$l")
                    else warn "malformed state line, left as is: $l"; keep+=("$l"); fi ;;
      linger)       loginctl disable-linger "$LAB_USER" \
                      || { warn "lingering for $LAB_USER not disabled"; keep+=("$l"); } ;;
      rootkey)      if [[ -f /root/.ssh/authorized_keys ]]; then
                      sed -i "\| $MARK\$|d" /root/.ssh/authorized_keys \
                        || { warn "driver key not removed from /root/.ssh/authorized_keys"; keep+=("$l"); }
                    fi ;;
      nix-absent)   nix_absent=1 ;;
      "nix-campaign "*) nix_kind=${l#nix-campaign } ;;
      cleanup)      ;;
      "pkg "*)      pkgs+=("${l#pkg }") ;;
      *)            warn "unknown state line, left as is: $l"; keep+=("$l") ;;
    esac
  done 3< <(tac "$STATE")

  # One purge for everything apt added, after ssh is stopped. No autoremove: it
  # would take the lab's own orphans with it.
  if [[ ${#pkgs[@]} -gt 0 ]]; then
    # Where policy-rc.d forbids service control (containers) the purge leaves a
    # running sshd behind; stopping it first is the same on a real PC.
    for f in "${pkgs[@]}"; do
      if [[ $f == openssh-server ]]; then
        systemctl disable --now ssh.socket ssh.service >/dev/null 2>&1 \
          || warn "ssh not stopped before the purge"
      fi
    done
    if ! DEBIAN_FRONTEND=noninteractive apt-get purge -y -qq "${pkgs[@]}" </dev/null; then
      warn "not all packages purged: ${pkgs[*]}"
      for f in "${pkgs[@]}"; do keep+=("pkg $f"); done
    fi
  fi

  if [[ -f $HOME_DIR/.ssh/authorized_keys ]]; then
    sed -i "\| $MARK\$|d" "$HOME_DIR/.ssh/authorized_keys" \
      || { warn "driver key not removed from $HOME_DIR/.ssh/authorized_keys"; keep+=(cleanup); }
  fi
  # What the campaign's install and cluster steps left in the lab user's home.
  rm -rf "$HOME_DIR/pipedpeer-lab" \
    || { warn "$HOME_DIR/pipedpeer-lab not removed"; keep+=(cleanup); }
  rm -f "$HOME_DIR"/.ssh/pipedpeer-lab-pc00* \
    || { warn "$HOME_DIR/.ssh/pipedpeer-lab-pc00 key not removed"; keep+=(cleanup); }
  # known_hosts entries pc00's ssh added (accept-new) name PCs of the lab and
  # are harmless; they are not removed.
  # Nix only where setup found none and the campaign's own install was recorded
  # (mark_nix): a Nix someone else installed meanwhile stays.
  if [[ $nix_absent == 1 && -e $NIX_DIR ]]; then
    case "$nix_kind" in
      determinate)
        if [[ -x $NIX_DIR/nix-installer ]]; then
          "$NIX_DIR/nix-installer" uninstall --no-confirm </dev/null \
            || { warn "Nix not removed; run $NIX_DIR/nix-installer uninstall by hand, then rerun undo"
                 keep+=(nix-absent "nix-campaign determinate"); }
        else
          warn "Nix was recorded as installed by the campaign but $NIX_DIR/nix-installer is gone; left in place, remove it by hand"
        fi ;;
      stock)
        warn "Nix left in place (stock installer, no $NIX_DIR/nix-installer): remove it by hand with the steps for a multi-user install at https://nix.dev/manual/nix/stable/installation/uninstall" ;;
      *)
        warn "Nix left in place: it was not recorded as installed by the campaign (run 'lab-pc.sh mark' after the install step)" ;;
    esac
  fi
  if [[ ${#keep[@]} -gt 0 ]]; then
    : >"$STATE.new"
    # keep is newest first (the loop read tac); the file is oldest first.
    for ((k = ${#keep[@]} - 1; k >= 0; k--)); do printf '%s\n' "${keep[k]}" >>"$STATE.new"; done
    mv "$STATE.new" "$STATE"
    warn "undo incomplete: $STATE kept with the steps to retry; rerun undo"
    return 1
  fi
  rm -rf "$(dirname "$STATE")"
  log "undo complete"
}

# Sourced by the case-table test: functions only.
if [[ ${BASH_SOURCE[0]} != "$0" ]]; then return 0; fi

[[ $EUID -eq 0 ]] || die "run with sudo"
command -v apt-get >/dev/null 2>&1 || die "this script is for Ubuntu (apt) only"
[[ -n $HOME_DIR ]] || die "no home directory for $LAB_USER"

case "${1:-}" in
  setup) setup ;;
  ports) ports; mark_nix ;;
  mark)  mark_nix ;;
  undo)  undo ;;
  *) die "usage: lab-pc.sh setup|ports|mark|undo" ;;
esac
