#!/usr/bin/env bash
# Lab PC setup for the PipedPeer campaign (docs/benchmarks/lab-campaign.md,
# Appendix A). Run as the lab user through sudo, never from a root shell:
#   curl -fsS http://DRIVER:PORT/lab-pc.sh | sudo bash -s -- setup [lab-user]
#   sudo bash /var/lib/pipedpeer-lab/lab-pc.sh undo
# (or: curl -fsS http://DRIVER:PORT/lab-pc.sh | sudo bash -s -- undo)
#
# setup opens SSH for the driver's campaign key, gives the lab user passwordless
# sudo, opens the PipedPeer ports, and prints one line starting "LAB-PC".
# Every change it makes is appended to $STATE as it happens, and only when it
# really changed something, so running setup twice records nothing new.
# undo replays $STATE backwards and restores what was there before.
# scripts/campaign/serve.sh fills in DRIVER and PORT.
set -Eeuo pipefail
DRIVER="__DRIVER_IP__"
PORT="__DRIVER_PORT__"
STATE_DIR=/var/lib/pipedpeer-lab
STATE=$STATE_DIR/state
MARK="# pipedpeer-lab"
SUDOERS=/etc/sudoers.d/pipedpeer-lab
PORTS=(22/tcp 4242/tcp 4242/udp 4243/udp 4244/udp)

log()  { echo "[lab-pc] $*"; }
warn() { echo "[lab-pc] WARNING: $*" >&2; }
die()  { echo "[lab-pc] ERROR: $*" >&2; exit 1; }
trap 'echo "[lab-pc] ERROR: line $LINENO: $BASH_COMMAND failed" >&2' ERR

# record appends one state line, once: a rerun of setup must not make undo act twice.
record() {
  mkdir -p "$STATE_DIR"; chmod 700 "$STATE_DIR"
  [[ -f $STATE ]] && grep -qxF -- "$1" "$STATE" && return 0
  printf '%s\n' "$1" >>"$STATE"
}

[[ $EUID -eq 0 ]] || die "run with sudo"

pkg_mgr() {
  if command -v apt-get >/dev/null; then echo apt
  elif command -v dnf >/dev/null; then echo dnf
  elif command -v pacman >/dev/null; then echo pacman
  else echo none; fi
}

pkg_list() {
  case "$1" in
    # Installed only: a removed package whose config files remain ("rc") is not.
    apt)    dpkg-query -W -f='${db:Status-Abbrev} ${Package}\n' | awk '$1=="ii"{print $2}' | sort -u ;;
    dnf)    rpm -qa --qf '%{NAME}\n' | sort -u ;;
    pacman) pacman -Qq | sort -u ;;
  esac
}

# The SSH server package and whether it is installed, per manager.
ssh_pkg() { case "$1" in apt) echo openssh-server ;; dnf) echo openssh-server ;; pacman) echo openssh ;; esac; }
pkg_installed() {
  case "$1" in
    apt)    [[ $(dpkg-query -W -f='${Status}' "$2" 2>/dev/null || true) == "install ok installed" ]] ;;
    dnf)    rpm -q "$2" >/dev/null 2>&1 ;;
    pacman) pacman -Q "$2" >/dev/null 2>&1 ;;
  esac
}

# install_ssh installs the SSH server and records every package and every
# system user or group the install created, so undo removes exactly those
# (dependencies pulled in included).
install_ssh() {
  local m; m=$(pkg_mgr)
  [[ $m != none ]] || die "unknown package manager; install an SSH server by hand"
  local p; p=$(ssh_pkg "$m")
  pkg_installed "$m" "$p" && return 0
  local before users groups rc=""; before=$(pkg_list "$m")
  [[ $m != apt ]] || rc=$(dpkg-query -W -f='${db:Status-Abbrev} ${Package}\n' | awk '$1=="rc"{print $2}' | sort -u)
  users=$(cut -d: -f1 /etc/passwd | sort); groups=$(cut -d: -f1 /etc/group | sort)
  log "installing $p"
  case "$m" in
    apt)    DEBIAN_FRONTEND=noninteractive apt-get update -qq
            DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends "$p" >/dev/null ;;
    dnf)    dnf install -y -q --setopt=install_weak_deps=False "$p" >/dev/null ;;
    pacman) pacman -S --noconfirm --needed "$p" >/dev/null ;;
  esac
  local n
  while read -r n; do
    [[ -n $n ]] || continue
    # One that was "rc" before goes back to "rc" (remove, not purge).
    if grep -qxF -- "$n" <<<"$rc"; then record "pkg-rc $m $n"; else record "pkg $m $n"; fi
  done < <(comm -13 <(echo "$before") <(pkg_list "$m"))
  while read -r n; do [[ -n $n ]] && record "user $n"; done < <(comm -13 <(echo "$users") <(cut -d: -f1 /etc/passwd | sort))
  while read -r n; do [[ -n $n ]] && record "group $n"; done < <(comm -13 <(echo "$groups") <(cut -d: -f1 /etc/group | sort))
}

unit_state() {
  local en act
  en=$(systemctl is-enabled "$1" 2>/dev/null || true); act=$(systemctl is-active "$1" 2>/dev/null || true)
  echo "${en:-unknown} ${act:-unknown}"
}

# start_ssh makes sshd run now and at boot. Before it changes anything it
# records the enabled and active state of the service and, on Ubuntu, of
# ssh.socket (starting ssh.service can start it too), so undo puts back exactly that.
start_ssh() {
  command -v systemctl >/dev/null || die "no systemctl; start sshd by hand"
  local unit=sshd.service; systemctl cat ssh.service >/dev/null 2>&1 && unit=ssh.service
  local en act; read -r en act < <(unit_state "$unit")
  [[ $en != masked ]] || die "$unit is masked; unmask it by hand first"
  if [[ $en == enabled && $act == active ]]; then return 0; fi
  # Ubuntu socket-activates sshd: a listening ssh.socket already serves port 22.
  if [[ $act != active ]] && systemctl is-active --quiet ssh.socket 2>/dev/null; then return 0; fi
  if systemctl cat ssh.socket >/dev/null 2>&1; then record "svc ssh.socket $(unit_state ssh.socket)"; fi
  record "svc $unit $en $act"
  local out l
  out=$(systemctl enable --now "$unit" 2>&1) || die "systemctl enable --now $unit failed: $out"
  # The links enable made (an alias such as sshd.service included): when the
  # package goes again at undo, these would be left dangling.
  while IFS= read -r l; do
    [[ -n $l ]] && record "link $l"
  done < <(sed -n "s/^Created symlink '\{0,1\}\([^' ]*\)'\{0,1\} → .*/\1/p" <<<"$out")
}

# perm_fix gives path the owner and the mode sshd's StrictModes accepts,
# recording the old owner and mode first when it has to change them.
perm_fix() {
  local path=$1 want=$2 uid gid cur
  read -r cur uid gid < <(stat -c '%a %u %g' "$path")
  local wuid wgid; wuid=$(id -u "$LAB_USER"); wgid=$(id -g "$LAB_USER")
  if [[ $uid == "$wuid" && $(( 8#$cur & 8#022 )) -eq 0 ]]; then return 0; fi
  record "perm $path $cur $uid $gid"
  chown "$wuid:$wgid" "$path"; chmod "$want" "$path"
}

add_key() {
  local key; key=$(curl -fsS "http://$DRIVER:$PORT/driver.pub") || die "cannot fetch driver.pub from $DRIVER:$PORT"
  key=$(printf '%s\n' "$key" | head -n1)
  [[ $key =~ ^(ssh-ed25519|ssh-rsa|ecdsa-sha2-[a-z0-9]+)\ [A-Za-z0-9+/=]+ ]] || die "driver.pub is not an SSH public key"
  local blob; blob=$(awk '{print $2}' <<<"$key")
  local line="$key $MARK"
  local ssh_dir=$HOME_DIR/.ssh ak=$HOME_DIR/.ssh/authorized_keys
  local uid gid; uid=$(id -u "$LAB_USER"); gid=$(id -g "$LAB_USER")

  if [[ -L $ssh_dir || ( -e $ssh_dir && ! -d $ssh_dir ) ]]; then die "$ssh_dir is not a plain directory"; fi
  if [[ ! -d $ssh_dir ]]; then
    install -d -m 700 -o "$uid" -g "$gid" "$ssh_dir"; record "mkdir $ssh_dir"
    command -v restorecon >/dev/null && restorecon "$ssh_dir" || true
  else
    perm_fix "$ssh_dir" 700
  fi
  [[ -L $ak ]] && die "$ak is a symlink"

  if [[ ! -e $ak ]]; then
    (umask 077; printf '%s\n' "$line" >"$ak"); chown "$uid:$gid" "$ak"; chmod 600 "$ak"
    record "akfile $ak"
    command -v restorecon >/dev/null && restorecon "$ak" || true
    return 0
  fi
  perm_fix "$ak" 600
  # Already authorized (with or without our mark): leave the file alone.
  awk -v b="$blob" '{for(i=1;i<=NF;i++) if($i==b){f=1}} END{exit !f}' "$ak" && return 0
  mkdir -p "$STATE_DIR"; chmod 700 "$STATE_DIR"
  cp -p "$ak" "$STATE_DIR/ak.before"
  # A last line without a newline would otherwise swallow ours.
  if [[ -s $ak && $(tail -c1 "$ak" | od -An -c | tr -d ' ') != '\n' ]]; then printf '\n' >>"$ak"; fi
  printf '%s\n' "$line" >>"$ak"
  cp -p "$ak" "$STATE_DIR/ak.after"
  record "akline $ak"
}

add_sudoers() {
  [[ -e $SUDOERS ]] && return 0
  local tmp; tmp=$(mktemp)
  printf '%s ALL=(ALL) NOPASSWD: ALL\n' "$LAB_USER" >"$tmp"
  visudo -cqf "$tmp" >/dev/null || { rm -f "$tmp"; die "generated sudoers line does not validate"; }
  install -m 440 -o root -g root "$tmp" "$SUDOERS"; rm -f "$tmp"
  record "sudoers $SUDOERS"
}

# open_ports opens the campaign ports in whichever firewall is active, and
# records only the rules it added: a rule that was there before survives undo.
open_ports() {
  local p out st
  if command -v ufw >/dev/null; then
    # Capture first: "ufw status | grep -q" can lose to SIGPIPE under pipefail.
    st=$(ufw status 2>/dev/null || true)
    if [[ $st == *"Status: active"* ]]; then
      for p in "${PORTS[@]}"; do
        out=$(ufw allow "$p" 2>&1) || die "ufw allow $p: $out"
        if [[ $out == *"Rule added"* ]]; then record "ufw $p"; fi
      done
      return 0
    fi
  fi
  if command -v firewall-cmd >/dev/null && firewall-cmd --state >/dev/null 2>&1; then
    # Runtime rules only (no --permanent): a firewalld reload or a reboot
    # closes them again, and nothing persists on the PC.
    for p in "${PORTS[@]}"; do
      firewall-cmd -q --query-port="$p" && continue
      firewall-cmd -q --add-port="$p" || die "firewall-cmd --add-port=$p failed"
      record "fwd $p"
    done
  fi
}

# save_self keeps a copy of this script on the PC so undo needs no driver.
save_self() {
  mkdir -p "$STATE_DIR"; chmod 700 "$STATE_DIR"
  if curl -fsS "http://$DRIVER:$PORT/lab-pc.sh" -o "$STATE_DIR/lab-pc.sh.new" 2>/dev/null; then
    mv -f "$STATE_DIR/lab-pc.sh.new" "$STATE_DIR/lab-pc.sh"
  else
    rm -f "$STATE_DIR/lab-pc.sh.new"; warn "could not save a copy of lab-pc.sh for undo"
  fi
}

resolve_user() {
  LAB_USER=${1:-${SUDO_USER:-$(logname 2>/dev/null || true)}}
  [[ -n $LAB_USER ]] || die "cannot tell the lab user; run through sudo as that user, or pass it: setup <user>"
  [[ $LAB_USER != root ]] || die "the lab user is root; run this through sudo as the lab user (the driver logs in as that user)"
  [[ $(id -u "$LAB_USER" 2>/dev/null || echo 0) != 0 ]] || die "lab user $LAB_USER does not exist or has uid 0"
  HOME_DIR=$(getent passwd "$LAB_USER" | cut -d: -f6)
  [[ -n $HOME_DIR && $HOME_DIR == /* && -d $HOME_DIR ]] || die "home directory of $LAB_USER not found"
}

setup() {
  [[ $DRIVER != __DRIVER_IP__ && $PORT != __DRIVER_PORT__ ]] || die "DRIVER is not filled in; serve this file with scripts/campaign/serve.sh"
  resolve_user "${1:-}"
  if [[ -f $STATE ]]; then
    local prev; prev=$(awk '$1=="user-is"{print $2; exit}' "$STATE")
    [[ -z $prev || $prev == "$LAB_USER" ]] || die "setup already ran for $prev; run undo first"
  fi
  record "user-is $LAB_USER $HOME_DIR"
  save_self
  # sshd makes its host keys on first start; they belong to no package, so
  # undo removes what appears under /etc/ssh from here on.
  if ! grep -qx etc-ssh "$STATE"; then
    find /etc/ssh 2>/dev/null | sort >"$STATE_DIR/etc-ssh.before" || true
    record "etc-ssh"
  fi
  install_ssh
  start_ssh
  add_key
  add_sudoers
  open_ports

  local ip
  ip=$(ip -4 route get "$DRIVER" 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1);exit}}' || true)
  [[ -n $ip ]] || ip=$( (hostname -I 2>/dev/null || true) | awk '{print $1}')
  [[ -n $ip ]] || die "cannot tell this PC's IP towards $DRIVER"
  local mid; mid=$(tr -d ' \n' </etc/machine-id 2>/dev/null || true)
  [[ -n $mid ]] || die "/etc/machine-id is empty"
  echo "LAB-PC host=$(uname -n) ip=$ip user=$LAB_USER cores=$(nproc) ram_gib=$(awk '/MemTotal/{printf "%.0f",$2/1048576}' /proc/meminfo) machine_id=$mid"
}

same_file() { [[ $(sha256sum <"$1") == "$(sha256sum <"$2")" ]]; }

# restore_ak puts authorized_keys back: byte for byte when nobody changed it
# since setup, otherwise by removing only the line setup added.
restore_ak() {
  local ak=$1
  [[ -f $ak ]] || return 0
  if [[ -f $STATE_DIR/ak.after && -f $STATE_DIR/ak.before ]] && same_file "$ak" "$STATE_DIR/ak.after"; then
    cat "$STATE_DIR/ak.before" >"$ak"
  else
    warn "$ak changed since setup; removing only the campaign key line"
    local tmp; tmp=$(mktemp)
    grep -v -- " $MARK\$" "$ak" >"$tmp" || true
    cat "$tmp" >"$ak"; rm -f "$tmp"
  fi
}

undo() {
  [[ -f $STATE ]] || { log "nothing recorded"; return 0; }
  trap - ERR
  set +e
  local pkgs=() rcpkgs=() users=() groups=() m="" l ssh_installed=0 reload=0
  grep -qE '^pkg(-rc)? [a-z]+ (openssh-server|openssh)$' "$STATE" && ssh_installed=1
  # Newest change first, so each step sees the state it made.
  while IFS= read -r l; do
    case "$l" in
      "ufw "*)    ufw delete allow "${l#ufw }" >/dev/null || warn "ufw ${l#ufw } not removed" ;;
      "fwd "*)    firewall-cmd -q --remove-port="${l#fwd }" || warn "firewalld ${l#fwd } not removed" ;;
      "sudoers "*) rm -f -- "${l#sudoers }" ;;
      "akline "*) restore_ak "${l#akline }" ;;
      "akfile "*) local f=${l#akfile }
                  if [[ -f $f ]]; then
                    if grep -qv -- " $MARK\$" "$f"; then warn "$f has other keys now; removing only ours"; restore_ak "$f"
                    else rm -f -- "$f"; fi
                  fi ;;
      "mkdir "*)  rmdir -- "${l#mkdir }" 2>/dev/null || warn "${l#mkdir } is not empty; left in place" ;;
      "perm "*)   read -r _ p mode uid gid <<<"$l"
                  [[ -e $p ]] && { chown "$uid:$gid" "$p"; chmod "$mode" "$p"; } ;;
      "link "*)   [[ -L ${l#link } ]] && rm -f -- "${l#link }"; reload=1 ;;
      "svc "*)    read -r _ unit en act <<<"$l"
                  [[ $act == active ]] || systemctl stop "$unit" 2>/dev/null || warn "$unit not stopped"
                  # When setup installed sshd, removing the package and the links
                  # above restores the enablement it had (none, or what "rc" kept).
                  [[ $en == enabled || $ssh_installed == 1 ]] || systemctl disable -q "$unit" 2>/dev/null || warn "$unit not disabled" ;;
      "pkg "*)    read -r _ m p <<<"$l"; pkgs+=("$p") ;;
      "pkg-rc "*) read -r _ m p <<<"$l"; rcpkgs+=("$p") ;;
      "user "*)   users+=("${l#user }") ;;
      "group "*)  groups+=("${l#group }") ;;
    esac
  done < <(tac "$STATE")
  if ((${#pkgs[@]})); then
    log "removing ${pkgs[*]}"
    case "$m" in
      apt)    DEBIAN_FRONTEND=noninteractive apt-get purge -y -qq "${pkgs[@]}" >/dev/null ;;
      dnf)    dnf remove -y -q "${pkgs[@]}" >/dev/null ;;
      pacman) pacman -Rns --noconfirm "${pkgs[@]}" >/dev/null ;;
    esac || warn "package removal failed: ${pkgs[*]}"
  fi
  if ((${#rcpkgs[@]})); then
    log "removing ${rcpkgs[*]} (config files kept, as before)"
    DEBIAN_FRONTEND=noninteractive apt-get remove -y -qq "${rcpkgs[@]}" >/dev/null || warn "package removal failed: ${rcpkgs[*]}"
  fi
  if [[ -f $STATE_DIR/etc-ssh.before ]] && grep -qx etc-ssh "$STATE"; then
    # Leftovers under /etc/ssh that were not there before the install, deepest first.
    find /etc/ssh 2>/dev/null | sort | comm -13 "$STATE_DIR/etc-ssh.before" - | sort -r | while IFS= read -r f; do
      if [[ -d $f && ! -L $f ]]; then rmdir -- "$f" || warn "$f not removed"; else rm -f -- "$f"; fi
    done
  fi
  for l in "${users[@]}"; do getent passwd "$l" >/dev/null && { userdel "$l" || warn "user $l not removed"; }; done
  for l in "${groups[@]}"; do getent group "$l" >/dev/null && { groupdel "$l" || warn "group $l not removed"; }; done
  ((reload)) && systemctl daemon-reload
  rm -rf -- "$STATE_DIR"
  log "undo complete"
}

case "${1:-}" in
  setup) setup "${2:-}" ;;
  undo)  undo ;;
  *) die "usage: lab-pc.sh setup [lab-user] | undo" ;;
esac
