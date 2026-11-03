#!/usr/bin/env bash
# lab.sh: the one script the operator types on every lab PC for the
# PipedPeer campaign (scripts/campaign/HEADLESS.md). Published as
# https://vinayakyadav2709.github.io/Pipedpeer/lab.sh by publish.sh, which fills
# in the campaign key and the pinned commit. Run from the lab user's terminal:
#
#   curl -fsSL https://vinayakyadav2709.github.io/Pipedpeer/lab.sh | sudo bash
#       worker: open SSH for the campaign key, passwordless sudo for the lab
#       user, open the ports, print LAB-PC and DONE. (`setup [user]` is the same.)
#   curl -fsSL .../lab.sh | sudo bash -s -- head [pcs]
#       the same, then ask for the campaign passphrase, fetch the code and
#       start the whole lab day (lab-day.sh) as a systemd unit; pcs is how many
#       PCs to wait for, this one included (default 17).
#   ... | sudo bash -s -- status      the day's phase, progress and ETA
#   ... | sudo bash -s -- undo        put this PC back exactly as it was
#   sudo bash /var/lib/pipedpeer-lab/lab.sh undo   (the copy setup keeps)
#
# Every change is appended to $STATE as it happens, and only when it really
# changed something, so running setup twice records nothing new. undo replays
# $STATE backwards and restores what was there before.
# lab-day.sh also calls `pkg <name>...` and `enable <unit>` here, so the
# packages and services it adds are undone the same way.
set -Eeuo pipefail
# publish.sh (or serve.sh) fills these lines; an unfilled copy refuses to run.
CAMPAIGN_KEY="__CAMPAIGN_PUBKEY__"
CAMPAIGN_COMMIT="__CAMPAIGN_COMMIT__"
PIPEDPEER_VERSION=""
SITE="https://vinayakyadav2709.github.io/Pipedpeer"
# Tests only: a local stand-in for GitHub Pages and codeload.
SITE=${LAB_SITE:-$SITE}
CODE_URL=${LAB_CODE_URL:-https://codeload.github.com/vinayakyadav2709/Pipedpeer/tar.gz/$CAMPAIGN_COMMIT}

STATE_DIR=/var/lib/pipedpeer-lab
STATE=$STATE_DIR/state
SECRETS=/run/pipedpeer-lab
UNIT=pipedpeer-lab-day
MARK="# pipedpeer-lab"
SUDOERS=/etc/sudoers.d/pipedpeer-lab
# 22 ssh, 4242-4244 pipedpeer, 4317 OTLP to pc00, 5201 iperf3, 8137 pc00's file server.
PORTS=(22/tcp 4242/tcp 4242/udp 4243/udp 4244/udp 4317/tcp 5201/tcp 8137/tcp)
# What the campaign may create in the lab user's home. undo removes each one
# that did not exist before setup (and parents it made, when empty).
# (motd.legal-displayed: Ubuntu writes it on the first ssh login; snowflake,
# pip, virtualenv: the day's Ansible venv; rclone: sync-r2.sh.)
HOME_PATHS=(pipedpeer-lab .ansible .nix-profile .nix-defexpr .nix-channels .ssh/known_hosts
  .cache/nix .cache/pipedpeer .cache/pip .cache/virtualenv .cache/snowflake .cache/motd.legal-displayed
  .local/state/nix .local/state/pipedpeer .local/share/pipedpeer .local/share/pipedpeer-lab
  .config/pipedpeer .config/pipedpeer-lab .config/nix .config/rclone)
HOME_PARENTS=(.cache .local/state .local/share .local .config)

log()  { echo "[lab] $*"; }
warn() { echo "[lab] WARNING: $*" >&2; }
die()  { echo "[lab] ERROR: $*" >&2; exit 1; }
trap 'echo "[lab] ERROR: line $LINENO: $BASH_COMMAND failed" >&2' ERR

# record appends one state line, once: a rerun of setup must not make undo act twice.
record() {
  mkdir -p "$STATE_DIR"; chmod 700 "$STATE_DIR"
  [[ -f $STATE ]] && grep -qxF -- "$1" "$STATE" && return 0
  printf '%s\n' "$1" >>"$STATE"
}

# stamp <event>: when this PC reached it (E14, curl to cluster), once; the
# day reads $STATE_DIR/times through sudo before undo removes it.
stamp() {
  mkdir -p "$STATE_DIR"; chmod 700 "$STATE_DIR"
  grep -q "^$1 " "$STATE_DIR/times" 2>/dev/null || printf '%s %s\n' "$1" "$(date +%s%N)" >>"$STATE_DIR/times"
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

pkg_installed() {
  case "$1" in
    apt)    [[ $(dpkg-query -W -f='${Status}' "$2" 2>/dev/null || true) == "install ok installed" ]] ;;
    dnf)    rpm -q "$2" >/dev/null 2>&1 ;;
    pacman) pacman -Q "$2" >/dev/null 2>&1 ;;
  esac
}

APT=(env DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=600 -y -qq)

# install_pkgs installs packages and records every package it added and every
# one it took away (apt replaces systemd-timesyncd with chrony), and every
# system user or group the install created, so undo restores exactly that.
install_pkgs() {
  local m; m=$(pkg_mgr)
  [[ $m != none ]] || die "unknown package manager; install $* by hand"
  local missing=() p
  for p in "$@"; do pkg_installed "$m" "$p" || missing+=("$p"); done
  ((${#missing[@]})) || return 0
  local before users groups rc=""; before=$(pkg_list "$m")
  [[ $m != apt ]] || rc=$(dpkg-query -W -f='${db:Status-Abbrev} ${Package}\n' | awk '$1=="rc"{print $2}' | sort -u)
  users=$(cut -d: -f1 /etc/passwd | sort); groups=$(cut -d: -f1 /etc/group | sort)
  log "installing ${missing[*]}"
  case "$m" in
    apt)    "${APT[@]}" update
            "${APT[@]}" install --no-install-recommends "${missing[@]}" >/dev/null ;;
    dnf)    dnf install -y -q --setopt=install_weak_deps=False "${missing[@]}" >/dev/null ;;
    pacman) pacman -S --noconfirm --needed "${missing[@]}" >/dev/null ;;
  esac
  local n after; after=$(pkg_list "$m")
  while read -r n; do
    [[ -n $n ]] || continue
    # One that was "rc" before goes back to "rc" (remove, not purge).
    if grep -qxF -- "$n" <<<"$rc"; then record "pkg-rc $m $n"; else record "pkg $m $n"; fi
  done < <(comm -13 <(echo "$before") <(echo "$after"))
  while read -r n; do [[ -n $n ]] && record "pkg-gone $m $n"; done < <(comm -23 <(echo "$before") <(echo "$after"))
  while read -r n; do [[ -n $n ]] && record "user $n"; done < <(comm -13 <(echo "$users") <(cut -d: -f1 /etc/passwd | sort))
  while read -r n; do [[ -n $n ]] && record "group $n"; done < <(comm -13 <(echo "$groups") <(cut -d: -f1 /etc/group | sort))
}

ssh_pkg() { case "$1" in pacman) echo openssh ;; *) echo openssh-server ;; esac; }

unit_state() {
  local en act
  en=$(systemctl is-enabled "$1" 2>/dev/null || true); act=$(systemctl is-active "$1" 2>/dev/null || true)
  echo "${en:-unknown} ${act:-unknown}"
}

# enable_unit makes a unit run now and at boot, recording its enabled and
# active state first (and the links enable made), so undo puts back exactly that.
enable_unit() {
  local unit=$1 en act out l
  read -r en act < <(unit_state "$unit")
  [[ $en != masked ]] || die "$unit is masked; unmask it by hand first"
  if [[ $en == enabled && $act == active ]]; then return 0; fi
  record "svc $unit $en $act"
  out=$(systemctl enable --now "$unit" 2>&1) || die "systemctl enable --now $unit failed: $out"
  # The links enable made (an alias such as sshd.service included): when the
  # package goes again at undo, these would be left dangling.
  while IFS= read -r l; do
    [[ -n $l ]] && record "link $l"
  done < <(sed -n "s/^Created symlink '\{0,1\}\([^' ]*\)'\{0,1\} → .*/\1/p" <<<"$out")
}

# start_ssh makes sshd run now and at boot. On Ubuntu it also records
# ssh.socket (starting ssh.service can start it too).
start_ssh() {
  command -v systemctl >/dev/null || die "no systemctl; start sshd by hand"
  local unit=sshd.service; systemctl cat ssh.service >/dev/null 2>&1 && unit=ssh.service
  local en act; read -r en act < <(unit_state "$unit")
  if [[ $en == enabled && $act == active ]]; then return 0; fi
  # Ubuntu socket-activates sshd: a listening ssh.socket already serves port 22.
  if [[ $act != active ]] && systemctl is-active --quiet ssh.socket 2>/dev/null; then return 0; fi
  if systemctl cat ssh.socket >/dev/null 2>&1; then record "svc ssh.socket $(unit_state ssh.socket)"; fi
  enable_unit "$unit"
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
  local key=$CAMPAIGN_KEY
  [[ $key =~ ^(ssh-ed25519|ssh-rsa|ecdsa-sha2-[a-z0-9]+)\ [A-Za-z0-9+/=]+ ]] || die "the embedded campaign key is not an SSH public key"
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

# The interface and address of the default route, and its on-link subnet.
route_dev() { ip -4 route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1);exit}}' || true; }
route_src() { ip -4 route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1);exit}}' || true; }
own_subnet() {
  local dev; dev=$(route_dev); [[ -n $dev ]] || return 0
  ip -4 route show dev "$dev" proto kernel scope link 2>/dev/null | awk '{print $1; exit}' || true
}

# open_ports opens the campaign ports, and all ports to this PC's own subnet
# (Ray and Dask pick ports at random), in whichever firewall is active. It
# records only the rules it added: a rule that was there before survives undo.
open_ports() {
  local p out st net; net=$(own_subnet)
  if command -v ufw >/dev/null; then
    # Capture first: "ufw status | grep -q" can lose to SIGPIPE under pipefail.
    st=$(ufw status 2>/dev/null || true)
    if [[ $st == *"Status: active"* ]]; then
      for p in "${PORTS[@]}"; do
        out=$(ufw allow "$p" 2>&1) || die "ufw allow $p: $out"
        if [[ $out == *"Rule added"* ]]; then record "ufw $p"; fi
      done
      if [[ -n $net ]]; then
        out=$(ufw allow from "$net" 2>&1) || warn "ufw allow from $net: $out"
        if [[ $out == *"Rule added"* ]]; then record "ufw-from $net"; fi
      fi
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
    if [[ -n $net ]] && ! firewall-cmd -q --zone=trusted --query-source="$net"; then
      if firewall-cmd -q --zone=trusted --add-source="$net" 2>/dev/null; then record "fwd-src $net"
      else warn "could not trust $net in firewalld; only the campaign ports are open"; fi
    fi
  fi
}

# save_self keeps a copy of this script on the PC, so undo needs no network.
save_self() {
  mkdir -p "$STATE_DIR"; chmod 700 "$STATE_DIR"
  local src=${BASH_SOURCE[0]:-}
  if [[ -n $src && -f $src && $src != "$STATE_DIR/lab.sh" ]]; then
    install -m 700 "$src" "$STATE_DIR/lab.sh"; return 0
  fi
  [[ -f $STATE_DIR/lab.sh ]] && grep -qxF "CAMPAIGN_COMMIT=\"$CAMPAIGN_COMMIT\"" "$STATE_DIR/lab.sh" && return 0
  # Piped from curl: fetch the same file again, and keep it only if it is this version.
  if curl -fsSL "$SITE/lab.sh" -o "$STATE_DIR/lab.sh.new" 2>/dev/null &&
     grep -qxF "CAMPAIGN_COMMIT=\"$CAMPAIGN_COMMIT\"" "$STATE_DIR/lab.sh.new"; then
    chmod 700 "$STATE_DIR/lab.sh.new"; mv -f "$STATE_DIR/lab.sh.new" "$STATE_DIR/lab.sh"
  else
    rm -f "$STATE_DIR/lab.sh.new"; warn "could not save a copy of lab.sh for undo; undo with: curl -fsSL $SITE/lab.sh | sudo bash -s -- undo"
  fi
}

resolve_user() {
  LAB_USER=${1:-${SUDO_USER:-$(logname 2>/dev/null || true)}}
  [[ -n $LAB_USER ]] || die "cannot tell the lab user; run through sudo as that user, or pass it: setup <user>"
  [[ $LAB_USER != root ]] || die "the lab user is root; run this through sudo as the lab user (the campaign logs in as that user)"
  [[ $(id -u "$LAB_USER" 2>/dev/null || echo 0) != 0 ]] || die "lab user $LAB_USER does not exist or has uid 0"
  HOME_DIR=$(getent passwd "$LAB_USER" | cut -d: -f6)
  [[ -n $HOME_DIR && $HOME_DIR == /* && -d $HOME_DIR ]] || die "home directory of $LAB_USER not found"
}

need_published() {
  [[ $CAMPAIGN_KEY != __CAMPAIGN_PUBKEY__ && $CAMPAIGN_COMMIT != __CAMPAIGN_COMMIT__ ]] ||
    die "this copy of lab.sh is not published yet (campaign key or commit not filled in); run scripts/campaign/publish.sh"
}

# home_before lists which of HOME_PATHS and HOME_PARENTS exist now, once.
home_before() {
  [[ -f $STATE_DIR/home.before ]] && return 0
  local p
  for p in "${HOME_PATHS[@]}" "${HOME_PARENTS[@]}"; do
    [[ -e $HOME_DIR/$p || -L $HOME_DIR/$p ]] && echo "$p"
  done >"$STATE_DIR/home.before"
  record "home-before"
}

setup() {
  need_published
  stamp setup_start
  resolve_user "${1:-}"
  if [[ -f $STATE ]]; then
    local prev; prev=$(awk '$1=="user-is"{print $2; exit}' "$STATE")
    [[ -z $prev || $prev == "$LAB_USER" ]] || die "setup already ran for $prev; run undo first"
  fi
  record "user-is $LAB_USER $HOME_DIR"
  save_self
  home_before
  # sshd makes its host keys on first start; they belong to no package, so
  # undo removes what appears under /etc/ssh from here on.
  if ! grep -qx etc-ssh "$STATE"; then
    find /etc/ssh 2>/dev/null | sort >"$STATE_DIR/etc-ssh.before" || true
    record "etc-ssh"
  fi
  install_pkgs "$(ssh_pkg "$(pkg_mgr)")"
  start_ssh
  add_key
  add_sudoers
  open_ports

  local ip; ip=$(route_src)
  [[ -n $ip ]] || ip=$( (hostname -I 2>/dev/null || true) | awk '{print $1}')
  [[ -n $ip ]] || die "cannot tell this PC's IP (no default route)"
  PC_IP=$ip
  local mid; mid=$(tr -d ' \n' </etc/machine-id 2>/dev/null || true)
  [[ -n $mid ]] || die "/etc/machine-id is empty"
  stamp labpc
  echo "LAB-PC host=$(uname -n) ip=$ip user=$LAB_USER cores=$(nproc) ram_gib=$(awk '/MemTotal/{printf "%.0f",$2/1048576}' /proc/meminfo) machine_id=$mid"
}

banner() {
  local line="=================================================================="
  printf '\n%s\n' "$line"
  local l; for l in "$@"; do printf '  %s\n' "$l"; done
  printf '%s\n\n' "$line"
}

worker() {
  setup "${1:-}"
  banner "DONE: this PC is ready, you can leave it.   (ip $PC_IP)"
}

# --- head ------------------------------------------------------------------

read_passphrase() {
  if [[ -n ${LAB_PASSPHRASE_FILE:-} ]]; then   # tests only
    PASS=$(<"$LAB_PASSPHRASE_FILE"); return 0
  fi
  [[ -r /dev/tty ]] || die "no terminal to ask for the passphrase on"
  printf 'Campaign passphrase: ' >/dev/tty
  IFS= read -rs PASS </dev/tty || die "no passphrase"
  printf '\n' >/dev/tty
}

# get_secrets decrypts the bundle into $SECRETS (tmpfs: gone at reboot, and
# removed by undo). The directory is root's and not listable; the two files
# in it belong to the lab user, mode 600, so the day can use them.
get_secrets() {
  local uid gid; uid=$(id -u "$LAB_USER"); gid=$(id -g "$LAB_USER")
  if [[ -f $SECRETS/campaign_ed25519 && -f $SECRETS/r2.env ]] && key_matches "$SECRETS/campaign_ed25519"; then
    log "secrets already in $SECRETS"; return 0
  fi
  command -v openssl >/dev/null || die "openssl not found; it decrypts the campaign secrets"
  local enc tmp try; enc=$(mktemp); tmp=$(mktemp -d)
  curl -fsSL "$SITE/lab-secrets.enc" -o "$enc" || { rm -rf "$enc" "$tmp"; die "cannot download $SITE/lab-secrets.enc"; }
  for try in 1 2 3; do
    read_passphrase
    rm -rf "${tmp:?}"/*
    if openssl enc -d -aes-256-cbc -pbkdf2 -iter 600000 -md sha256 -in "$enc" -pass fd:3 3< <(printf '%s\n' "$PASS") 2>/dev/null |
       tar -xz -C "$tmp" 2>/dev/null && [[ -f $tmp/campaign_ed25519 && -f $tmp/r2.env ]] && key_matches "$tmp/campaign_ed25519"; then
      break
    fi
    [[ -z ${LAB_PASSPHRASE_FILE:-} && $try -lt 3 ]] || { rm -rf "$enc" "$tmp"; die "wrong passphrase"; }
    echo "[lab] wrong passphrase, try again" >&2
  done
  PASS=""
  mkdir -p "$SECRETS"; chown root:root "$SECRETS"; chmod 711 "$SECRETS"
  record "secrets $SECRETS"
  local f
  for f in campaign_ed25519 r2.env; do
    install -m 600 -o "$uid" -g "$gid" "$tmp/$f" "$SECRETS/$f"
  done
  rm -rf "$enc" "$tmp"
  log "secrets decrypted into $SECRETS"
}

# key_matches: the private key's public half is the embedded campaign key.
key_matches() {
  local pub; pub=$(ssh-keygen -y -f "$1" 2>/dev/null) || return 1
  [[ $(awk '{print $2}' <<<"$pub") == "$(awk '{print $2}' <<<"$CAMPAIGN_KEY")" ]]
}

as_lab() { runuser -u "$LAB_USER" -- "$@"; }

# fetch_code unpacks the pinned commit under the lab user's home, once: the
# results of the day live inside it, so it is never unpacked over again.
fetch_code() {
  DAY_DIR=$HOME_DIR/pipedpeer-lab
  REPO=$DAY_DIR/repo
  if [[ -f $REPO/.lab-commit ]]; then
    [[ $(<"$REPO/.lab-commit") == "$CAMPAIGN_COMMIT" ]] || die "$REPO holds commit $(<"$REPO/.lab-commit"), not $CAMPAIGN_COMMIT; finish or undo that day first"
    return 0
  fi
  as_lab mkdir -p "$REPO"
  log "fetching the campaign code at $CAMPAIGN_COMMIT"
  as_lab curl -fsSL --retry 3 "$CODE_URL" -o "$DAY_DIR/code.tar.gz" || die "cannot download $CODE_URL"
  as_lab tar -xzf "$DAY_DIR/code.tar.gz" --strip-components=1 -C "$REPO" || die "cannot unpack the code"
  [[ -f $REPO/scripts/campaign/lab-day.sh ]] || die "the code at $CAMPAIGN_COMMIT has no scripts/campaign/lab-day.sh"
  # shellcheck disable=SC2016  # expanded by the inner sh
  as_lab sh -c 'printf "%s\n" "$1" >"$2"' _ "$CAMPAIGN_COMMIT" "$REPO/.lab-commit"
}

unit_active() { [[ $(systemctl is-active "$UNIT" 2>/dev/null || true) == @(active|activating|deactivating) ]]; }

launch_day() {
  local pcs=$1
  if unit_active; then log "the lab day is already running; following it"; return 0; fi
  systemctl reset-failed "$UNIT" >/dev/null 2>&1 || true
  local envs=(--setenv=LAB_PCS="$pcs" --setenv=LAB_KEY="$SECRETS/campaign_ed25519"
    --setenv=PIPEDPEER_LAB_R2_ENV="$SECRETS/r2.env" --setenv=LAB_SITE="$SITE"
    --setenv=LAB_PIPEDPEER_VERSION="$PIPEDPEER_VERSION")
  local v
  # Tests and overrides, passed through only when set: the day's own, and
  # run.sh's PL_* knobs (README.md beside lab-day.sh).
  for v in LAB_DISCOVER_TIMEOUT LAB_PROFILE LAB_SUBNET LAB_SESSION LAB_CAL_STRONG_S LAB_CAL_WEAK_S CAL_MIN_S \
    PIPEDPEER_LAB_S3_ENDPOINT PIPEDPEER_RELEASE_BASE_URL $(compgen -v PL_ || true); do
    [[ -n ${!v:-} ]] && envs+=(--setenv="$v=${!v}")
  done
  # A system unit owned by the lab user: it survives closing the terminal,
  # logging out and the screen lock, and needs no linger. TasksMax: the unit
  # holds the Ray head, the Dask scheduler, Ansible's forks and the
  # collectors; systemd's default (15 % of pid_max) killed Ray's gRPC threads
  # where pid_max is small.
  systemd-run --quiet --unit="$UNIT" --uid="$LAB_USER" --gid="$(id -g "$LAB_USER")" \
    -p WorkingDirectory="$REPO" -p KillMode=control-group -p SyslogIdentifier="$UNIT" -p TasksMax=infinity "${envs[@]}" \
    /bin/bash "$REPO/scripts/campaign/lab-day.sh" || die "systemd-run failed"
  log "started the lab day as the systemd unit $UNIT"
}

follow_day() {
  banner "The lab day runs on its own now; it ends with a summary here." \
    "You may close this terminal. To look again later, type:" \
    "  curl -fsSL $SITE/lab.sh | sudo bash -s -- status" \
    "or follow the full log:  sudo journalctl -fu $UNIT"
  # The day's own output only (sudo logs into the same unit).
  journalctl -fu "$UNIT" -t "$UNIT" -o cat -n 30 &
  local jp=$!
  while unit_active; do sleep 5; done
  sleep 2; kill "$jp" 2>/dev/null || true; wait "$jp" 2>/dev/null || true
  if [[ $(systemctl show -p Result --value "$UNIT" 2>/dev/null || true) == success ]]; then
    return 0
  fi
  banner "The lab day stopped before the end. Nothing is lost:" \
    "type the same head line again and it continues where it stopped."
}

head_mode() {
  local pcs=${1:-17}
  [[ $pcs =~ ^[0-9]+$ && $pcs -ge 1 ]] || die "usage: head [number of PCs, this one included]"
  need_published
  stamp head_start
  # The day already finished its cleanup but was stopped before undoing this PC.
  if [[ -f $STATE_DIR/day-finished ]]; then
    cat "$STATE_DIR/summary.txt" 2>/dev/null || true
    log "the lab day had finished; undoing this PC"; undo; return 0
  fi
  local t; for t in curl tar openssl systemd-run python3 ssh ssh-keygen runuser; do
    command -v "$t" >/dev/null || die "$t not found on this PC; the head needs it"
  done
  resolve_user "${2:-}"
  get_secrets
  setup "$LAB_USER"
  fetch_code
  launch_day "$pcs"
  follow_day
}

status() {
  local home=""
  [[ -f $STATE ]] && home=$(awk '$1=="user-is"{print $3; exit}' "$STATE")
  [[ -n $home || -z ${SUDO_USER:-} ]] || home=$(getent passwd "$SUDO_USER" | cut -d: -f6 || true)
  local prog=${home:-/nonexistent}/pipedpeer-lab/progress.txt
  echo "unit $UNIT: $(systemctl is-active "$UNIT" 2>/dev/null || true)"
  if [[ -f $prog ]]; then cat "$prog"
  else journalctl -u "$UNIT" -t "$UNIT" -o cat -n 40 --no-pager 2>/dev/null || echo "no lab day on this PC"; fi
}

# --- undo ------------------------------------------------------------------

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

# undo_home removes what the campaign left in the lab user's home: each of
# HOME_PATHS that did not exist at setup, then the parents setup did not find,
# when empty. On the head, results not yet confirmed in R2 are kept.
undo_home() {
  local home=$1 p
  [[ -f $STATE_DIR/home.before && -d $home ]] || return 0
  for p in "${HOME_PATHS[@]}"; do
    grep -qxF -- "$p" "$STATE_DIR/home.before" && continue
    [[ -e $home/$p || -L $home/$p ]] || continue
    if [[ $p == pipedpeer-lab && -f $home/$p/repo/results/lab/runs.jsonl && ! -f $home/$p/.synced ]]; then
      warn "kept $home/$p: its results are not confirmed in R2"; continue
    fi
    rm -rf --one-file-system -- "${home:?}/$p" || warn "$home/$p not removed"
  done
  for p in "${HOME_PARENTS[@]}"; do
    grep -qxF -- "$p" "$STATE_DIR/home.before" && continue
    [[ -d $home/$p && ! -L $home/$p ]] && rmdir -- "$home/$p" 2>/dev/null || true
  done
}

undo() {
  [[ -f $STATE ]] || { log "nothing recorded"; return 0; }
  trap - ERR
  set +e
  local pkgs=() rcpkgs=() gone=() users=() groups=() m="" l ssh_installed=0 reload=0 home
  home=$(awk '$1=="user-is"{print $3; exit}' "$STATE")
  undo_home "$home"
  grep -qE '^pkg(-rc)? [a-z]+ (openssh-server|openssh)$' "$STATE" && ssh_installed=1
  # Newest change first, so each step sees the state it made.
  while IFS= read -r l; do
    case "$l" in
      "ufw-from "*) ufw delete allow from "${l#ufw-from }" >/dev/null || warn "ufw from ${l#ufw-from } not removed" ;;
      "ufw "*)    ufw delete allow "${l#ufw }" >/dev/null || warn "ufw ${l#ufw } not removed" ;;
      "fwd-src "*) firewall-cmd -q --zone=trusted --remove-source="${l#fwd-src }" || warn "firewalld source ${l#fwd-src } not removed" ;;
      "fwd "*)    firewall-cmd -q --remove-port="${l#fwd }" || warn "firewalld ${l#fwd } not removed" ;;
      "sudoers "*) rm -f -- "${l#sudoers }" ;;
      "secrets "*) rm -rf -- "${l#secrets }" ;;
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
                  # When setup installed the package, removing it and the links
                  # above restores the enablement it had (none, or what "rc" kept).
                  [[ $en == enabled || ( $ssh_installed == 1 && $unit == ssh* ) ]] || systemctl disable -q "$unit" 2>/dev/null || warn "$unit not disabled" ;;
      "pkg "*)    read -r _ m p <<<"$l"; pkgs+=("$p") ;;
      "pkg-rc "*) read -r _ m p <<<"$l"; rcpkgs+=("$p") ;;
      "pkg-gone "*) read -r _ m p <<<"$l"; gone+=("$p") ;;
      "user "*)   users+=("${l#user }") ;;
      "group "*)  groups+=("${l#group }") ;;
    esac
  done < <(tac "$STATE")
  if ((${#pkgs[@]})); then
    log "removing ${pkgs[*]}"
    case "$m" in
      apt)    "${APT[@]}" purge "${pkgs[@]}" >/dev/null ;;
      dnf)    dnf remove -y -q "${pkgs[@]}" >/dev/null ;;
      pacman) pacman -Rns --noconfirm "${pkgs[@]}" >/dev/null ;;
    esac || warn "package removal failed: ${pkgs[*]}"
  fi
  if ((${#rcpkgs[@]})); then
    log "removing ${rcpkgs[*]} (config files kept, as before)"
    "${APT[@]}" remove "${rcpkgs[@]}" >/dev/null || warn "package removal failed: ${rcpkgs[*]}"
  fi
  if ((${#gone[@]})); then
    log "reinstalling ${gone[*]} (an install had replaced them)"
    case "$m" in
      apt)    "${APT[@]}" install --no-install-recommends "${gone[@]}" >/dev/null ;;
      dnf)    dnf install -y -q "${gone[@]}" >/dev/null ;;
      pacman) pacman -S --noconfirm "${gone[@]}" >/dev/null ;;
    esac || warn "reinstall failed: ${gone[*]}"
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

case "${1:-worker}" in
  worker|setup) worker "${2:-}" ;;
  head)   head_mode "${2:-}" "${3:-}" ;;
  status) status ;;
  undo)   undo ;;
  pkg)    shift; (($#)) || die "usage: pkg <package>..."; [[ -f $STATE ]] || die "setup has not run here"; install_pkgs "$@" ;;
  enable) [[ -n ${2:-} ]] || die "usage: enable <unit>"; [[ -f $STATE ]] || die "setup has not run here"; enable_unit "$2" ;;
  *) die "usage: lab.sh [worker|setup [user]] | head [pcs] | status | undo | pkg <name>... | enable <unit>" ;;
esac
