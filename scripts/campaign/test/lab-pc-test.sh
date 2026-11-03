#!/usr/bin/env bash
# End-to-end test of lab-pc.sh, serve.sh and scan.sh on systemd containers.
#   scripts/campaign/test/lab-pc-test.sh
# Seven lab PC stand-ins (512 MB each) on their own podman network:
#   ub-fresh   Ubuntu 24.04, ufw active, no openssh, no ~/.ssh; setup twice
#   ub-pre     Ubuntu, ufw active with 22/tcp already allowed, ~/.ssh 775 and an
#              authorized_keys (664, no final newline) holding another key
#   fd-fresh   Fedora, firewalld, no openssh-server, no ~/.ssh; setup twice
#   fd-haskey  Fedora, sshd installed, enabled but not running; authorized_keys
#              already holding the campaign key, unmarked
#   fd-off     Fedora, sshd installed, disabled and not running; set up after the scans
#   ub-refuse  Ubuntu: setup from a root shell must refuse and change nothing
#   ub-rc      Ubuntu with openssh-server removed but its config kept (dpkg "rc");
#              set up after the scans, undone with the rest
# Each PC is snapshotted (packages, users, ssh units, ~/.ssh tree with modes and
# hashes, /etc/sudoers.d, /etc/ssh, firewall rules, state dir), set up the real
# way (curl from serve.sh | sudo bash -s -- setup, as user lab), checked (LAB-PC
# line, key login and sudo -n from the driver side), scanned with scan.sh, then
# undone; the snapshot after undo must equal the one before.
# Needs rootless podman (the driver side runs in its network namespace) or
# rootful docker. Removes everything it made on exit.
# shellcheck disable=SC2015,SC2016,SC2086  # pass||fail lines; remote snippets; LAB_SSH_OPTS splits
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
camp=$(cd "$here/.." && pwd)
rt=${LAB_TEST_RUNTIME:-$(command -v podman >/dev/null && echo podman || echo docker)}
tag=pplt-$$
net=$tag
work=$(mktemp -d)
export LAB_RESULTS=$work/results LAB_KEY=$work/key/pipedpeer-lab_ed25519 LAB_PORT=8000
export LAB_SSH_OPTS="-F /dev/null -o UserKnownHostsFile=$work/known_hosts -o LogLevel=ERROR"
pcs=(ub-fresh ub-pre fd-fresh fd-haskey ub-refuse ub-rc fd-off)
fails=0

say()  { printf '\n=== %s\n' "$*"; }
pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; fails=$((fails + 1)); }

# The driver side must reach the container network: rootless podman keeps it
# in its own network namespace.
in_net() {
  if [[ $rt == podman && $(podman info --format '{{.Host.Security.Rootless}}') == true ]]; then
    podman unshare --rootless-netns "$@"
  else
    "$@"
  fi
}

cleanup() {
  set +e
  bash "$camp/serve.sh" stop >/dev/null 2>&1
  for p in "${pcs[@]}"; do $rt rm -f -t 0 "$tag-$p" >/dev/null 2>&1; done
  $rt network rm "$net" >/dev/null 2>&1
  rm -rf "$work"
}
trap cleanup EXIT

say "build images"
for os in ubuntu fedora; do
  $rt build -q -t "pipedpeer-labpc-$os" -f "$here/Dockerfile.$os" "$here" >"$work/build-$os.log" 2>&1 \
    || { cat "$work/build-$os.log"; echo "image build failed"; exit 1; }
done

$rt network create "$net" >/dev/null
gw=$($rt network inspect "$net" --format '{{range .Subnets}}{{.Gateway}}{{end}}')
subnet=$($rt network inspect "$net" --format '{{range .Subnets}}{{.Subnet}}{{end}}')
[[ -n $gw && -n $subnet ]] || { echo "cannot read network $net"; exit 1; }
echo "network $net $subnet, driver at $gw"

run_pc() {
  local name=$1 image=$2
  if [[ $rt == podman ]]; then
    podman run -d --name "$tag-$name" --hostname "$name" --network "$net" --memory 512m \
      --cap-add NET_ADMIN,NET_RAW --systemd=always "$image" >/dev/null
  else
    docker run -d --name "$tag-$name" --hostname "$name" --network "$net" --memory 512m \
      --cap-add NET_ADMIN --cap-add NET_RAW --cgroupns=host -v /sys/fs/cgroup:/sys/fs/cgroup:rw \
      --tmpfs /run --tmpfs /tmp "$image" /sbin/init >/dev/null
  fi
}
for p in "${pcs[@]}"; do
  case $p in ub-*) run_pc "$p" pipedpeer-labpc-ubuntu ;; fd-*) run_pc "$p" pipedpeer-labpc-fedora ;; esac
done
x()     { $rt exec "$tag-$1" bash -c "$2"; }        # as root
x_lab() { $rt exec -u lab -w /home/lab "$tag-$1" bash -c "$2"; }
x_lab_in() { $rt exec -i -u lab -w /home/lab "$tag-$1" bash -c "$2"; }   # stdin passed through
ip_of() { $rt inspect "$tag-$1" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'; }
for p in "${pcs[@]}"; do
  for _ in $(seq 60); do
    st=$(x "$p" 'systemctl is-system-running 2>/dev/null' || true)
    [[ $st == running || $st == degraded ]] && break
    sleep 0.5
  done
done

say "serve.sh"
out=$(in_net bash "$camp/serve.sh" "$gw")
echo "$out"
line=$(grep -E '^  curl ' <<<"$out" | sed 's/^  //')
[[ $line == "curl -fsS http://$gw:8000/lab-pc.sh | sudo bash -s -- setup" ]] \
  && pass "operator line: $line" || fail "operator line: '$line'"
grep -q "^DRIVER=\"$gw\"$" "$LAB_RESULTS/serve/lab-pc.sh" && pass "DRIVER filled in" || fail "DRIVER not filled in"
cmp -s "$LAB_RESULTS/serve/driver.pub" "$LAB_KEY.pub" && pass "driver.pub is the campaign key" || fail "driver.pub"
pub=$(cat "$LAB_KEY.pub")

say "prepare pre-existing state"
x ub-fresh 'ufw --force enable >/dev/null'
x ub-pre 'ufw --force enable >/dev/null && ufw allow 22/tcp >/dev/null'
x ub-refuse 'ufw --force enable >/dev/null'
x_lab ub-pre 'mkdir -p .ssh && chmod 775 .ssh && printf "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOtherKeyOtherKeyOtherKeyOtherKeyOtherKey0 other@pc" >.ssh/authorized_keys && chmod 664 .ssh/authorized_keys'
x_lab fd-haskey "mkdir -m 700 .ssh && printf '%s\n' '$pub' >.ssh/authorized_keys && chmod 600 .ssh/authorized_keys"
x ub-rc 'ufw --force enable >/dev/null && apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends openssh-server >/dev/null 2>&1 && apt-get remove -y -qq openssh-server >/dev/null 2>&1 && dpkg -l openssh-server | grep -q "^rc"'
# Both images start without an SSH server, so setup must install it.
for p in fd-haskey fd-off; do x "$p" 'dnf install -y -q openssh-server >/dev/null 2>&1'; done
x fd-haskey 'systemctl enable sshd >/dev/null 2>&1; systemctl stop sshd'
x fd-off 'systemctl disable sshd >/dev/null 2>&1; systemctl stop sshd'

snapshot() {
  x "$1" '
    echo "== packages"
    if command -v dpkg-query >/dev/null; then dpkg-query -W -f="\${Package} \${Version} \${db:Status-Abbrev}\n" | sort
    else rpm -qa --qf "%{NAME}-%{VERSION}-%{RELEASE}.%{ARCH}\n" | sort; fi
    echo "== users"; sha256sum /etc/passwd /etc/group /etc/shadow /etc/gshadow 2>/dev/null; cut -d: -f1 /etc/passwd
    echo "== units"; for u in ssh.service ssh.socket sshd.service; do echo "$u $(systemctl is-enabled $u 2>&1) $(systemctl is-active $u 2>&1)"; done
    echo "== home"; find /home/lab -path /home/lab/.cache -prune -o -printf "%p %M %u %g\n" | sort
    find /home/lab/.ssh -type f -exec sha256sum {} + 2>/dev/null | sort
    echo "== sudoers"; find /etc/sudoers.d -printf "%p %M %u %g\n" | sort; sha256sum /etc/sudoers.d/* | sort
    echo "== etc-ssh"; find /etc/ssh -printf "%p %M\n" 2>/dev/null | sort
    echo "== firewall"
    if command -v ufw >/dev/null; then ufw status verbose; sha256sum /etc/ufw/user.rules /etc/ufw/user6.rules; fi
    if command -v firewall-cmd >/dev/null; then firewall-cmd --list-all; find /etc/firewalld -type f -exec sha256sum {} + | sort; fi
    echo "== state"; ls -la /var/lib/pipedpeer-lab 2>&1
    true
  '
}
for p in "${pcs[@]}"; do snapshot "$p" >"$work/before-$p"; done

setup_line="curl -fsS http://$gw:8000/lab-pc.sh | sudo bash -s -- setup"
check_setup() {
  local p=$1 out=$2 ip mid last
  ip=$(ip_of "$p"); mid=$(x "$p" 'cat /etc/machine-id')
  last=$(tail -n1 <<<"$out")
  if [[ $last =~ ^LAB-PC\ host=$p\ ip=$ip\ user=lab\ cores=[0-9]+\ ram_gib=[0-9]+\ machine_id=$mid$ ]]; then
    pass "$p: $last"
  else
    fail "$p: last line '$last'"; echo "$out"
  fi
  if in_net ssh $LAB_SSH_OPTS -o BatchMode=yes -o StrictHostKeyChecking=accept-new -i "$LAB_KEY" "lab@$ip" 'sudo -n true && echo sudo-ok' 2>&1 | grep -qx sudo-ok; then
    pass "$p: key login from the driver and passwordless sudo"
  else
    fail "$p: key login or sudo -n"
  fi
}

say "refusals"
out=$(x ub-refuse "$setup_line" 2>&1 || true)
grep -q 'ERROR: the lab user is root' <<<"$out" && pass "root shell through sudo refused" || { fail "root via sudo not refused"; echo "$out"; }
out=$(x ub-refuse "curl -fsS http://$gw:8000/lab-pc.sh | bash -s -- setup" 2>&1 || true)
grep -q 'ERROR: ' <<<"$out" && pass "root shell without sudo refused: $(tail -n1 <<<"$out")" || { fail "root shell not refused"; echo "$out"; }
out=$(x_lab_in ub-refuse "sudo bash -s -- setup" <"$camp/lab-pc.sh" 2>&1 || true)
grep -q 'ERROR: DRIVER is not filled in' <<<"$out" && pass "unfilled DRIVER refused" || { fail "unfilled DRIVER"; echo "$out"; }
snapshot ub-refuse >"$work/after-ub-refuse"
diff -u "$work/before-ub-refuse" "$work/after-ub-refuse" && pass "ub-refuse unchanged" || fail "ub-refuse changed by a refused setup"

say "setup"
for p in ub-fresh ub-pre fd-fresh fd-haskey; do
  out=$(x_lab "$p" "$setup_line" 2>&1) || { fail "$p: setup exited non-zero"; echo "$out"; continue; }
  check_setup "$p" "$out"
  x "$p" 'cat /var/lib/pipedpeer-lab/state' >"$work/state1-$p"
  echo "--- $p state:"; sed 's/^/    /' "$work/state1-$p"
done
for p in ub-fresh fd-fresh; do
  out=$(x_lab "$p" "$setup_line" 2>&1) || { fail "$p: second setup exited non-zero"; echo "$out"; continue; }
  check_setup "$p" "$out"
  x "$p" 'cat /var/lib/pipedpeer-lab/state' >"$work/state2-$p"
  diff -u "$work/state1-$p" "$work/state2-$p" && pass "$p: second setup recorded nothing new" || fail "$p: second setup changed the state"
  [[ $(x_lab "$p" 'grep -c pipedpeer-lab .ssh/authorized_keys') == 1 ]] && pass "$p: one campaign key line" || fail "$p: duplicate key lines"
done
grep -q '^ufw 22/tcp$' "$work/state1-ub-pre" && fail "ub-pre: pre-existing ufw 22/tcp recorded" || pass "ub-pre: pre-existing ufw 22/tcp not recorded"
grep -q '^ufw 4242/tcp$' "$work/state1-ub-pre" && pass "ub-pre: ufw rules it added are recorded" || fail "ub-pre: ufw 4242/tcp not recorded"
grep -q '^fwd 4242/tcp$' "$work/state1-fd-fresh" && pass "fd-fresh: firewalld ports recorded" || fail "fd-fresh: no firewalld ports recorded"
grep -q '^ak' "$work/state1-fd-haskey" && fail "fd-haskey: unmarked key touched" || pass "fd-haskey: unmarked existing key left alone"
[[ $(x_lab ub-pre 'wc -l <.ssh/authorized_keys') == 2 ]] && pass "ub-pre: other key and campaign key on separate lines" || fail "ub-pre: key lines merged"
x ub-pre 'ufw status' | grep -q '^4243/udp .*ALLOW' && pass "ub-pre: ufw has 4243/udp" || fail "ub-pre: ufw lacks 4243/udp"
x fd-fresh 'firewall-cmd --query-port=4244/udp' >/dev/null && pass "fd-fresh: firewalld has 4244/udp" || fail "fd-fresh: firewalld lacks 4244/udp"

say "scan.sh"
scan() { in_net bash "$camp/scan.sh" "$subnet" --user lab "$@"; }
x fd-haskey 'systemctl stop sshd'                    # off during the first scan
out=$(scan) && echo "$out" || { fail "scan.sh failed"; echo "$out"; }
grep -q 'python3 TCP-22 sweep' <<<"$out" && pass "scan without nmap fell back to the python sweep" || \
  { grep -q '^scan: nmap -p22' <<<"$out" && pass "scan used nmap" || fail "scan: no sweep method reported"; }
inv=$LAB_RESULTS/inventory.ini
cat "$inv"; cat "$LAB_RESULTS/hosts.csv"
id_of() { awk -v ip="$1" '$0 ~ "ansible_host="ip" " {print $1}' "$inv"; }
first=$(sort -t. -k4,4n < <(for p in ub-fresh ub-pre fd-fresh; do ip_of "$p"; done) | head -n1)
[[ $(id_of "$first") == pc00 ]] && pass "lowest IP is pc00 on a first scan" || fail "pc00 is not the lowest IP"
[[ $(grep -c '^pc[0-9][0-9] ansible_host=' "$inv") == 3 ]] && pass "3 PCs found (refuse PC has no sshd, fd-haskey stopped)" || fail "expected 3 PCs"
python3 - "$inv" "$LAB_RESULTS/hosts.csv" "$LAB_KEY" <<'EOF' && pass "inventory and hosts.csv format" || fail "inventory or hosts.csv format"
import re, sys, csv
inv, hosts, key = sys.argv[1:]
lines = [l.rstrip("\n") for l in open(inv) if not l.startswith("#")]
text = "\n".join(lines)
assert re.search(r"^\[head\]\npc00 ansible_host=\S+ machine_id=[0-9a-f]{32}\n\n\[workers\]\n(pc\d\d ansible_host=\S+ machine_id=[0-9a-f]{32}\n)+\n\[all:vars\]\nansible_user=lab\nansible_ssh_private_key_file=" + re.escape(key) + r"\nansible_python_interpreter=auto_silent$", text), text
rows = list(csv.reader(open(hosts)))
assert rows[0] == ["pc_id", "ip", "hostname", "machine_id", "cores", "ram_gib"], rows[0]
assert len(rows) == 4 and all(len(r) == 6 for r in rows), rows
EOF
for p in ub-fresh ub-pre fd-fresh; do
  [[ $(grep "ansible_host=$(ip_of "$p") " "$inv" | sed 's/.*machine_id=//') == "$(x "$p" 'cat /etc/machine-id')" ]] \
    && pass "$p: machine_id matches" || fail "$p: machine_id"
done
cp "$inv" "$work/inv1"
x fd-haskey 'systemctl start sshd'
x ub-fresh 'systemctl stop ssh'                     # missing from the next scan
scan >/dev/null
hk=$(ip_of fd-haskey)
[[ $(id_of "$hk") == pc03 ]] && pass "new PC appended as pc03" || fail "new PC id $(id_of "$hk")"
for p in ub-pre fd-fresh; do
  [[ $(id_of "$(ip_of "$p")") == "$(awk -v ip="$(ip_of "$p")" '$0 ~ "ansible_host="ip" " {print $1}' "$work/inv1")" ]] \
    && pass "$p keeps its id on rescan" || fail "$p changed id"
done
x ub-fresh 'systemctl start ssh'
scan >/dev/null
[[ $(id_of "$(ip_of ub-fresh)") == "$(awk -v ip="$(ip_of ub-fresh)" '$0 ~ "ansible_host="ip" " {print $1}' "$work/inv1")" ]] \
  && pass "ub-fresh gets its old id back after missing a scan" || fail "ub-fresh lost its id"
old0=$(awk '/^pc00 /{print $2}' "$inv" | sed 's/ansible_host=//')
scan --head "$hk" >/dev/null
[[ $(id_of "$hk") == pc00 ]] && pass "--head pins pc00" || fail "--head did not pin pc00"
[[ $(id_of "$old0") == pc04 ]] && pass "former pc00 moved to pc04" || fail "former pc00 got $(id_of "$old0")"
scan >/dev/null
[[ $(id_of "$hk") == pc00 ]] && pass "head pin survives a rescan without --head" || fail "head pin lost"
cat "$inv"

say "setup from dpkg rc state, and with sshd disabled"
for p in ub-rc fd-off; do
  out=$(x_lab "$p" "$setup_line" 2>&1) && check_setup "$p" "$out" || { fail "$p: setup exited non-zero"; echo "$out"; }
done
grep -q '^pkg-rc apt openssh-server$' <(x ub-rc 'cat /var/lib/pipedpeer-lab/state') && pass "ub-rc: reinstall from rc recorded as pkg-rc" || fail "ub-rc: no pkg-rc line"

say "undo"
# Two through the copy setup saved on the PC, two through the driver.
for p in ub-fresh ub-pre fd-fresh fd-haskey ub-rc fd-off; do
  case $p in
    ub-fresh|fd-fresh) undo_cmd='sudo bash /var/lib/pipedpeer-lab/lab-pc.sh undo' ;;
    *) undo_cmd="curl -fsS http://$gw:8000/lab-pc.sh | sudo bash -s -- undo" ;;
  esac
  out=$(x_lab "$p" "$undo_cmd" 2>&1) || { fail "$p: undo exited non-zero"; echo "$out"; }
  grep -q WARNING <<<"$out" && { fail "$p: undo warned"; echo "$out"; }
  snapshot "$p" >"$work/after-$p"
  if diff -u "$work/before-$p" "$work/after-$p"; then pass "$p: byte-identical after undo"; else fail "$p: differs after undo"; fi
done

bash "$camp/serve.sh" stop >/dev/null
if [[ ! -f $LAB_RESULTS/serve.pid ]] && ! in_net python3 -c 'import urllib.request,sys; urllib.request.urlopen(sys.argv[1], timeout=2)' "http://$gw:8000/driver.pub" 2>/dev/null; then
  pass "serve.sh stop: server gone"
else
  fail "serve.sh stop: still serving"
fi

echo
if ((fails)); then echo "lab-pc-test: $fails FAILED"; exit 1; fi
echo "lab-pc-test: all passed"
