#!/usr/bin/env bash
# End-to-end test of bootstrap-ssh.sh on systemd containers (the images of
# lab-pc-test.sh). Two lab PC stand-ins, Ubuntu and Fedora, each already
# running sshd with password login for user "lab" and a sudo that asks for
# that password: the state of a lab nobody has visited yet.
#   scripts/campaign/test/bootstrap-ssh-test.sh
#   LAB_BOOT_MODE=sshpass scripts/campaign/test/bootstrap-ssh-test.sh   # the fallback
# Checks: a wrong password sets up nothing and says ERROR; the right one,
# given once on stdin to a subnet sweep, gives a LAB-PC line per PC, key
# login and sudo -n from the driver; the password is in no file on either
# side (results, the PCs' homes, state dir, logs, journal); undo restores.
# Needs rootless podman or rootful docker. Removes everything it made on exit.
# shellcheck disable=SC2015,SC2016,SC2086
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
camp=$(cd "$here/.." && pwd)
rt=${LAB_TEST_RUNTIME:-$(command -v podman >/dev/null && echo podman || echo docker)}
tag=ppbs-$$
net=$tag
work=$(mktemp -d)
export LAB_RESULTS=$work/results LAB_KEY=$work/key/pipedpeer-lab_ed25519 LAB_PORT=8000
export LAB_SSH_OPTS="-F /dev/null -o UserKnownHostsFile=$work/known_hosts -o LogLevel=ERROR"
pcs=(ub fd)
pw="bs-$(od -An -N9 -tx1 /dev/urandom | tr -d ' \n')"
fails=0

say()  { printf '\n=== %s\n' "$*"; }
pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; fails=$((fails + 1)); }

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
run_pc ub pipedpeer-labpc-ubuntu
run_pc fd pipedpeer-labpc-fedora
x()     { $rt exec "$tag-$1" bash -c "$2"; }
x_in()  { $rt exec -i "$tag-$1" bash -c "$2"; }
ip_of() { $rt inspect "$tag-$1" --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'; }
for p in "${pcs[@]}"; do
  for _ in $(seq 60); do
    st=$(x "$p" 'systemctl is-system-running 2>/dev/null' || true)
    [[ $st == running || $st == degraded ]] && break
    sleep 0.5
  done
done

say "make them a lab nobody visited: sshd with password login, sudo asks for the password"
x ub 'apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends openssh-server >/dev/null 2>&1; systemctl enable --now ssh >/dev/null 2>&1'
x fd 'dnf install -y -q openssh-server >/dev/null 2>&1; systemctl enable --now sshd >/dev/null 2>&1'
for p in "${pcs[@]}"; do
  printf 'lab:%s\n' "$pw" | x_in "$p" 'chpasswd'
  x "$p" 'printf "PasswordAuthentication yes\nKbdInteractiveAuthentication yes\n" >/etc/ssh/sshd_config.d/00-test-password.conf
          echo "lab ALL=(ALL) ALL" >/etc/sudoers.d/00-lab-test; chmod 440 /etc/sudoers.d/00-lab-test
          systemctl restart ssh 2>/dev/null || systemctl restart sshd'
  x "$p" 'sudo -u lab sudo -n true' 2>/dev/null && fail "$p: sudo is passwordless before setup" || pass "$p: sudo asks for the password"
done

say "serve.sh"
in_net bash "$camp/serve.sh" "$gw" | grep -E '^  curl '

say "wrong password"
set +e
out=$(printf 'not-%s\n' "$pw" | in_net bash "$camp/bootstrap-ssh.sh" "$subnet" --user lab 2>&1); rc=$?
set -e
echo "$out"
((rc != 0)) && pass "wrong password: non-zero exit" || fail "wrong password: exit 0"
[[ $(grep -c ' ERROR ' <<<"$out") == 2 ]] && pass "wrong password: ERROR for both PCs" || fail "wrong password: expected 2 ERROR lines"
for p in "${pcs[@]}"; do
  x "$p" 'test -e /var/lib/pipedpeer-lab/state' && fail "$p: wrong password changed the PC" || pass "$p: wrong password changed nothing"
done

say "right password, once, subnet sweep"
start=$(date +%s)
set +e
out=$(printf '%s\n' "$pw" | in_net bash "$camp/bootstrap-ssh.sh" "$subnet" --user lab 2>&1); rc=$?
set -e
echo "$out"
echo "took $(($(date +%s) - start)) s"
((rc == 0)) && pass "bootstrap exit 0" || fail "bootstrap exit $rc"
for p in "${pcs[@]}"; do
  ip=$(ip_of "$p"); mid=$(x "$p" 'cat /etc/machine-id')
  grep -Eq "^$ip LAB-PC host=$p ip=$ip user=lab cores=[0-9]+ ram_gib=[0-9]+ machine_id=$mid$" <<<"$out" \
    && pass "$p: LAB-PC line" || fail "$p: no LAB-PC line"
  if in_net ssh $LAB_SSH_OPTS -o BatchMode=yes -o StrictHostKeyChecking=accept-new -i "$LAB_KEY" "lab@$ip" 'sudo -n true && echo sudo-ok' 2>&1 | grep -qx sudo-ok; then
    pass "$p: key login from the driver and passwordless sudo"
  else
    fail "$p: key login or sudo -n"
  fi
done
grep -c ' LAB-PC ' "$LAB_RESULTS/bootstrap.txt" | grep -qx 2 && pass "bootstrap.txt holds the 2 LAB-PC lines" || fail "bootstrap.txt"

say "the password is in no file"
grep -rqaF -- "$pw" "$LAB_RESULTS" "$work/known_hosts" 2>/dev/null && fail "password found under the driver's results" || pass "not in results/ or known_hosts"
for p in "${pcs[@]}"; do
  printf '%s\n' "$pw" | x_in "$p" 'IFS= read -r s; grep -rqaF -- "$s" /home/lab /root /var/lib/pipedpeer-lab /var/log /tmp /etc/sudoers.d 2>/dev/null && exit 1
    journalctl --no-pager 2>/dev/null | grep -qaF -- "$s" && exit 1; exit 0' \
    && pass "$p: not in homes, state dir, /var/log, /tmp, journal" || fail "$p: password found on the PC"
done

say "undo"
for p in "${pcs[@]}"; do
  ip=$(ip_of "$p")
  in_net ssh $LAB_SSH_OPTS -o BatchMode=yes -i "$LAB_KEY" "lab@$ip" "curl -fsS http://$gw:8000/lab-pc.sh | sudo -n bash -s -- undo" >/dev/null 2>&1 \
    && pass "$p: undo through the key" || fail "$p: undo"
  x "$p" 'sudo -u lab sudo -n true' 2>/dev/null && fail "$p: still passwordless sudo after undo" || pass "$p: sudo asks for the password again"
done

echo
if ((fails)); then echo "bootstrap-ssh-test: $fails FAILED"; exit 1; fi
echo "bootstrap-ssh-test: all passed"
