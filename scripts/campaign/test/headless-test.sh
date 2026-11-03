#!/usr/bin/env bash
# Rehearsal of the headless lab day on systemd containers (HEADLESS.md):
#   scripts/campaign/test/headless-test.sh [--keep]
# Four lab PC stand-ins on their own podman network (LAB_PCS=4):
#   pc00  Ubuntu 24.04, ufw active, no python3-venv: runs lab.sh head
#   w1    Ubuntu, ufw active          w2  Fedora, firewalld      w3  Ubuntu
# The host side stands in for the internet: a local http server for GitHub
# Pages (lab.sh filled in the way publish.sh does, lab-secrets.enc encrypted
# with a test passphrase) and for codeload (the working tree as a tarball,
# with rehearsal stand-ins from headless-stubs/ for what needs crun, Nix or
# the runner), and `rclone serve s3` for R2.
#
# It snapshots every PC, runs the worker line on w1-w3, then the head line on
# pc00 through a pty (the passphrase is read from /dev/tty), kills the
# pipedpeer-lab-day unit midway through the experiments, runs the head line
# again, and checks: the day resumed (finished steps skipped, the cut-off
# experiment marked interrupted and run again), R2 has the results with the
# local count, the summary says ALL DONE, and every PC's snapshot after the
# day equals the one before. Containers are removed at the end; images stay.
# Needs rootless podman (the host side runs in its network namespace).
# shellcheck disable=SC2015,SC2016  # pass||fail lines; remote snippets
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
camp=$(cd "$here/.." && pwd)
repo=$(cd "$camp/../.." && pwd)
keep=0; [[ ${1:-} == --keep ]] && keep=1
rt=podman
tag=pphl-$$
net=$tag
work=$(mktemp -d)
port=8100 s3port=9000
pcs=(pc00 w1 w2 w3)
pass_phrase="rehearsal horse battery staple lab day"
fails=0

say()  { printf '\n=== %s\n' "$*"; }
pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; fails=$((fails + 1)); }

in_net() { podman unshare --rootless-netns "$@"; }

cleanup() {
  set +e
  [[ -f $work/http.pid ]] && in_net kill "$(cat "$work/http.pid")" 2>/dev/null
  [[ -f $work/s3.pid ]] && in_net kill "$(cat "$work/s3.pid")" 2>/dev/null
  if ((keep == 0)); then
    for p in "${pcs[@]}"; do $rt rm -f -t 0 "$tag-$p" >/dev/null 2>&1; done
    $rt network rm "$net" >/dev/null 2>&1
    rm -rf "$work"
  else
    echo "kept: containers $tag-*, network $net, $work"
  fi
}
trap cleanup EXIT

say "build images"
for os in ubuntu fedora; do
  $rt build -q -t "pipedpeer-labpc-$os" -f "$here/Dockerfile.$os" "$here" >"$work/build-$os.log" 2>&1 \
    || { cat "$work/build-$os.log"; echo "image build failed"; exit 1; }
done
$rt network create "$net" >/dev/null
gw=$($rt network inspect "$net" --format '{{range .Subnets}}{{.Gateway}}{{end}}')
site=http://$gw:$port
echo "network $net, site $site, S3 http://$gw:$s3port"

run_pc() {  # run_pc <name> <image> <memory>
  podman run -d --name "$tag-$1" --hostname "$1" --network "$net" --memory "$3" \
    --cap-add NET_ADMIN,NET_RAW --systemd=always "$2" >/dev/null
}
run_pc pc00 pipedpeer-labpc-ubuntu 1g
run_pc w1 pipedpeer-labpc-ubuntu 768m
run_pc w2 pipedpeer-labpc-fedora 768m
run_pc w3 pipedpeer-labpc-ubuntu 768m
x()     { $rt exec "$tag-$1" bash -c "$2"; }
x_lab() { $rt exec -u lab -w /home/lab "$tag-$1" bash -c "$2"; }
for p in "${pcs[@]}"; do
  for _ in $(seq 60); do
    st=$(x "$p" 'systemctl is-system-running 2>/dev/null' || true)
    [[ $st == running || $st == degraded ]] && break
    sleep 0.5
  done
done
for p in pc00 w1 w3; do x "$p" 'ufw --force enable >/dev/null'; done

say "stand-ins for Pages, codeload and R2"
mkdir -p "$work/site" "$work/cfg" "$work/s3/pipedpeer-lab" "$work/tree"
ssh-keygen -q -t ed25519 -N '' -C test -f "$work/cfg/campaign_ed25519"
printf 'R2_ACCOUNT_ID=rehearsal\nR2_ACCESS_KEY_ID=TESTKEY\nR2_SECRET_ACCESS_KEY=TESTSECRET\n' >"$work/cfg/r2.env"
chmod 600 "$work/cfg/r2.env"
# The code tarball: the working tree (committed or not), with the stand-ins.
commit=$(git -C "$repo" rev-parse HEAD)
(cd "$repo" && git ls-files -co --exclude-standard -z | tar --null -T - -cf - 2>/dev/null) | tar -xf - -C "$work/tree"
s=$camp/test/headless-stubs
cp "$s/install-pipedpeer.sh" "$work/tree/scripts/install-pipedpeer.sh"
cp "$s/env-install.sh" "$work/tree/scripts/campaign/env/install.sh"
cp "$s/calibrate.sh" "$work/tree/scripts/campaign/calibrate.sh"
# lab/runner's run.sh and analyse.py need real pipedpeer, Ray and Dask: stand-ins.
cp "$s/run.sh" "$work/tree/scripts/campaign/run.sh"
cp "$s/analyse.py" "$work/tree/scripts/campaign/analyse.py"
tar -C "$work/tree/.." -czf "$work/site/code.tar.gz" --transform "s|^tree|Pipedpeer-$commit|" tree
# lab.sh and the bundle, as publish.sh makes them.
bash "$camp/lab-fill.sh" fill "$camp/lab.sh" "$work/site/lab.sh" "$(cat "$work/cfg/campaign_ed25519.pub")" "$commit" "$site" rehearsal
bash "$camp/lab-fill.sh" check "$work/site/lab.sh" >/dev/null && pass "site lab.sh is lab.sh with the published lines filled" || fail "site lab.sh"
tar -C "$work/cfg" -czf - campaign_ed25519 r2.env |
  openssl enc -aes-256-cbc -pbkdf2 -iter 600000 -md sha256 -salt -out "$work/site/lab-secrets.enc" -pass fd:3 3< <(printf '%s\n' "$pass_phrase")
in_net sh -c "cd '$work/site' && exec python3 -m http.server $port --bind $gw >'$work/http.log' 2>&1 & echo \$! >'$work/http.pid'"
# shellcheck source=../otel/lib.sh
( export PIPEDPEER_LAB_HOME=$work/rclone; source "$camp/otel/lib.sh"; lab_install_rclone >/dev/null )
in_net sh -c "exec '$work/rclone/bin/rclone' serve s3 --auth-key TESTKEY,TESTSECRET --addr $gw:$s3port '$work/s3' >'$work/s3.log' 2>&1 & echo \$! >'$work/s3.pid'"
sleep 2
in_net curl -fsS -o /dev/null "$site/lab.sh" && pass "site serves lab.sh" || fail "site down"

snapshot() {
  x "$1" '
    echo "== packages"
    if command -v dpkg-query >/dev/null; then dpkg-query -W -f="\${Package} \${Version} \${db:Status-Abbrev}\n" | sort
    else rpm -qa --qf "%{NAME}-%{VERSION}-%{RELEASE}.%{ARCH}\n" | sort; fi
    echo "== users"; sha256sum /etc/passwd /etc/group /etc/shadow /etc/gshadow 2>/dev/null
    echo "== unit files"; systemctl list-unit-files --no-legend 2>/dev/null | sort
    echo "== ssh units"; for u in ssh.service ssh.socket sshd.service; do echo "$u $(systemctl is-enabled $u 2>&1) $(systemctl is-active $u 2>&1)"; done
    echo "== home"; find /home/lab -printf "%p %M %u %g\n" | sort
    find /home/lab -type f -exec sha256sum {} + 2>/dev/null | sort
    echo "== sudoers"; find /etc/sudoers.d -printf "%p %M %u %g\n" | sort; sha256sum /etc/sudoers.d/* | sort
    echo "== etc-ssh"; find /etc/ssh -printf "%p %M\n" 2>/dev/null | sort
    echo "== bin"; ls -A /usr/local/bin
    echo "== tmp"; find /tmp /var/tmp -maxdepth 1 -user lab 2>/dev/null | sort
    echo "== nix"; ls -d /nix 2>&1
    echo "== run"; ls -d /run/pipedpeer-lab /var/lib/pipedpeer-lab 2>&1
    echo "== firewall"
    if command -v ufw >/dev/null; then ufw status verbose; sha256sum /etc/ufw/user.rules /etc/ufw/user6.rules; fi
    if command -v firewall-cmd >/dev/null; then firewall-cmd --list-all; firewall-cmd --zone=trusted --list-all; fi
    true
  '
}
for p in "${pcs[@]}"; do snapshot "$p" >"$work/before-$p"; done

say "worker line on w1-w3"
for p in w1 w2 w3; do
  out=$(x_lab "$p" "curl -fsSL $site/lab.sh | sudo bash" 2>&1) || { fail "$p: worker setup exited non-zero"; echo "$out"; continue; }
  grep -q "DONE: this PC is ready, you can leave it." <<<"$out" && pass "$p: $(grep '^LAB-PC' <<<"$out")" || { fail "$p: no DONE"; echo "$out"; }
done

head_line="curl -fsSL $site/lab.sh | sudo LAB_CODE_URL=$site/code.tar.gz PIPEDPEER_LAB_S3_ENDPOINT=http://$gw:$s3port LAB_DISCOVER_TIMEOUT=600 bash -s -- head 4"
# The passphrase goes in through a pty: lab.sh reads it from /dev/tty.
head_run() { printf '%s\n' "$pass_phrase" | $rt exec -i -u lab -w /home/lab "$tag-pc00" script -qec "$head_line" /dev/null; }

say "head line on pc00 (first run, killed midway)"
head_run >"$work/head1.log" 2>&1 &
hp=$!
killed=0
for _ in $(seq 900); do
  if x pc00 'grep -q "^e4 " /home/lab/pipedpeer-lab/exp-started 2>/dev/null'; then
    sleep 4   # e4's runs have started
    x pc00 'systemctl kill -s KILL pipedpeer-lab-day' && killed=1
    break
  fi
  kill -0 "$hp" 2>/dev/null || break
  sleep 2
done
wait "$hp" || true
sed 's/^/    | /' "$work/head1.log" | tail -n 40
((killed)) && pass "killed the lab day unit during e4" || { fail "the day never reached e4"; cat "$work/head1.log"; exit 1; }
grep -q 'The lab day stopped before the end' "$work/head1.log" && pass "head said the day stopped and how to continue" || fail "no stop banner"
grep -q 'Campaign passphrase:' "$work/head1.log" && pass "passphrase asked on the terminal" || fail "no passphrase prompt"
x pc00 'cat /home/lab/pipedpeer-lab/done' >"$work/ledger1"
echo "ledger after the kill: $(tr '\n' ' ' <"$work/ledger1")"
out=$(x_lab pc00 "curl -fsSL $site/lab.sh | sudo bash -s -- status" 2>&1)
grep -q '^phase:' <<<"$out" && pass "status shows the phase: $(grep '^now:' <<<"$out")" || { fail "status"; echo "$out"; }

say "head line again (resume)"
head_run >"$work/head2.log" 2>&1 || true
sed 's/^/    | /' "$work/head2.log" | tail -n 45
for s in discover prep.nixenv calibrate exp.e1 exp.e1b; do
  grep -q "$s: done before, skipped" "$work/head2.log" && pass "resume skipped $s" || fail "resume did not skip $s"
done
grep -q 'e4 was cut off before; running it again' "$work/head2.log" && pass "e4 run again after the kill" || fail "e4 not marked as cut off"
grep -q 'ALL DONE, the lab PCs are back as they were. You can leave.' "$work/head2.log" && pass "summary: ALL DONE" || fail "no ALL DONE"

say "R2 stand-in"
raw=$work/s3/pipedpeer-lab/raw
for f in runs.jsonl attempts.jsonl machines.csv network.csv clocks.txt workloads.md workloads.json result-cache.json inventory.ini logs/day/lab-day.log e1.csv; do
  [[ -s $raw/$f ]] && pass "R2 has raw/$f" || fail "R2 lacks raw/$f"
done
ls "$work/s3/pipedpeer-lab/otel" >/dev/null 2>&1 && pass "R2 has otel/ ($(find "$work/s3/pipedpeer-lab/otel" -type f | wc -l) objects)" || fail "no otel/ objects in R2"
python3 - "$raw" <<'EOF' && pass "attempts: e4 interrupted then ok; one ok run of every experiment" || fail "attempts.jsonl"
import json, sys, collections
raw = sys.argv[1]
att = [json.loads(l) for l in open(raw + "/attempts.jsonl")]
st = collections.defaultdict(list)
for a in att:
    st[a["experiment"]].append(a["status"])
print(dict(st))
assert st["e4"] == ["interrupted", "ok"], st["e4"]
assert all(v[-1] == "ok" for v in st.values()), st
assert sorted(st) == sorted(["e1", "e1b", "e4", "e5", "e6", "e7", "e8", "e12", "e13", "e15"]), sorted(st)
runs = [json.loads(l) for l in open(raw + "/runs.jsonl")]
e1 = [r for r in runs if r["experiment"] == "e1"]
assert len(e1) == 3 * 3 * 4, len(e1)   # N 1,4,16 x 3 systems x (1 warm-up + 3): once, not twice
cache = json.load(open(raw + "/result-cache.json"))
assert {k for k in cache if k.startswith("W1|")} == {"W1|BENCH_ITEMS=40", "W1|BENCH_ITEMS=160", "W1|BENCH_ITEMS=640"}, cache
assert all(v["source"].startswith("plain-precomputed:") for v in cache.values()), cache
EOF
python3 - "$raw/timeline.jsonl" <<'EOF' && pass "timeline.jsonl has every E14 event the day writes" || fail "timeline.jsonl (E14)"
import json, sys
ev = {json.loads(l)["event"] for l in open(sys.argv[1])}
need = {"head_start", "setup_start", "labpc", "ssh_ok", "pc_map", "install_start", "install_end", "join_end", "cluster_ready"}
assert need <= ev, sorted(need - ev)
EOF
grep -q 'calibrated on pc01' "$raw/workloads.json" && pass "workloads.json calibrated on pc01" || fail "workloads.json not calibrated"
grep -q '^| W1 | `BENCH_ITEMS=240`' "$raw/workloads.md" && pass "workloads.md sizes table rewritten" || fail "workloads.md table"
grep -q 'local:  *\([0-9]*\) ' "$work/head2.log" && pass "final count: $(grep -E '^(local|remote):' "$work/head2.log" | tr '\n' ' ')" || fail "no final count"

say "every PC as it was"
for p in "${pcs[@]}"; do
  snapshot "$p" >"$work/after-$p"
  if diff -u "$work/before-$p" "$work/after-$p"; then pass "$p: identical after the day"; else fail "$p: differs after the day"; fi
done

echo
if ((fails)); then echo "headless-test: $fails FAILED"; exit 1; fi
echo "headless-test: all passed"
