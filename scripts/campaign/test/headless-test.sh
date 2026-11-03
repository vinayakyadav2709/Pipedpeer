#!/usr/bin/env bash
# Rehearsal of the whole headless lab day on systemd containers (HEADLESS.md):
#   scripts/campaign/test/headless-test.sh [--keep]
# Four lab PC stand-ins on their own podman network (LAB_PCS=4), 2 CPUs each:
#   pc00  Ubuntu 24.04, ufw active, no python3-venv: runs lab.sh head
#   w1    Ubuntu, ufw active          w2  Fedora, firewalld      w3  Ubuntu
#
# Everything on the PCs is the real thing: lab.sh, the passphrase on
# /dev/tty, the systemd unit, scan.sh, Ansible over SSH, pipedpeer (built
# from this checkout, installed by install-pipedpeer.sh with its checksum,
# `pipedpeer setup -y` putting in Nix with the Determinate installer and
# crun), the Nix env distributed from pc00's binary cache, calibrate.sh,
# run.sh with real Ray, Dask and torchrun, analyse.py, every trace
# (collectors, pp-poll.py, tcpdump), sync-r2.sh through ingest.sh, cleanup
# and undo. What stands in, and why (HEADLESS.md, "Rehearsal"):
#   - The host side plays the internet: a local http server for GitHub Pages,
#     codeload and the pipedpeer release, and test/ingest-standin.py (the
#     ingest Worker's routes and caps, a directory for the bucket) so no
#     rehearsal data reaches the real R2 bucket.
#   - pc00's /nix is a copy of the pipedpeer-lab-nix volume (PL_NIX_VOLUME),
#     where the 2.5 GiB campaign env is already built, so pc00 "had Nix"
#     and does not download the env from cache.nixos.org. The workers start
#     without Nix and get everything else for real.
#   - Containers see all the host's CPUs: --cpus 2, PL_NPROC=2 and
#     PYTHON_CPU_COUNT=2 make them 2-core PCs.
#   - test/profile-rehearsal.tsv: N to what four PCs fit, one repeat, a 10 s
#     strong target for calibration.
#
# It snapshots every PC, runs the worker line on w1-w3, then the head line on
# pc00 through a pty, kills the pipedpeer-lab-day unit during e4, runs the
# head line again, and checks: the resume, the uploads, the traces, the
# summary, and that every PC's snapshot after the day equals the one before.
# Needs rootless podman; about 6 GB of RAM (pc00 2 GB: it is the head of everything) and 25 GB of disk.
# shellcheck disable=SC2015,SC2016  # pass||fail lines; remote snippets
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
camp=$(cd "$here/.." && pwd)
repo=$(cd "$camp/../.." && pwd)
keep=0; [[ ${1:-} == --keep ]] && keep=1
rt=podman
tag=pphl-$$
net=$tag
# On disk, not /tmp: the bucket stand-in and the pc00 store copy are gigabytes.
work=$(mktemp -d -p "${XDG_CACHE_HOME:-$HOME/.cache}" pipedpeer-headless.XXXXXX)
port=8100 ingest_port=8101
pcs=(pc00 w1 w2 w3)
pass_phrase="rehearsal horse battery staple lab day"
token=$(head -c 24 /dev/urandom | base64 | tr -d '/+=')
nixvol_src=${PL_NIX_VOLUME:-pipedpeer-lab-nix}
nixvol=$tag-nix
fails=0
t_start=$(date +%s)

say()  { printf '\n=== [%s] %s\n' "$(date +%T)" "$*"; }
pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*"; fails=$((fails + 1)); }

in_net() { podman unshare --rootless-netns "$@"; }

cleanup() {
  set +e
  [[ -f $work/http.pid ]] && in_net kill "$(cat "$work/http.pid")" 2>/dev/null
  [[ -f $work/ingest.pid ]] && in_net kill "$(cat "$work/ingest.pid")" 2>/dev/null
  if ((keep == 0)); then
    for p in "${pcs[@]}"; do $rt rm -f -t 0 "$tag-$p" >/dev/null 2>&1; done
    $rt network rm "$net" >/dev/null 2>&1
    $rt volume rm -f "$nixvol" >/dev/null 2>&1
    # The bucket stand-in and the store copy belong to container uids.
    $rt unshare rm -rf "$work"
  else
    echo "kept: containers $tag-*, network $net, volume $nixvol, $work"
  fi
}
trap cleanup EXIT

# Memory: every container is capped (no swap); the set waits for room.
source "$repo/scripts/lib/lab.sh"
pp_lab_mem_check "$(( (2048 + 3 * 1280) * 1048576 ))" headless-test

say "build images and pipedpeer"
for os in ubuntu fedora; do
  $rt build -q -t "pipedpeer-labpc-$os" -f "$here/Dockerfile.$os" "$here" >"$work/build-$os.log" 2>&1 \
    || { cat "$work/build-$os.log"; echo "image build failed"; exit 1; }
done
rel=$work/site/releases/download/lab-rehearsal
mkdir -p "$rel"
# As publish.sh builds the lab's pipedpeer, served as its release lab-rehearsal.
(cd "$repo/src" && CGO_ENABLED=0 GOFLAGS=-buildvcs=false go build -trimpath -ldflags "-s -w -X main.version=lab-rehearsal" -o "$rel/pipedpeer-linux-amd64" .)
(cd "$rel" && sha256sum pipedpeer-linux-amd64 >checksums.txt)
echo "pipedpeer $("$rel/pipedpeer-linux-amd64" --version 2>&1 | tail -n1)"

say "pc00's /nix: a copy of the $nixvol_src volume"
src_mp=$($rt volume inspect "$nixvol_src" --format '{{.Mountpoint}}') || { echo "no volume $nixvol_src"; exit 1; }
$rt volume create "$nixvol" >/dev/null
dst_mp=$($rt volume inspect "$nixvol" --format '{{.Mountpoint}}')
$rt unshare cp -a "$src_mp/." "$dst_mp/"
echo "copied $(du -sh "$dst_mp" | cut -f1)"

$rt network create "$net" >/dev/null
gw=$($rt network inspect "$net" --format '{{range .Subnets}}{{.Gateway}}{{end}}')
site=http://$gw:$port
echo "network $net, site $site, ingest http://$gw:$ingest_port"

run_pc() {  # run_pc <name> <image> <memory> [podman args]
  local n=$1 img=$2 mem=$3; shift 3
  podman run -d --name "$tag-$n" --hostname "$n" --network "$net" --memory "$mem" --memory-swap "$mem" \
    --cpus 2 --shm-size 256m --pids-limit 32768 --privileged --systemd=always "$@" "$img" >/dev/null
}
run_pc pc00 pipedpeer-labpc-ubuntu 2048m -v "$nixvol:/nix"
run_pc w1 pipedpeer-labpc-ubuntu 1280m
run_pc w2 pipedpeer-labpc-fedora 1280m
run_pc w3 pipedpeer-labpc-ubuntu 1280m
x()     { $rt exec "$tag-$1" bash -c "$2"; }
x_lab() { $rt exec -u lab -w /home/lab "$tag-$1" bash -c "$2"; }
for p in "${pcs[@]}"; do
  for _ in $(seq 60); do
    st=$(x "$p" 'systemctl is-system-running 2>/dev/null' || true)
    [[ $st == running || $st == degraded ]] && break
    sleep 0.5
  done
  # Each PC its own machine id, as on real PCs (the image ships one, and the
  # daemon's machine key hashes it).
  # journald writes under the machine id it started with: restart it, or
  # journalctl finds no journal and the head line shows no log.
  x "$p" 'tr -d - </proc/sys/kernel/random/uuid >/etc/machine-id; systemctl restart systemd-journald'
  # 2-core PCs for Python (multiprocessing, joblib) in every session and unit.
  x "$p" 'echo PYTHON_CPU_COUNT=2 >>/etc/environment; mkdir -p /etc/systemd/system.conf.d
    printf "[Manager]\nDefaultEnvironment=PYTHON_CPU_COUNT=2\n" >/etc/systemd/system.conf.d/cpus.conf; systemctl daemon-reexec'
done
for p in pc00 w1 w3; do x "$p" 'ufw --force enable >/dev/null'; done
# A lab PC that already had single-user Nix, with the env built.
x pc00 'chown -R lab:lab /nix'

say "stand-ins for Pages, codeload, the release and the ingest Worker"
mkdir -p "$work/cfg" "$work/bucket" "$work/tree"
ssh-keygen -q -t ed25519 -N '' -C test -f "$work/cfg/campaign_ed25519"
printf 'INGEST_URL=http://%s:%s\nINGEST_TOKEN=%s\n' "$gw" "$ingest_port" "$token" >"$work/cfg/r2.env"
chmod 600 "$work/cfg/r2.env"
# The code tarball: the working tree (committed or not), as codeload serves it.
commit=$(git -C "$repo" rev-parse HEAD)
(cd "$repo" && git ls-files -co --exclude-standard -z | tar --null -T - -cf - 2>/dev/null) | tar -xf - -C "$work/tree"
rm -rf "$work/tree/results/lab/rehearsal" "$work/tree/results/lab/runs.jsonl"
tar -C "$work" -czf "$work/site/code.tar.gz" --transform "s|^tree|Pipedpeer-$commit|" tree
bash "$camp/lab-fill.sh" fill "$camp/lab.sh" "$work/site/lab.sh" "$(cat "$work/cfg/campaign_ed25519.pub")" "$commit" "$site" lab-rehearsal
bash "$camp/lab-fill.sh" check "$work/site/lab.sh" >/dev/null && pass "site lab.sh is lab.sh with the published lines filled" || fail "site lab.sh"
tar -C "$work/cfg" -czf - campaign_ed25519 r2.env |
  openssl enc -aes-256-cbc -pbkdf2 -iter 600000 -md sha256 -salt -out "$work/site/lab-secrets.enc" -pass fd:3 3< <(printf '%s\n' "$pass_phrase")
in_net sh -c "cd '$work/site' && exec python3 -m http.server $port --bind $gw >'$work/http.log' 2>&1 & echo \$! >'$work/http.pid'"
in_net sh -c "INGEST_TOKEN='$token' exec python3 '$here/ingest-standin.py' $gw $ingest_port '$work/bucket' >'$work/ingest.log' 2>&1 & echo \$! >'$work/ingest.pid'"
sleep 2
in_net curl -fsS -o /dev/null "$site/lab.sh" && pass "site serves lab.sh" || fail "site down"
in_net env INGEST_URL="http://$gw:$ingest_port" INGEST_TOKEN="$token" bash "$camp/ingest/ingest.sh" usage >/dev/null &&
  pass "ingest stand-in answers" || fail "ingest stand-in down"

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
    echo "== etc-nix"; ls -A /etc/nix 2>&1; echo "== profile.d"; ls -A /etc/profile.d
    echo "== bin"; ls -A /usr/local/bin
    echo "== tmp"; find /tmp /var/tmp -maxdepth 1 -user lab 2>/dev/null | sort
    echo "== nix"; ls -d /nix 2>&1
    echo "== run"; ls -d /run/pipedpeer-lab /var/lib/pipedpeer-lab 2>&1
    echo "== lab processes"; ps -u lab -o comm= 2>/dev/null | sort
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

profile=/home/lab/pipedpeer-lab/repo/scripts/campaign/test/profile-rehearsal.tsv
knobs="LAB_CODE_URL=$site/code.tar.gz PIPEDPEER_RELEASE_BASE_URL=$site/releases LAB_DISCOVER_TIMEOUT=600"
knobs+=" LAB_PROFILE=$profile LAB_CAL_STRONG_S=10 LAB_CAL_WEAK_S=5 CAL_MIN_S=1 PL_NPROC=2"
knobs+=" PL_RAY_START_ARGS=--object-store-memory=100000000 PL_DASK_WORKER_ARGS='--memory-limit 0'"
knobs+=" PL_WAIT_TIMEOUT=240 PL_RUN_TIMEOUT=900"
head_line="curl -fsSL $site/lab.sh | sudo $knobs bash -s -- head 4"
# The passphrase goes in through a pty: lab.sh reads it from /dev/tty.
head_run() { printf '%s\n' "$pass_phrase" | $rt exec -i -u lab -w /home/lab "$tag-pc00" script -qec "$head_line" /dev/null; }

say "head line on pc00 (first run, killed during e4)"
head_run >"$work/head1.log" 2>&1 &
hp=$!
killed=0
for _ in $(seq 5400); do
  if x pc00 'grep -q "^e4 " /home/lab/pipedpeer-lab/exp-started 2>/dev/null'; then
    # Mid-experiment: once e4 has written two runs.
    for _ in $(seq 300); do
      n=$(x pc00 'f=/home/lab/pipedpeer-lab/repo/results/lab/runs.jsonl; s=$(awk "\$1==\"e4\"{print \$2}" /home/lab/pipedpeer-lab/exp-started | tail -n1); echo $(( $(wc -l <$f) - s ))' 2>/dev/null || echo 0)
      ((n >= 2)) && break
      sleep 2
    done
    x pc00 'systemctl kill -s KILL pipedpeer-lab-day' && killed=1
    echo "killed after $n e4 run(s)"
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
echo "$out" >"$work/status-after-kill.txt"

say "head line again (resume)"
head_run >"$work/head2.log" 2>&1 || true
sed 's/^/    | /' "$work/head2.log" | tail -n 45
for s in discover prep.nixenv calibrate exp.e1 exp.e1b; do
  grep -q "$s: done before, skipped" "$work/head2.log" && pass "resume skipped $s" || fail "resume did not skip $s"
done
grep -q 'e4 was cut off before; running it again' "$work/head2.log" && pass "e4 run again after the kill" || fail "e4 not marked as cut off"
grep -q 'ALL DONE, the lab PCs are back as they were. You can leave.' "$work/head2.log" && pass "summary: ALL DONE" || fail "no ALL DONE"

say "uploads (the ingest stand-in's bucket)"
raw=$work/bucket/raw
got=$work/got
mkdir -p "$got"
for f in runs.jsonl attempts.jsonl machines.csv network.csv clocks.txt workloads.md workloads.json result-cache.json \
  inventory.ini logs/day/lab-day.log csv/e1.csv summary.txt summary-day.txt; do
  if [[ -s $raw/$f.gz ]]; then pass "uploaded raw/$f.gz"; mkdir -p "$got/$(dirname "$f")"; gzip -dc "$raw/$f.gz" >"$got/$f"
  else fail "not uploaded: raw/$f.gz"; fi
done
for f in e1_speedup e1b_efficiency e4_uneven e5_env; do
  [[ -s $raw/figures/$f.png ]] && pass "figure $f.png uploaded" || fail "no figure $f.png"
done
n_otel=$(find "$raw/otel" -name '*.zjsonl*' 2>/dev/null | wc -l)
((n_otel > 0)) && pass "driver collector files uploaded by ingest.sh: $n_otel under raw/otel/" || fail "no otel files uploaded"
for p in pc00 pc01 pc02 pc03; do
  n=$(find "$raw/pcap/$p" -name '*.pcap' 2>/dev/null | wc -l)
  ((n > 0)) && pass "pcap from $p: $n file(s), $(du -ch "$raw/pcap/$p"/*.pcap | tail -n1 | cut -f1)" || fail "no pcap from $p"
done
grep -q '^local:' "$work/head2.log" && pass "final count: $(grep -E '^(local|remote):' "$work/head2.log" | tail -n2 | tr '\n' ' ')" || fail "no final count"

say "attempts, runs and the weak references"
cp "$here/profile-rehearsal.tsv" "$got/profile.tsv"
python3 - "$got" <<'EOF' && pass "attempts and runs as expected" || fail "attempts/runs"
import collections, json, sys
d = sys.argv[1]
att = [json.loads(l) for l in open(d + "/attempts.jsonl")]
st = collections.defaultdict(list)
for a in att:
    st[a["experiment"]].append(a["status"])
print("attempts:", dict(st))
assert st["e4"] == ["interrupted", "ok"], st["e4"]
assert all(v[-1] == "ok" for v in st.values()), st
want = sorted(l.split()[0] for l in open(d + "/profile.tsv") if l.strip() and not l.startswith("#"))
assert sorted(st) == want, (sorted(st), want)
runs = [json.loads(l) for l in open(d + "/runs.jsonl")]
by = collections.Counter(r["experiment"] for r in runs)
bad = collections.Counter(r["experiment"] for r in runs if r.get("error"))
print("runs:", dict(by), "failed:", dict(bad))
vers = {r.get("pipedpeer_version") for r in runs}
assert vers == {"pipedpeer version lab-rehearsal"} or all("lab-rehearsal" in (v or "") for v in vers), vers
e1 = [r for r in runs if r["experiment"] == "e1"]
# W1-W4 x (N=1: 4 systems, N=2 and 4: 3 systems) x (warm-up + 1): once, not twice.
assert len(e1) == 4 * (4 + 3 + 3) * 2, len(e1)
# pipedpeer really spreads: every W1 run at N >= 2 sent items to peers.
pp = [r for r in e1 if r["workload"] == "W1" and r["system"] == "pipedpeer" and r["n"] >= 2]
assert pp and all((r.get("receipt") or {}).get("remote_items", 0) > 0 for r in pp), \
    [(r["n"], (r.get("receipt") or {}).get("remote_items")) for r in pp]
assert all(len(json.dumps(r)) < 20000 for r in runs), max(len(json.dumps(r)) for r in runs)   # receipts are summaries
short = [r for r in e1 if r["n"] == 4 and r["system"] in ("ray", "dask")]
assert short and all("inventory has 3 workers" in (r.get("error") or "") for r in short), short[:1]
other = [(r["workload"], r["system"], r["n"], (r.get("error") or "")[:90]) for r in e1 if r.get("error") and r not in short]
print("e1 failures besides N=4 > workers:", other)
# Known, not the day's: W4 under pipedpeer gives another best_score (HEADLESS.md, "Rehearsal");
# Ray's memory monitor kills W3 tasks in 1.25 GB containers.
assert all((w, sy) in (("W4", "pipedpeer"), ("W3", "ray")) for w, sy, _, _ in other), other
cache = json.load(open(d + "/result-cache.json"))
weak = {k: v for k, v in cache.items() if v["source"].startswith("plain-precomputed:")}
print("weak references:", sorted(weak))
assert len(weak) == 2, weak   # N = 1 and 3
e1b = [r for r in runs if r["experiment"] == "e1b" and not r.get("warmup")]
assert e1b and all((r.get("result_ref") or "").startswith("plain-precomputed:") or r["system"] == "plain" for r in e1b), \
    [(r["system"], r["n"], r.get("result_ref")) for r in e1b]
EOF
gzip -dc "$raw/timeline.jsonl.gz" >"$got/timeline.jsonl" 2>/dev/null || true
python3 - "$got/timeline.jsonl" <<'EOF' && pass "timeline.jsonl has every E14 event the day writes" || fail "timeline.jsonl (E14)"
import json, sys
ev = {json.loads(l)["event"] for l in open(sys.argv[1])}
need = {"head_start", "setup_start", "labpc", "ssh_ok", "pc_map", "install_start", "install_end", "join_end", "cluster_ready"}
assert need <= ev, sorted(need - ev)
EOF
grep -q 'e4: ' "$got/summary.txt" && pass "analyse.py summary: $(grep '^e4: ' "$got/summary.txt")" || fail "no e4 in summary.txt"
grep -q 'calibrated on pc01' "$got/workloads.json" && pass "workloads.json calibrated on pc01" || fail "workloads.json not calibrated"

say "traces"
if python3 -I "$camp/otel/otelread.py" check "$raw/otel" 1000000 >"$work/otel-check.txt" 2>&1; then
  awk '$3=="pipedpeer-run"{print $1}' "$work/otel-check.txt" | grep -qx pc00 && pass "trace pipedpeer-run from pc00, the submitter" || fail "no pipedpeer-run trace from pc00"
  for src in host.cpu host.process pp-poll/api net/sockets net/snmp node-state/procs daemon-log ray-log dask-log prom.ray prom.dask; do
    n=$(awk -v s="$src" '$3==s{c++} END{print c+0}' "$work/otel-check.txt")
    ((n == 4)) && pass "trace $src from all 4 PCs" ||
      { ((n > 0)) && echo "NOTE: trace $src from $n of 4 PCs: $(awk -v s="$src" '$3==s{printf "%s ", $1}' "$work/otel-check.txt")" || fail "no trace $src"; }
  done
  grep -q 'run ids seen: e' "$work/otel-check.txt" && pass "records carry lab.run_id" || fail "no lab.run_id on any record"
else
  fail "otelread.py check failed"; tail -5 "$work/otel-check.txt"
fi

say "Ray work stays off pc00 (Pool and joblib actors ask for a CPU)"
python3 -I "$here/ray-placement.py" "$raw/otel" pc00 | tee "$work/ray-placement.txt" | tail -n 12
[[ ${PIPESTATUS[0]} == 0 ]] && pass "no Ray Pool actor or task ran on pc00" || fail "Ray work ran on pc00 ($work/ray-placement.txt)"

say "every PC as it was"
for p in "${pcs[@]}"; do
  snapshot "$p" >"$work/after-$p"
  if diff -u "$work/before-$p" "$work/after-$p"; then pass "$p: identical after the day"; else fail "$p: differs after the day"; fi
done

say "phase timings (lab-day.log)"
python3 "$here/day-timings.py" "$got/logs/day/lab-day.log" | tee "$work/timings.txt"
echo "rehearsal wall: $(( ($(date +%s) - t_start) / 60 )) min"
mkdir -p "$repo/results/lab/rehearsal-day"
cp "$work/timings.txt" "$work/otel-check.txt" "$work/ray-placement.txt" "$got/summary.txt" "$got/attempts.jsonl" "$repo/results/lab/rehearsal-day/" 2>/dev/null || true
gzip -dc "$raw/logs/day/lab-day.log.gz" >"$repo/results/lab/rehearsal-day/lab-day.log" 2>/dev/null || true

echo
if ((fails)); then echo "headless-test: $fails FAILED"; exit 1; fi
echo "headless-test: all passed"
