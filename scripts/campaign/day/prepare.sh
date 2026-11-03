# shellcheck shell=bash
# The lab day's steps up to the experiments: bootstrap, discover, prepare,
# calibrate, weak references, and each experiment. Sourced by lab-day.sh.
# shellcheck disable=SC2016,SC2031,SC2034  # remote commands single-quoted; CUR_STEP is read in lib.sh

R='$HOME/pipedpeer-lab/repo/scripts/campaign'   # the campaign scripts, on any PC
pc00_ip() { host_ip pc00 "$INV_ALL"; }

bootstrap() {
  if [[ ! -x $VENV/bin/ansible ]]; then
    mkdir -p "$(dirname "$VENV")"
    # Ubuntu ships python3 without venv's ensurepip; virtualenv.pyz needs neither.
    if ! python3 -m venv "$VENV" >/dev/null 2>&1; then
      rm -rf "$VENV"
      curl -fsSL --retry 3 https://bootstrap.pypa.io/virtualenv.pyz -o "$DAY/virtualenv.pyz" || return 1
      python3 "$DAY/virtualenv.pyz" -q "$VENV" || return 1
      rm -f "$DAY/virtualenv.pyz"
    fi
    "$VENV/bin/python" -m pip install -q --disable-pip-version-check 'ansible-core>=2.17,<2.23' matplotlib || return 1
  fi
  # run.sh's lib/ansible.sh reads results with the json callback, which lives
  # in ansible.posix (ansible-core has none), in the venv's collections dir.
  if [[ ! -d $VENV/collections/ansible_collections/ansible/posix ]]; then
    "$VENV/bin/ansible-galaxy" collection install -p "$VENV/collections" ansible.posix >/dev/null ||
      blocker "ansible.posix did not install; run.sh's json callback may be missing"
  fi
  if [[ -f $CAMP/lib/ansible.sh ]]; then
    bash "$CAMP/lib/ansible.sh" install >/dev/null || blocker "run.sh's lib/ansible.sh install failed"
  fi
  "$VENV/bin/ansible" --version | head -n1
}

# discover scans pc00's subnet until LAB_PCS PCs answer, or the timeout.
discover() {
  local ip sub n=0 deadline
  ip=$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1);exit}}')
  sub=${LAB_SUBNET:-$(ip -4 route show dev "$(ip -4 route get 1.1.1.1 | awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1);exit}}')" proto kernel scope link | awk '{print $1; exit}')}
  [[ -n $ip && -n $sub ]] || { say "cannot tell this PC's address or subnet"; return 1; }
  deadline=$(( $(date +%s) + ${LAB_DISCOVER_TIMEOUT:-1800} ))
  say "discover: scanning $sub for PCs that ran lab.sh; this one ($ip) is pc00"
  while :; do
    LAB_RESULTS=$RESULTS bash "$CAMP/scan.sh" "$sub" --head "$ip" --user "$USER" >"$DAY/logs/scan.log" 2>&1 ||
      say "scan: $(tail -n1 "$DAY/logs/scan.log")"
    n=$(grep -c '^pc[0-9]* ansible_host=' "$INV" 2>/dev/null || echo 0)
    tl_seen   # E14: day/timeline.sh
    progress discover "$n of $LAB_PCS PCs answered"
    say "discover: $n of $LAB_PCS PCs answered"
    ((n >= LAB_PCS)) && break
    if (($(date +%s) >= deadline)); then
      blocker "only $n of $LAB_PCS PCs answered within ${LAB_DISCOVER_TIMEOUT:-1800} s; the day goes on with them"
      break
    fi
    sleep 15
  done
  grep -q '^pc00 ansible_host='"$ip"' ' "$INV" || { say "pc00 is not this PC in $INV"; return 1; }
  # pc00 is this PC: Ansible runs its commands here, without ssh.
  sed -i '/^pc00 /{/ansible_connection=/!s/$/ ansible_connection=local/}' "$INV"
  cp -f "$INV" "$INV_ALL"
  cat "$INV"
  [[ -n $(workers) ]]
}

# ensure_services restarts what lives in this unit's cgroup and so died with
# an earlier run of it: the file server, the driver and pc00 collectors, pc00's daemon.
ensure_services() {
  is_done discover || return 0
  is_done prep.otel && { OTELCOL_TARBALL=$DAY/serve/otelcol.tar.gz bash "$CAMP/telemetry.sh" up >/dev/null 2>&1 || say "driver collector did not start"; }
  is_done prep.otel && { bash "$CAMP/otel/node.sh" status >/dev/null 2>&1 || bash "$CAMP/otel/node.sh" start pc00 "$(pc00_ip)" --session "$SESSION" --pcap >/dev/null 2>&1 || say "pc00 node collector did not start"; }
  if is_done prep.join && command -v pipedpeer >/dev/null; then
    pipedpeer status >/dev/null 2>&1 || pipedpeer start >/dev/null 2>&1 || say "pc00 daemon did not start"
  fi
  return 0
}

# dist_up serves $DAY/serve on pc00 (code, the Nix binary cache, otelcol), so
# each download from the internet happens once and the PCs fetch from pc00.
dist_up() {
  mkdir -p "$DAY/serve"
  ln -sfn ../code.tar.gz "$DAY/serve/code.tar.gz"
  if [[ -f $DAY/dist.pid ]] && kill -0 "$(<"$DAY/dist.pid")" 2>/dev/null; then return 0; fi
  setsid nohup python3 -m http.server "$DIST_PORT" --bind "$(pc00_ip)" -d "$DAY/serve" >"$DAY/logs/dist.log" 2>&1 </dev/null &
  echo $! >"$DAY/dist.pid"
  local i; for i in $(seq 50); do
    curl -fsS -o /dev/null "http://$(pc00_ip):$DIST_PORT/code.tar.gz" 2>/dev/null && return 0
    sleep 0.2
  done
  say "file server on pc00 did not come up: $(tail -n2 "$DAY/logs/dist.log")"; return 1
}
dist_down() { [[ -f $DAY/dist.pid ]] && kill "$(<"$DAY/dist.pid")" 2>/dev/null; rm -f "$DAY/dist.pid"; }

prep_machines() {
  ans machines all '
    echo "hostname=$(uname -n)"
    echo "cpu_model=$(LC_ALL=C lscpu 2>/dev/null | sed -n "s/^Model name: *//p" | head -n1)"
    echo "cores=$(nproc)"
    echo "threads_per_core=$(LC_ALL=C lscpu 2>/dev/null | sed -n "s/^Thread(s) per core: *//p" | head -n1)"
    echo "ram_gib=$(awk "/MemTotal/{printf \"%.1f\",\$2/1048576}" /proc/meminfo)"
    . /etc/os-release 2>/dev/null; echo "os=$PRETTY_NAME"
    echo "kernel=$(uname -r)"
    dev=$(ip -4 route get 1.1.1.1 2>/dev/null | awk "{for(i=1;i<=NF;i++) if(\$i==\"dev\"){print \$(i+1);exit}}")
    echo "nic=$dev"; echo "nic_mbps=$(cat /sys/class/net/$dev/speed 2>/dev/null)"
    echo "python=$(python3 -V 2>&1)"; echo "machine_id=$(cat /etc/machine-id)"' || drop_bad "no answer to the inventory"
  python3 "$CAMP/day/tables.py" machines "$DAY/out/machines" "$INV_ALL" "$RESULTS/machines.csv"
}

# netem_check: E15 adds delay with tc's netem qdisc, a kernel module
# (sch_netem; on Ubuntu it is in linux-modules-extra-<kernel>). Each PC is
# asked whether it is loaded or loadable (modprobe -n: a dry run, nothing
# changes), results/lab/netem.csv records the answer, and a blocker names
# the PCs without it now, rather than E15 failing on them hours later.
netem_check() {
  local h lacking=()
  ans netem all 'if [ -d /sys/module/sch_netem ] || sudo -n modprobe -n -q sch_netem 2>/dev/null; then echo PLNETEM yes; else echo "PLNETEM no $(uname -r)"; fi' || true
  {
    echo "pc_id,netem,kernel"
    for h in "${OK_HOSTS[@]}" "${BAD_HOSTS[@]}"; do
      local line; line=$(out_of netem "$h" | grep '^PLNETEM ' | tail -n1)
      case $line in
        "PLNETEM yes") echo "$h,yes," ;;
        "PLNETEM no "*) echo "$h,no,${line#PLNETEM no }"; lacking+=("$h") ;;
        *) echo "$h,unknown,"; lacking+=("$h") ;;
      esac
    done | sort
  } >"$RESULTS/netem.csv"
  if ((${#lacking[@]})); then
    blocker "no sch_netem kernel module on ${lacking[*]} (results/lab/netem.csv): E15's runs that use them will fail; on Ubuntu it comes with linux-modules-extra-<kernel> (kernels in netem.csv)"
  else
    say "netem: available on every PC"
  fi
}

prep_network() {
  local ip h; ip=$(pc00_ip)
  netem_check
  ans_drop iperf all 'command -v iperf3 >/dev/null || sudo -n bash '"$LABSH"' pkg iperf3' "iperf3 install failed" || return 1
  pkill -u "$USER" -x iperf3 2>/dev/null || true
  iperf3 -s -D -p 5201 --pidfile "$DAY/iperf3.pid" || return 1
  sleep 1
  for h in $(workers); do
    # One PC at a time, so each measures the link and not the others.
    ans "iperf.$h" "$h" "iperf3 -c $ip -p 5201 -t 5 -J; echo '@@REVERSE@@'; iperf3 -c $ip -p 5201 -t 5 -R -J" ||
      say "iperf3 from $h failed"
  done
  kill "$(cat "$DAY/iperf3.pid" 2>/dev/null)" 2>/dev/null || true
  python3 "$CAMP/day/tables.py" network "$DAY/out" "$INV" "$RESULTS/network.csv"
  cat "$RESULTS/network.csv"
}

prep_clocks() {
  local prefix="ssh $SSH_COMMON -o BatchMode=yes -o ConnectTimeout=5 -l $USER" over h
  bash "$CAMP/clocks.sh" --exec "$prefix" && return 0
  over=$(awk '$4=="OVER"{print $1}' "$RESULTS/clocks.txt")
  [[ -n $over ]] || { blocker "clocks.sh could not reach some PCs (results/lab/clocks.txt)"; return 0; }
  say "clock over 10 ms on: $over; starting chrony there"
  for h in $over; do
    ans "chrony.$h" "$h" 'set -e; sudo -n bash '"$LABSH"' pkg chrony
      u=chronyd; systemctl cat chrony.service >/dev/null 2>&1 && u=chrony
      sudo -n bash '"$LABSH"' enable $u; sleep 10; sudo -n chronyc -a makestep' || say "chrony on $h failed"
  done
  sleep 20
  cp -f "$RESULTS/clocks.txt" "$RESULTS/clocks-before-chrony.txt"
  bash "$CAMP/clocks.sh" --exec "$prefix" || blocker "clocks still over 10 ms after chrony: $(awk '$4=="OVER"{print $1}' "$RESULTS/clocks.txt" | tr '\n' ' ')"
  return 0
}

prep_code() {
  dist_up || return 1
  local commit; commit=$(cat "$REPO/.lab-commit" 2>/dev/null || echo unknown)
  ans_drop code workers 'set -e; d=$HOME/pipedpeer-lab
    [ "$(cat $d/repo/.lab-commit 2>/dev/null)" = '"$commit"' ] && exit 0
    rm -rf $d/repo; mkdir -p $d/repo
    curl -fsS http://'"$(pc00_ip)"':'"$DIST_PORT"'/code.tar.gz | tar -xz --strip-components=1 -C $d/repo
    echo '"$commit"' >$d/repo/.lab-commit' "could not fetch the code from pc00"
}

pipedpeer_version() {
  [[ -n ${LAB_PIPEDPEER_VERSION:-} ]] && { echo "$LAB_PIPEDPEER_VERSION"; return; }
  [[ -s $DAY/pipedpeer-version ]] && { cat "$DAY/pipedpeer-version"; return; }
  # The newest nightly build, fixed for the whole day.
  curl -fsSL "https://api.github.com/repos/vinayakyadav2709/Pipedpeer/releases?per_page=30" 2>/dev/null |
    python3 -c 'import json,sys
for r in json.load(sys.stdin):
    t = r.get("tag_name", "")
    if t.startswith("nightly-"):
        print(t[len("nightly-"):]); break' >"$DAY/pipedpeer-version" || true
  cat "$DAY/pipedpeer-version"
}

# mirror_release <tag>: the release's binaries and checksums.txt into
# $DAY/serve/releases/download/<tag>/, verified, for install-pipedpeer.sh.
mirror_release() {
  local tag=$1 src=${PIPEDPEER_RELEASE_BASE_URL:-https://github.com/vinayakyadav2709/Pipedpeer/releases}
  local d=$DAY/serve/releases/download/$tag a
  dist_up || return 1
  mkdir -p "$d"
  [[ -s $d/checksums.txt ]] || curl -fsSL --retry 3 "$src/download/$tag/checksums.txt" -o "$d/checksums.txt" || return 1
  for a in amd64 arm64; do
    grep -q " pipedpeer-linux-$a\$" "$d/checksums.txt" || continue
    [[ -s $d/pipedpeer-linux-$a ]] || curl -fsSL --retry 3 "$src/download/$tag/pipedpeer-linux-$a" -o "$d/pipedpeer-linux-$a" || return 1
  done
  (cd "$d" && sha256sum -c --quiet --ignore-missing checksums.txt) || { rm -f "$d"/pipedpeer-linux-*; return 1; }
  say "pipedpeer $tag on pc00's file server ($(du -sh "$d" | cut -f1))"
}

prep_pipedpeer() {
  local v h; v=$(pipedpeer_version)
  [[ -n $v ]] || blocker "could not pin a pipedpeer version; installing the latest nightly"
  # What each PC had before the day touched it, recorded once.
  ans_drop pre all 'test -e /nix && echo had-nix; command -v pipedpeer >/dev/null && echo had-pipedpeer; true' "no answer before the install" || return 1
  for h in "${OK_HOSTS[@]}"; do
    [[ -f $HOSTREC/$h ]] || out_of pre "$h" | grep '^had-' >"$HOSTREC/$h" || true
    grep -q had-pipedpeer "$HOSTREC/$h" && blocker "$h already had pipedpeer; the day upgrades it and leaves it installed"
  done
  # The campaign's own build (publish.sh: release lab-<commit12>) comes down
  # once to pc00's file server, checksum-checked, and every PC installs it
  # from there; a nightly comes from GitHub on each PC.
  local base=${PIPEDPEER_RELEASE_BASE_URL:-}
  if [[ $v == lab-* ]]; then
    mirror_release "$v" || { say "could not fetch pipedpeer $v"; return 1; }
    base=http://$(pc00_ip):$DIST_PORT/releases
  fi
  local irc=0   # E14: timed per PC (day/timeline.sh)
  ans_drop install all "$(tl_wrap install "${base:+PIPEDPEER_RELEASE_BASE_URL=$base }bash $R/../install-pipedpeer.sh ${v:+--version $v} -y")" "pipedpeer install failed" || irc=1
  tl_from_out install install
  ((irc == 0)) || return 1
  return 0
}

prep_join() {
  local line code
  line=$(pipedpeer invite 2>"$DAY/logs/invite.err") || { say "pipedpeer invite: $(cat "$DAY/logs/invite.err")"; return 1; }
  code=$(sed -n 's/^pipedpeer connect //p' <<<"$line" | tail -n1)
  [[ -n $code ]] || { say "no code in: $line"; return 1; }
  ans_drop join workers "$(tl_wrap join "pipedpeer connect $code")" "pipedpeer connect failed" || true
  tl_from_out join join   # E14 (day/timeline.sh)
  local want n i; want=$(workers | wc -l)
  for i in $(seq 40); do
    # Healthy nodes in pc00's view (its own entry may be among them).
    # The API wants the cluster token (the one config file), as systems.py sends it.
    n=$(curl -fsS -H "X-Pipedpeer-Token: $(sed -n 's/.*"token" *: *"\([^"]*\)".*/\1/p' "${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer/cluster.json" 2>/dev/null)" \
      http://127.0.0.1:4242/v1/nodes 2>/dev/null |
      python3 -c 'import json,sys; print(sum(1 for x in json.load(sys.stdin) or [] if x.get("state") == "healthy"))' 2>/dev/null || echo 0)
    ((n >= want)) && break
    sleep 3
  done
  say "pc00 sees $n healthy nodes for $want workers"
  tl pc00 cluster_ready "$(date +%s%N)" pc00 "" "healthy=$n workers=$want"
  pipedpeer status >"$RESULTS/join-status.txt" 2>&1 || true
  node_ids
  # doctor after the join: before it, each PC's setup has made a cluster of
  # its own, and doctor reports every other PC on the LAN as a stranger.
  ans doctor all 'pipedpeer --version; pipedpeer doctor' || true
  mkdir -p "$RESULTS/doctor"
  for h in "${OK_HOSTS[@]}" "${BAD_HOSTS[@]}"; do out_of doctor "$h" >"$RESULTS/doctor/$h.txt"; done
  ((${#BAD_HOSTS[@]})) && blocker "pipedpeer doctor reported problems on ${BAD_HOSTS[*]} (results/lab/doctor/)"
  return 0
}

# node_ids writes results/lab/node-ids.csv: each PC's pipedpeer node id (the
# key fingerprint receipts, record.json and the daemon logs name machines
# by), so they join to pc ids. Rerun at the end of the day by finish.sh.
node_ids() {
  local h id
  ans nodeid all 'cat "${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer/daemon.node"' "$INV_ALL" || true
  {
    echo "pc_id,node_id"
    for h in "${OK_HOSTS[@]}"; do
      id=$(out_of nodeid "$h" | tr -cd '0-9a-f\n' | grep -E '^[0-9a-f]{8,}$' | tail -n1)
      [[ -n $id ]] && echo "$h,$id"
    done | sort
  } >"$RESULTS/node-ids.csv"
  say "node ids: $(($(wc -l <"$RESULTS/node-ids.csv") - 1)) PCs in results/lab/node-ids.csv"
}

# prep_nixenv: pc00 builds the campaign env from cache.nixos.org and writes
# its closure, and the flake's inputs, into a binary cache it serves; each
# worker pulls from there as root (no signatures needed, no nix.conf change),
# then install.sh finds every path present. A failed pull falls back to
# cache.nixos.org inside install.sh.
prep_nixenv() {
  dist_up || return 1
  local envdir=$CAMP/env out nix cache=$DAY/serve/nix
  out=$(bash "$envdir/install.sh" | sed -n 's/^LAB-ENV //p' | tail -n1)
  [[ -n $out ]] || { say "the campaign env did not build on pc00"; return 1; }
  nix=$(command -v nix) || return 1
  if [[ ! -s $cache/lab-paths.txt ]]; then
    local xf=(--extra-experimental-features 'nix-command flakes') inputs
    "$nix" "${xf[@]}" copy --to "file://$cache?compression=zstd" "$out" || return 1
    inputs=$("$nix" "${xf[@]}" flake archive --json --to "file://$cache?compression=zstd" "path:$envdir" |
      python3 -c 'import json,sys
def walk(n):
    yield n["path"]
    for i in n.get("inputs", {}).values(): yield from walk(i)
print("\n".join(walk(json.load(sys.stdin))))') || return 1
    printf '%s\n' "$out" "$inputs" >"$cache/lab-paths.txt.new"
    mv -f "$cache/lab-paths.txt.new" "$cache/lab-paths.txt"
  fi
  ans_drop nixenv workers 'cd '"$R"'/env
    if [ ! -x $HOME/.local/share/pipedpeer-lab/env/bin/python3 ]; then
      nix=$(command -v nix || echo /nix/var/nix/profiles/default/bin/nix)
      paths=$(curl -fsS http://'"$(pc00_ip)"':'"$DIST_PORT"'/nix/lab-paths.txt) &&
        sudo -n "$nix" --extra-experimental-features nix-command copy --no-check-sigs \
          --from http://'"$(pc00_ip)"':'"$DIST_PORT"'/nix $paths && echo "pulled the env from pc00" ||
        echo "pull from pc00 failed; install.sh falls back to cache.nixos.org"
    fi
    bash install.sh' "the campaign env did not install"
}

prep_otel() {
  dist_up || return 1
  # Downloaded once, checksum-verified, then every PC (pc00 too) installs from it.
  if [[ ! -s $DAY/serve/otelcol.tar.gz ]]; then
    ( source "$CAMP/otel/lib.sh"; arch=$(lab_arch); sum=OTELCOL_SHA256_$arch
      lab_fetch "https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v$OTELCOL_VERSION/otelcol-contrib_${OTELCOL_VERSION}_linux_$arch.tar.gz" \
        "${!sum}" "$DAY/serve/otelcol.tar.gz" "${OTELCOL_TARBALL:-}" ) || { blocker "otelcol download failed; telemetry is off"; return 0; }
  fi
  OTELCOL_TARBALL=$DAY/serve/otelcol.tar.gz bash "$CAMP/telemetry.sh" up ||
    blocker "the driver collector did not start; telemetry is off"
  # tcpdump for node.sh --pcap: from the distribution, so lab.sh undo removes it.
  ans tcpdump all 'command -v tcpdump >/dev/null || PATH=$PATH:/usr/sbin command -v tcpdump >/dev/null || sudo -n bash '"$LABSH"' pkg tcpdump' ||
    blocker "tcpdump did not install on ${BAD_HOSTS[*]}; no pcap from there"
  # Every trace (otel/README.md, "Runner integration" 1): the session in
  # $LAB_HOME/session, then the collector, the poller and the pcap.
  ans otel all 'set -e; cd '"$R"'/otel; t=$HOME/pipedpeer-lab/otelcol.tar.gz
    curl -fsS http://'"$(pc00_ip)"':'"$DIST_PORT"'/otelcol.tar.gz -o $t
    OTELCOL_TARBALL=$t bash install-otelcol.sh; rm -f $t
    mkdir -p $HOME/.local/share/pipedpeer-lab; echo '"$SESSION"' >$HOME/.local/share/pipedpeer-lab/session
    export PATH=$PATH:/usr/sbin
    bash node.sh status >/dev/null 2>&1 || bash node.sh start {{ inventory_hostname }} '"$(pc00_ip)"' --session '"$SESSION"' --pcap' ||
    blocker "node collector failed on ${BAD_HOSTS[*]}; no host metrics from there"
  local h
  for h in "${OK_HOSTS[@]}"; do
    # No ^: Fedora's sudo prefixes the line with terminal escapes (OSC 3008).
    out_of otel "$h" | grep -q 'pcap: tcpdump pid' || blocker "$h: no pcap ($(out_of otel "$h" | grep -i pcap | tail -n1 | tr -cd '[:print:]' | cut -c1-200))"
  done
  return 0
}

# calibrate: W1-W3 sizes on the first worker (pc01 unless it was dropped).
# The targets are each workload's own (calibrate.sh: 60 s, W4 30 s) unless
# LAB_CAL_STRONG_S and LAB_CAL_WEAK_S set one for all (the rehearsal's few seconds).
calibrate() {
  local h w js ok=1; h=$(workers | head -n1)
  local targets="${LAB_CAL_STRONG_S:+$LAB_CAL_STRONG_S ${LAB_CAL_WEAK_S:-$LAB_CAL_STRONG_S}}"
  mkdir -p "$RESULTS/calibration"
  for w in W1 W2 W3 W4; do
    [[ -s $RESULTS/calibration/$w.json ]] && continue
    local try
    for try in 1 2; do
      ans "cal.$w" "$h" "cd $R/../.. && CAL_VERIFY=1 ${CAL_MIN_S:+CAL_MIN_S=$CAL_MIN_S }${PL_NPROC:+PL_NPROC=$PL_NPROC }bash scripts/campaign/calibrate.sh $w $targets" || true
      out_of "cal.$w" "$h" >"$RESULTS/calibration/$w-$try.log"
      js=$(sed -n 's/^CALIBRATE //p' "$RESULTS/calibration/$w-$try.log" | tail -n1)
      [[ -n $js ]] || continue
      # One PC alone should take the target (60 s; W4 30 s) give or take a sixth.
      if python3 -c 'import json,sys; j=json.loads(sys.argv[1]); v=j.get("verify_s"); t=j.get("strong_target_s", 60); sys.exit(0 if v is None or abs(v-t)<=t/6 else 1)' "$js" || ((try == 2)); then
        printf '%s\n' "$js" >"$RESULTS/calibration/$w.json"; break
      fi
      say "$w: the strong size ran outside its target on $h; calibrating again"
    done
    [[ -s $RESULTS/calibration/$w.json ]] || { blocker "calibrating $w on $h failed (results/lab/calibration/$w-*.log)"; ok=0; }
  done
  python3 "$CAMP/day/sizes.py" apply "$REPO" "$RESULTS/calibration" "$h" || return 1
  cp -f "$CAMP/workloads.json" "$RESULTS/workloads.json"
  ((ok))
}

# weakrefs: the plain W1 result at each weak size of e1b, once, each on its
# own idle worker and in parallel, into run.sh's result cache.
weakrefs() {
  local ns row; row=$(printf '%s\n' "${EXPS[@]}" | awk '$1=="e1b"')
  [[ -n $row ]] || return 0
  ns=$(sed -n 's/.*--n \([0-9,]*\).*/\1/p' <<<"$row" | tr , ' ')
  [[ -n $ns ]] || ns="1 4 16"
  local -a hosts; mapfile -t hosts < <(workers | tac)
  local i=0 n size pids=()
  for n in $ns; do
    size=$(python3 "$CAMP/day/sizes.py" weak "$REPO" W1 "$n") || return 1
    python3 "$CAMP/day/sizes.py" have "$REPO" W1 "$size" && continue
    local h=${hosts[$((i % ${#hosts[@]}))]}; i=$((i + 1))
    say "weak reference N=$n ($size) on $h"
    ( ans "weak.$n" "$h" "cd $R/../.. && $size BENCH_PROCS=${PL_NPROC:-\$(nproc)} \$HOME/.local/share/pipedpeer-lab/env/bin/python3 docs/benchmarks/workloads/pool_map.py" >/dev/null ) &
    pids+=("$!:$n:$h:$size")
  done
  local p ok=1
  for p in "${pids[@]}"; do
    wait "${p%%:*}" || true
    IFS=: read -r _ n h size <<<"$p"
    if [[ $(rc_of "weak.$n" "$h") == 0 ]]; then
      python3 "$CAMP/day/sizes.py" put "$REPO" W1 "$size" "plain-precomputed:$h" <"$DAY/out/weak.$n/$h.out" || ok=0
    else
      ok=0; say "weak reference N=$n on $h failed: $(out_of "weak.$n" "$h" | tail -n2)"
    fi
  done
  ((ok))
}

# health drops workers that no longer answer, before each experiment.
health() {
  CUR_STEP=${1:-health}
  ANSIBLE_TIMEOUT=15 ans health workers true || drop_bad "no answer before ${1:-an experiment}"
  return 0
}

runs_lines() { [[ -f $RESULTS/runs.jsonl ]] && wc -l <"$RESULTS/runs.jsonl" || echo 0; }

# attempt <experiment> <status> <from> <to> [rc]: one line in attempts.jsonl.
attempt() {
  printf '{"experiment": "%s", "status": "%s", "runs_from_line": %d, "runs_to_line": %d, "rc": %s, "time": "%(%Y-%m-%dT%H:%M:%S%z)T"}\n' \
    "$1" "$2" "$3" "$4" "${5:-null}" -1 >>"$RESULTS/attempts.jsonl"
}

# run_exp <id> [run.sh args]: one experiment. A failure is recorded and the day goes on.
run_exp() {
  local id=$1; shift
  local name=exp.$id from to rc prev
  is_done "$name" && { say "$name: done before, skipped"; return 0; }
  # Cut off before: its lines so far stay, marked so analyse.py can drop them.
  prev=$(awk -v e="$id" '$1==e{l=$2} END{print l}' "$DAY/exp-started" 2>/dev/null)
  if [[ -n $prev ]]; then
    attempt "$id" interrupted "$prev" "$(runs_lines)"
    say "$id was cut off before; running it again (lines $prev-$(runs_lines) of runs.jsonl marked interrupted)"
  fi
  health "$name"
  ensure_services
  CUR_STEP=$name
  progress run "experiment $id"
  from=$(runs_lines)
  echo "$id $from" >>"$DAY/exp-started"
  say "== $name: run.sh $id --inventory $INV $*"
  if [[ ! -f $CAMP/run.sh ]]; then
    rc=127; blocker "$id: scripts/campaign/run.sh is missing"
  else
    # PL_SYNC=0: run.sh leaves the upload to the sync below, after attempts.jsonl has this attempt.
    (cd "$REPO" && PL_SYNC=0 bash "$CAMP/run.sh" "$id" --inventory "$INV" "$@") 2>&1 | tee "$RESULTS/logs/day/$id.log"
    rc=${PIPESTATUS[0]}
  fi
  to=$(runs_lines)
  if ((rc == 0)); then attempt "$id" ok "$from" "$to" 0
  else attempt "$id" failed "$from" "$to" "$rc"; blocker "experiment $id failed (rc $rc; results/lab/logs/day/$id.log)"; fi
  bash "$CAMP/sync-r2.sh" || blocker "R2 sync after $id failed; the final sync retries"
  mark_done "$name"
}

analyse() {
  local py=$VENV/bin/python
  [[ -f $CAMP/analyse.py ]] || { say "no analyse.py"; return 1; }
  (cd "$REPO" && "$py" "$CAMP/analyse.py") || return 1
  bash "$CAMP/sync-r2.sh" || true
}
