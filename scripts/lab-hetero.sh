#!/usr/bin/env bash
# Brings up a deliberately uneven cluster of local workers, so scheduling
# decisions have something to decide.
#
# The even-split bug this exists to catch is invisible on a uniform cluster:
# if every node is the same speed, splitting by headcount and splitting by
# throughput give the same answer. It only shows up when the nodes differ, and
# the two real machines available here differ by 20 cores to 16 - not enough
# for the difference to clear measurement noise.
#
# So each worker gets a hard CPU quota via its cgroup. An 8-core and a 1-core
# worker are genuinely eight times apart, which is the shape of a real cluster
# (a workstation next to a laptop next to a small VM) and makes an even split
# visibly wrong: the 1-core worker is handed a third of the work and everyone
# waits for it.
set -euo pipefail

cpus=(${LAB_CPUS:-8 2 1})
ports=(38081 38082 38083)
bin="${1:-$HOME/bin/pipedpeer}"
labdir="$HOME/lab-hetero"

# No compose here, only plain containers, so a runtime without a compose
# implementation is still perfectly usable - but this one builds an image, and
# docker is preferred for it. On the test machine podman answers `info` and
# then refuses the build: "short-name resolution enforced but cannot prompt
# without a TTY". The image below is fully qualified now, which is the real
# fix, and the order stays docker-first because that is what this script has
# always been exercised with.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/runtime.sh"
PP_NEED_COMPOSE=0 PP_RUNTIME_ORDER="docker podman" pp_pick_runtime || exit 1
runtime="$PP_RUNTIME"

# Containers first: they bind-mount the binary, so replacing it underneath a
# running one fails with "Text file busy".
for i in 1 2 3; do
	$runtime rm -f "pp-hetero-$i" >/dev/null 2>&1 || true
done
sleep 1

# Workers are recreated on every run, so each gets a new certificate and last
# run's pin is stale. Refusing them is correct for real machines and pure
# friction here, where the daemons are disposable by design.
"$bin" auth forget --all >/dev/null 2>&1 || true

mkdir -p "$labdir"
rm -f "$labdir/pipedpeer"
cp "$bin" "$labdir/pipedpeer"
chmod +x "$labdir/pipedpeer"

cat > "$labdir/Dockerfile" <<'DOCKER'
FROM docker.io/nixos/nix:2.31.3
RUN nix-env -iA nixpkgs.crun
CMD ["sh", "-c", "sleep infinity"]
DOCKER

echo "building image..."
$runtime build -q -t pp-hetero "$labdir" >/dev/null

for i in "${!ports[@]}"; do
	name="pp-hetero-$((i + 1))"
	port="${ports[$i]}"
	quota="${cpus[$i]}"
	echo "starting $name: ${quota} cpu(s) on :$port"
	# A memory limit as well as a CPU one. Without it the container's daemon
	# has no systemd to place it in a scope and no limit from docker either,
	# so it runs completely unbounded beside a host daemon that is correctly
	# capped - and the kernel, finding the machine out of memory, picks a
	# victim outside both. Measured: it killed the desktop shell.
	$runtime run -d --name "$name" \
		--privileged \
		--network host \
		--cpus="$quota" \
		--memory="${LAB_MEM:-2g}" \
		-v "$labdir/pipedpeer:/usr/local/bin/pipedpeer:ro" \
		-e XDG_DATA_HOME=/var/lib/pipedpeer \
		pp-hetero \
		sh -c "/usr/local/bin/pipedpeer setup -y --port $port && exec sleep infinity" >/dev/null
done

echo "waiting for workers..."
for port in "${ports[@]}"; do
	ok=0
	for _ in $(seq 1 90); do
		if curl -sf --max-time 2 "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
			ok=1
			break
		fi
		sleep 1
	done
	if [[ $ok -ne 1 ]]; then
		echo "FAIL: worker on :$port never came up"
		$runtime logs "pp-hetero-$((${#ports[@]}))" 2>&1 | tail -20
		exit 1
	fi
done

echo "cluster up:"
for i in "${!ports[@]}"; do
	echo "  :${ports[$i]} — ${cpus[$i]} cpu(s)"
done
