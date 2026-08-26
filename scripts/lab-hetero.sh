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

runtime=docker
command -v docker >/dev/null || runtime=podman

mkdir -p "$labdir"
cp "$bin" "$labdir/pipedpeer"
chmod +x "$labdir/pipedpeer"

cat > "$labdir/Dockerfile" <<'DOCKER'
FROM nixos/nix:2.31.3
RUN nix-env -iA nixpkgs.crun
CMD ["sh", "-c", "sleep infinity"]
DOCKER

echo "building image..."
$runtime build -q -t pp-hetero "$labdir" >/dev/null

for i in "${!ports[@]}"; do
	name="pp-hetero-$((i + 1))"
	$runtime rm -f "$name" >/dev/null 2>&1 || true
done
# Host ports linger for a moment after the containers holding them go away.
sleep 1

for i in "${!ports[@]}"; do
	name="pp-hetero-$((i + 1))"
	port="${ports[$i]}"
	quota="${cpus[$i]}"
	echo "starting $name: ${quota} cpu(s) on :$port"
	$runtime run -d --name "$name" \
		--privileged \
		--network host \
		--cpus="$quota" \
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
