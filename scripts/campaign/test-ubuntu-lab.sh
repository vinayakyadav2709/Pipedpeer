#!/usr/bin/env bash
# test-ubuntu-lab.sh [up|test|down]: rehearse the campaign on Ubuntu 22.04 and
# 24.04 containers with systemd as PID 1 (brief S17, plan 1.7). The containers
# are lab PCs; a driver container plays the laptop and runs the real scripts.
#
#   up    build the images, start UBU_LAB_N PCs (ubu1 on 22.04, the rest on
#         24.04) and the driver (ubudrv) on their own network
#   test  the eight steps, each printing "STEP <k> PASS|FAIL <what>"; the
#         first miss stops the run
#   down  remove exactly what `up` recorded in $XDG_STATE_HOME/pipedpeer/ubulab.created
#
# Containers prove the scripts' logic and ordering, never speed: the PCs share
# the host's CPUs and have no GNOME session, proxy or captive portal.
#
# Environment: UBU_LAB_N (PCs, default 3), UBU_LAB_SLEEP (step 6 job, seconds,
# default 600), UBU_LAB_DROP (step 6 disconnect, seconds, default 90),
# UBU_LAB_FROM (test starts at that step), UBU_LAB_SKIP (space-separated step numbers to leave out), UBU_LAB_RMI=1 (down also removes the images), LAB_MEM_SPARE (see lib/lab.sh), UBU_LAB_MEM (memory per PC, default 2048m) and UBU_LAB_DRV_MEM (driver, default 3072m: ray and dask OOM below it), UBU_LAB_MEM1 (pc00, default 3072m: it holds the Ray head and Dask scheduler, which OOM at 2g),
# UBU_LAB_CPUSET (optional, e.g. 1-15: passed as --cpuset-cpus to every lab
# container, PCs and driver, so a suspect CPU such as CPU 0 can be avoided;
# unset means no pinning).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
# shellcheck source=/dev/null
source "$repo/scripts/lib/runtime.sh"
# shellcheck source=/dev/null
source "$repo/scripts/lib/lab.sh"

N="${UBU_LAB_N:-3}"
NET=ubulab
VOL=ubulab-results
DRV=ubudrv
IMG_DRV=pp-ubudrv
state_dir="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
created="$state_dir/ubulab.created"
snapdir="$state_dir/ubulab-snap"
PASSWORD_FILE=/home/lab/.labpw

usage() { sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

pp_lab_runtime || exit 1

# --- shared bits -----------------------------------------------------------

rec() { mkdir -p "$state_dir"; echo "$*" >>"$created"; }
recorded() { awk -v k="$1" '$1 == k { print $2 }' "$created" 2>/dev/null; }

# Fixed addresses so a reconnect (step 6) restores the driver's own: PCs from
# .16, driver .2; scanning only .16/28 keeps the bridge gateway out of the scan.
subnet() { recorded subnet; }
prefix() { local s; s="$(subnet)"; echo "${s%.0/24}"; }
pc_ip() { echo "$(prefix).$((15 + $1))"; }
drv_ip() { echo "$(prefix).2"; }
scan_cidr() { echo "$(prefix).16/28"; }
pc_release() { if (($1 == 1)); then echo 22.04; else echo 24.04; fi; }

# dr <shell command>: run in the driver, in the campaign directory.
dr() { "$rt" exec "$DRV" bash -c "cd /repo/scripts/campaign && $*"; }
# on <ubuN> <shell command>: run as root on a PC.
on() { local c="$1"; shift; "$rt" exec "$c" bash -c "$*"; }

STEP_K=0
pass() { printf 'STEP %s PASS %s\n' "$STEP_K" "$*"; }
fail() { printf 'STEP %s FAIL %s\n' "$STEP_K" "$*"; exit 1; }

# --- up ----------------------------------------------------------------------

cmd_up() {
	if [[ -s "$created" ]]; then
		echo "the Ubuntu lab is already up ($created); run '$0 down' first" >&2
		exit 1
	fi
	pp_lab_mem_check "$(($(pp_bytes "${UBU_LAB_MEM:-2048m}") * N + $(pp_bytes "${UBU_LAB_MEM1:-3072m}") - $(pp_bytes "${UBU_LAB_MEM:-2048m}") + $(pp_bytes 512m)))" ubulab
	local sub k rel
	sub="$(pp_lab_subnet ubulab 100)"
	rec subnet "$sub"
	rec runtime "$rt"
	for rel in 22.04 24.04; do
		echo "building pp-ubu:$rel"
		$rt build -q --build-arg "UBUNTU=$rel" -t "pp-ubu:$rel" -f "$repo/lab/ubuntu/Dockerfile" "$repo/lab/ubuntu" >/dev/null
		rec image "pp-ubu:$rel"
	done
	echo "building $IMG_DRV"
	$rt build -q -t "$IMG_DRV" -f "$repo/lab/ubuntu/Dockerfile.driver" "$repo/lab/ubuntu" >/dev/null
	rec image "$IMG_DRV"

	rec network "$NET"
	pp_lab_net "$NET" "$sub" nat
	rec volume "$VOL"
	$rt volume create "$VOL" >/dev/null

	local sysd=()
	if [[ "$rt" == podman ]]; then sysd=(--systemd=always); fi
	local cpuset=()
	if [[ -n "${UBU_LAB_CPUSET:-}" ]]; then
		cpuset=(--cpuset-cpus "$UBU_LAB_CPUSET")
		echo "pinning every lab container to CPUs $UBU_LAB_CPUSET (UBU_LAB_CPUSET)"
	fi
	local mem
	for ((k = 1; k <= N; k++)); do
		rel="$(pc_release "$k")"
		mem="${UBU_LAB_MEM:-2048m}"
		# pc00 carries the Ray head and the Dask scheduler besides its own work
		((k == 1)) && mem="${UBU_LAB_MEM1:-3072m}"
		rec container "ubu$k"
		$rt run -d --name "ubu$k" --hostname "ubu$k" --privileged --cgroupns=host \
			-v /sys/fs/cgroup:/sys/fs/cgroup:rw --tmpfs /run --tmpfs /run/lock \
			--memory "$mem" --memory-swap "$mem" "${sysd[@]}" "${cpuset[@]}" \
			--network "$NET" --ip "$(pc_ip "$k")" "pp-ubu:$rel" >/dev/null
	done
	rec container "$DRV"
	$rt run -d --name "$DRV" --hostname "$DRV" --memory "${UBU_LAB_DRV_MEM:-3072m}" --memory-swap "${UBU_LAB_DRV_MEM:-3072m}" "${cpuset[@]}" \
		-v "$repo:/repo:ro" -v "$VOL:/lab-results" \
		-e LAB_RESULTS=/lab-results -e REPO=/repo -e PP_BINARY=/repo/bin/pipedpeer \
		--network "$NET" --ip "$(drv_ip)" "$IMG_DRV" >/dev/null
	$rt exec "$DRV" git config --global --add safe.directory /repo
	for ((k = 1; k <= N; k++)); do
		# degraded is fine (no getty, no tty): the point is that systemd answers.
		if ! timeout 90 "$rt" exec "ubu$k" systemctl is-system-running --wait >/dev/null 2>&1; then
			state="$($rt exec "ubu$k" systemctl is-system-running 2>&1 || true)"
			[[ "$state" == running || "$state" == degraded ]] || {
				echo "ubu$k: systemd is '$state' after 90 s" >&2
				exit 1
			}
		fi
	done
	echo "up: $N PCs ($(subnet)) and $DRV"
}

# --- down --------------------------------------------------------------------

cmd_down() {
	if [[ ! -s "$created" ]]; then
		echo "down: nothing recorded in $created"
		return 0
	fi
	local c n v i left=0
	rt="$(recorded runtime | head -1)"
	[[ -n "$rt" ]] || rt=docker
	for c in $(recorded container); do
		$rt rm -f "$c" >/dev/null 2>&1 || { echo "down: container $c not removed" >&2; left=1; }
	done
	for n in $(recorded network); do $rt network rm "$n" >/dev/null 2>&1 || { echo "down: network $n not removed" >&2; left=1; }; done
	for v in $(recorded volume); do $rt volume rm "$v" >/dev/null 2>&1 || { echo "down: volume $v not removed" >&2; left=1; }; done
	if [[ "${UBU_LAB_RMI:-0}" == 1 ]]; then
		for i in $(recorded image); do $rt rmi "$i" >/dev/null 2>&1 || echo "down: image $i not removed" >&2; done
	fi
	if ((left)); then
		echo "down: something was not removed; $created kept so down can be run again" >&2
		return 1
	fi
	rm -rf "$snapdir"
	rm -f "$created"
	echo "down: done"
}

# --- test --------------------------------------------------------------------

# snap <name>: per PC, the state the lab-pc.sh undo must give back.
snap() {
	local name="$1" k out
	for ((k = 1; k <= N; k++)); do
		out="$snapdir/$name/ubu$k"
		mkdir -p "$(dirname "$out")"
		# shellcheck disable=SC2016  # the remote shell expands $1
		on "ubu$k" '
			echo "## dpkg"; dpkg -l | awk "NR>5 {print \$1, \$2}"
			echo "## sudoers.d"; ls /etc/sudoers.d
			echo "## lab authorized_keys"; cat /home/lab/.ssh/authorized_keys 2>/dev/null || true
			echo "## root authorized_keys"; cat /root/.ssh/authorized_keys 2>/dev/null || true
			echo "## ufw"; ufw status verbose 2>&1
			echo "## sleep targets"; systemctl is-enabled sleep.target suspend.target hibernate.target hybrid-sleep.target 2>&1 || true
			echo "## linger"; loginctl show-user lab --property=Linger 2>/dev/null | grep -qx Linger=yes && echo Linger=yes || echo Linger=no
			echo "## ssh"; systemctl is-active ssh.service ssh.socket 2>&1 || true
			echo "## lab user leftovers"; ls -d /home/lab/pipedpeer-lab /home/lab/.ssh/pipedpeer-lab-pc00* 2>&1 || true
			echo "## nix"; ls -d /nix 2>&1 || true
			echo "## lab state"; ls /var/lib/pipedpeer-lab 2>&1 || true' >"$out" 2>&1 || true
	done
}

step1() {
	STEP_K=1
	rm -rf "$snapdir"
	snap before
	[[ -s "$snapdir/before/ubu1" ]] || fail "snapshot is empty"
	pass "snapshot of $N PCs (dpkg, sudoers.d, authorized_keys, ufw, sleep targets)"
}

step2() {
	STEP_K=2
	local k line out s1 s2 drip
	drip="$(drv_ip)"
	"$rt" exec -d "$DRV" bash -c "cd /repo/scripts/campaign && ./serve.sh $drip $(subnet) >/lab-results/serve.log 2>&1"
	local tries=0
	until dr "wget -qO- http://$drip:8000/SETUP-LINE.txt" >"$state_dir/setup-line.txt" 2>/dev/null; do
		((++tries < 30)) || fail "serve.sh did not come up: $(dr 'tail -5 /lab-results/serve.log' 2>&1)"
		sleep 1
	done
	line="$(head -1 "$state_dir/setup-line.txt")"
	[[ "$line" == wget* ]] || fail "no setup line: $line"
	line="${line//sudo bash/sudo -S bash}"
	for ((k = 1; k <= N; k++)); do
		on "ubu$k" "echo lab >$PASSWORD_FILE; chown lab:lab $PASSWORD_FILE; chmod 600 $PASSWORD_FILE"
		out="$("$rt" exec -u lab "ubu$k" bash -c "$line <$PASSWORD_FILE" 2>&1)" \
			|| fail "setup line on ubu$k: $(printf '%s\n' "$out" | tail -5)"
		printf '%s\n' "$out" | grep -q '^LAB-PC host=' || fail "ubu$k printed no LAB-PC line: $(printf '%s\n' "$out" | tail -3)"
		s1="$(on "ubu$k" 'cat /var/lib/pipedpeer-lab/state')"
		out="$("$rt" exec -u lab "ubu$k" bash -c "$line <$PASSWORD_FILE" 2>&1)" \
			|| fail "second setup run on ubu$k: $(printf '%s\n' "$out" | tail -5)"
		s2="$(on "ubu$k" 'cat /var/lib/pipedpeer-lab/state')"
		[[ "$s1" == "$s2" ]] || fail "ubu$k: the second run changed the state file: $(diff <(echo "$s1") <(echo "$s2") | head -5)"
	done
	pass "setup line as lab on $N PCs; second run left the state file unchanged"
}

step3() {
	STEP_K=3
	local k out cnt c
	out="$(dr "./scan.sh $(scan_cidr) --pc00 $(pc_ip 1)" 2>&1)" || fail "scan.sh: $(printf '%s\n' "$out" | tail -5)"
	printf '%s\n' "$out" | grep -q "SCAN found=$N " || fail "scan.sh did not find $N PCs: $(printf '%s\n' "$out" | tail -5)"
	out="$(dr "source lib.sh; f=\$(lab_hosts $N); lab_fanout s3-true \$f true" 2>&1)" || fail "pssh true: $(printf '%s\n' "$out" | tail -5)"
	printf '%s\n' "$out" | grep -q "FANOUT step=s3-true ok=$N failed=0" || fail "pssh true not green: $(printf '%s\n' "$out" | tail -5)"
	out="$(dr "source lib.sh; f=\$(lab_hosts $N); lab_fanout s3-ports \$f 'sudo bash /tmp/lab-pc.sh ports'" 2>&1)" \
		|| fail "ports (ufw inactive): $(printf '%s\n' "$out" | tail -5)"
	printf '%s\n' "$out" | grep -q "FANOUT step=s3-ports ok=$N failed=0" || fail "ports not green with ufw inactive"
	for ((k = 1; k <= N; k++)); do
		on "ubu$k" 'ufw --force enable >/dev/null 2>&1 && ufw status | grep -q "Status: active"' \
			|| fail "ufw --force enable did not leave ufw active on ubu$k"
	done
	for c in 1 2; do # twice: the rules must appear once
		out="$(dr "source lib.sh; f=\$(lab_hosts $N); lab_fanout s3-ports-ufw$c \$f 'sudo bash /tmp/lab-pc.sh ports'" 2>&1)" \
			|| fail "ports (ufw active, run $c): $(printf '%s\n' "$out" | tail -5)"
		printf '%s\n' "$out" | grep -q "FANOUT step=s3-ports-ufw$c ok=$N failed=0" || fail "ports not green with ufw active (run $c)"
	done
	for ((k = 1; k <= N; k++)); do
		cnt="$(on "ubu$k" 'ufw status | grep -cE "^4242/tcp +ALLOW"')" || true
		[[ "$cnt" == 1 ]] || fail "ubu$k: 4242/tcp rule appears $cnt times after ports ran twice"
		on "ubu$k" 'ufw status | grep -q "ALLOW.*'"$(subnet)"'"' || fail "ubu$k: no rule for the lab subnet"
	done
	pass "scan found $N, pssh true green, ports green with ufw off and on (rules once)"
}

step4() {
	STEP_K=4
	local out k apparmor=""
	[[ -x "$repo/bin/pipedpeer" ]] || fail "step 4 needs $repo/bin/pipedpeer: run 'make build' first"
	out="$(dr "PP_BINARY=/repo/bin/pipedpeer ./install.sh dev" 2>&1)" || fail "install.sh: $(printf '%s\n' "$out" | tail -8)"
	printf '%s\n' "$out" | grep -q "INSTALL ok=$N failed=0" || fail "install.sh: $(printf '%s\n' "$out" | tail -5)"
	local ids
	# shellcheck disable=SC2016  # the driver's shell expands $4
	ids="$(dr 'awk -F, "NR>1 {print \$4}" /lab-results/install.csv | sort -u | wc -l')"
	[[ "$ids" == "$N" ]] || fail "status shows $ids distinct node ids, want $N"
	for ((k = 1; k <= N; k++)); do
		[[ "$(pc_release "$k")" == 24.04 ]] || continue
		if on "ubu$k" 'sysctl -n kernel.apparmor_restrict_unprivileged_userns' >/dev/null 2>&1; then
			on "ubu$k" 'ls /etc/apparmor.d | grep -qi pipedpeer' || fail "ubu$k: the AppArmor userns profile was not installed"
			apparmor=" AppArmor profile present"
		else
			apparmor=" AppArmor profile NOT CHECKED (this kernel has no AppArmor)"
		fi
	done
	pass "install.sh ok=$N, $ids distinct node ids;$apparmor"
}

step5() {
	STEP_K=5
	local out
	out="$(dr './pins.sh' 2>&1)" || fail "pins.sh: $(printf '%s\n' "$out" | tail -8)"
	out="$(dr './venv.sh' 2>&1)" || fail "venv.sh: $(printf '%s\n' "$out" | tail -8)"
	pass "pins.sh and venv.sh built the same pinned venv on 22.04 and 24.04 (python $(dr 'cat /lab-results/python-version'))"
}

step6() {
	STEP_K=6
	local sl="${UBU_LAB_SLEEP:-600}" drop="${UBU_LAB_DROP:-90}" id=SSHDROP out code
	dr "rm -rf /lab-results/hosts/pc01/runs/$id; source lib.sh; lab_ssh pc01 'rm -rf ~/pipedpeer-lab/runs/$id'" >/dev/null 2>&1 || true
	dr "source lib.sh; lab_ssh pc01 'mkdir -p ~/pipedpeer-lab/work' ; lab_rsync stamp.py pc01:pipedpeer-lab/work/; lab_detach pc01 $id 'sleep $sl; echo done'" \
		|| fail "could not start the detached job"
	"$rt" network disconnect "$NET" "$DRV"
	echo "driver off the network for ${drop}s (job: sleep $sl)"
	sleep "$drop"
	"$rt" network connect --ip "$(drv_ip)" "$NET" "$DRV"
	out="$(dr "source lib.sh; lab_detach pc01 $id 'sleep $sl; echo done'" 2>&1)" || fail "second detach: $out"
	[[ "$out" == *ALREADY-STARTED* ]] || fail "the job was started a second time: $out"
	code="$(dr "source lib.sh; lab_wait pc01 $id $((sl + 300))" 2>/dev/null)" || fail "lab_wait: $code"
	[[ "$code" == 0 ]] || fail "job exit $code"
	dr "source lib.sh; lab_fetch pc01 $id /lab-results/step6" >/dev/null 2>&1 || fail "fetch of the job's result"
	[[ "$(dr 'cat /lab-results/step6/out')" == "done" ]] || fail "job output is not exactly one 'done'"
	pass "job survived a ${drop}s drop, was not started twice, result fetched"
}

step7() {
	STEP_K=7
	local log=/lab-results/logs/step7 name cmd
	dr "mkdir -p $log; mkdir -p ~/.config/pipedpeer-lab /srv/s3/pipedpeer-lab"
	dr 'cat >~/.config/pipedpeer-lab/r2.env <<EOF
RCLONE_CONFIG_R2_TYPE=s3
RCLONE_CONFIG_R2_PROVIDER=Other
RCLONE_CONFIG_R2_ACCESS_KEY_ID=lab
RCLONE_CONFIG_R2_SECRET_ACCESS_KEY=labsecret
RCLONE_CONFIG_R2_ENDPOINT=http://127.0.0.1:8333
EOF
chmod 600 ~/.config/pipedpeer-lab/r2.env'
	"$rt" exec -d "$DRV" bash -c 'rclone serve s3 /srv/s3 --addr 127.0.0.1:8333 --auth-key lab,labsecret >/lab-results/rclone-serve.log 2>&1'
	sleep 3
	local -a seq=(
		"check-driver|./check-driver.sh"
		"stage|./stage.sh"
		"inventory|./inventory.sh"
		"clocks|./clocks.sh"
		"network|./network.sh"
		"calibrate|./calibrate.sh --target 5 --force"
		"SMOKE|./run.sh SMOKE"
		"REF|./run.sh REF --sizes 1"
		"FC|./run.sh FC --repeats 2"
		"E7|./run.sh E7 --repeats 1"
		"E8|./run.sh E8 --sizes 2 --repeats 1"
		"e4|./e4.sh --dry-run"
		"e5|./e5.sh --n 2"
		"e6|./e6.sh --n 2"
		"e11|./e11.sh --sizes 2 --idle 30"
		"upload|./upload.sh"
		"analyse|python3 ./analyse.py --lab /lab-results"
	)
	local item
	for item in "${seq[@]}"; do
		name="${item%%|*}"
		cmd="${item#*|}"
		echo "step 7: $cmd"
		dr "$cmd >$log/$name.log 2>&1" || {
			dr "tail -25 $log/$name.log" >&2 || true
			fail "$cmd (full output: $log/$name.log in the driver's /lab-results)"
		}
	done
	dr "grep -q UPLOAD-OK $log/upload.log" || fail "upload.sh printed no UPLOAD-OK"
	pass "SMOKE, REF, FC, E7, E8, e5, e6, e11 ran for real; e4 only as --dry-run and E1/E1b not at all; upload and analyse done"
}

step8() {
	STEP_K=8
	local k out tries d
	# The PCs the earlier steps used (lab_hosts N), not every PC in the host table: a
	# down or unrelated PC must not turn the fanout into a failure.
	dr "source lib.sh; f=\$(lab_hosts $N); lab_fanout s8-uninstall \$f 'pipedpeer uninstall -y'" >/dev/null 2>&1 \
		|| fail "pipedpeer uninstall -y failed on every PC"
	dr "source lib.sh; f=\$(lab_hosts $N); lab_fanout s8-undo \$f 'sudo systemd-run --unit=lab-undo bash /tmp/lab-pc.sh undo'" >/dev/null 2>&1 \
		|| fail "could not start the detached undo"
	for ((k = 1; k <= N; k++)); do
		tries=0
		until on "ubu$k" 'journalctl -u lab-undo --no-pager 2>/dev/null | grep -q "undo complete"'; do
			((++tries < 60)) || fail "ubu$k: no 'undo complete' in the journal after 120 s"
			sleep 2
		done
	done
	# ufw was switched on by step 3 (the test's own doing, not lab-pc.sh's): off
	# again, after undo had to cope with an active one.
	for ((k = 1; k <= N; k++)); do on "ubu$k" 'ufw --force disable >/dev/null'; done
	snap after
	d=0
	for ((k = 1; k <= N; k++)); do
		# Nix from the stock installer cannot be removed by undo; it says so and
		# names the manual step. That is the expected path, not a leftover.
		if on "ubu$k" 'journalctl -u lab-undo --no-pager 2>/dev/null | grep -q "Nix left in place (stock installer"'; then
			echo "WARN ubu$k: stock-installer Nix left in place, as undo said; /nix not compared" >&2
			if ! out="$(diff <(grep -vE "^(/nix|ls: cannot access '/nix')" "$snapdir/before/ubu$k") <(grep -vE "^(/nix|ls: cannot access '/nix')" "$snapdir/after/ubu$k"))"; then
				printf 'ubu%s differs after undo:\n%s\n' "$k" "$out" >&2
				d=1
			fi
		elif ! out="$(diff "$snapdir/before/ubu$k" "$snapdir/after/ubu$k")"; then
			printf 'ubu%s differs after undo:\n%s\n' "$k" "$out" >&2
			d=1
		fi
	done
	((d == 0)) || fail "the snapshot after undo differs from step 1"
	pass "uninstall and undo left every PC as step 1 found it"
}

cmd_test() {
	[[ -s "$created" ]] || { echo "test: the lab is not up; run '$0 up' first" >&2; exit 1; }
	rt="$(recorded runtime | head -1)"
	local s
	# UBU_LAB_FROM=<k> restarts at step k on a lab the earlier steps already set up.
	for s in step1 step2 step3 step4 step5 step6 step7 step8; do
		((${s#step} >= ${UBU_LAB_FROM:-1})) || continue
		[[ " ${UBU_LAB_SKIP:-} " != *" ${s#step} "* ]] || { echo "skipping $s (UBU_LAB_SKIP)"; continue; }
		"$s"
	done
}

case "${1:-}" in
	up) cmd_up ;;
	test) cmd_test ;;
	down) cmd_down ;;
	-h | --help) usage ;;
	*) usage >&2; exit 2 ;;
esac
