#!/usr/bin/env bash
# One script per machine role. Safe to re-run: each step checks before it acts.
#
#   ./setup-machine.sh <role> [--introducer IP] [--token T] [--rendezvous] [--prep]
#
# role: ubuntu-pc | fedora-laptop | gpu-laptop
#   --introducer IP  join through this machine (the laptop running `pipedpeer rendezvous`
#                    or any machine already in the cluster). Without it, rely on LAN
#                    auto-discovery: the daemon finds peers that share the secret.
#                    Checked first: no TCP connection to IP:38080, no join.
#   --token T        cluster secret from `pipedpeer auth show` on the first machine.
#                    Without it, this machine makes a new secret (first machine only)
#                    and writes join-command.txt: the other machines' lines, ready to paste.
#   --rendezvous     first machine: run `pipedpeer rendezvous` as the systemd user
#                    service pipedpeer-rendezvous (no terminal to keep open).
# Also adds ~/.local/bin to PATH in ~/.bashrc and ~/.zshrc when it is missing.
#   --prep           also run the prep scripts at the lab visit (prep-4-common, and
#                    prep-3-gpu on gpu-laptop). Slow the first time.
# Steps are those of join-machine.sh (install, setup, auth, join) plus the prep scripts;
# nothing is copied from them.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
role="${1:-}"
[[ -n "$role" ]] && shift || true
introducer="" token="" prep=0 rendezvous=0
while [[ $# -gt 0 ]]; do
	case "$1" in
	--introducer) introducer="${2:?--introducer needs an IP}"; shift 2 ;;
	--token) token="${2:?--token needs a value}"; shift 2 ;;
	--prep) prep=1; shift ;;
	--rendezvous) rendezvous=1; shift ;;
	*) echo "unknown argument: $1" >&2; exit 2 ;;
	esac
done
case "$role" in
ubuntu-pc | fedora-laptop | gpu-laptop) ;;
*) echo "usage: $0 ubuntu-pc|fedora-laptop|gpu-laptop [--introducer IP] [--token T] [--rendezvous] [--prep]" >&2; exit 2 ;;
esac

source "$here/demo/_checks.sh"

# write_join_command TOKEN: the first machine writes the other machines' lines,
# with the real secret and this machine's LAN IP, so nobody retypes the token.
write_join_command() {
	local tok="$1" ip file="$here/join-command.txt"
	ip="$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i = 1; i < NF; i++) if ($i == "src") { print $(i + 1); exit }}')" || true
	if [[ -z "$ip" ]]; then
		echo "warn: no IPv4 route out of this machine; join-command.txt says <LAPTOP-A IP> instead (find it with: ip -4 addr)" >&2
		ip="<LAPTOP-A IP>"
	fi
	(
		umask 077
		cat >"$file" <<EOF
# Contains the cluster secret: send it to yourself (chat, email or a secret gist), never commit it.
# Written by setup-machine.sh on the first machine ($ip). Run each line in pipedpeer-demo/.

# LAPTOP-B (the GPU laptop):
./setup-machine.sh gpu-laptop --token $tok --introducer $ip --prep

# A lab PC that is not PC-3:
./setup-machine.sh ubuntu-pc --token $tok --introducer $ip --prep

# The live-join machine (PC-3, or LAPTOP-C in layout 1), on stage in step 1:
./demo/0-join.sh $ip $tok
# or by hand:  pipedpeer auth set $tok && pipedpeer join $ip
EOF
	)
	chmod 600 "$file"
	echo "Secret made here. The other machines' commands are in $file (mode 600)."
	echo "Send it to yourself (chat, email or a secret gist) and open it on the others, or type the values: --token $tok --introducer $ip"
}

# start_rendezvous: `pipedpeer rendezvous` as a systemd user service, so it
# outlives the terminal. Re-running finds it active and starts nothing.
start_rendezvous() {
	local unit=pipedpeer-rendezvous bin pids dir="$HOME/.config/systemd/user"
	if systemctl --user is-active --quiet "$unit" 2>/dev/null; then
		echo "already running ($unit)"
	else
		if ! systemctl --user show-environment >/dev/null 2>&1; then
			cat >&2 <<'EOF'

PROBLEM: no systemd user session here (systemctl --user can't connect), so the
rendezvous can't run as a service.
Fix, either:
  1. Log in at this machine's own screen (not over ssh) and run this there, or
  2. run  pipedpeer rendezvous  in a terminal of its own and leave it open.
Check: systemctl --user status  prints a tree, not "Failed to connect".
Then re-run this same command.
EOF
			return 1
		fi
		if pids="$(pgrep -u "$(id -u)" -f '(^|/)pipedpeer rendezvous( |$)')"; then
			cat >&2 <<EOF

PROBLEM: a rendezvous already runs outside the service (pid $(echo $pids)),
so the service can't take its ports.
Fix:
  1. Stop it: Ctrl+C in its terminal, or  kill $(echo $pids)
Check: pgrep -af 'pipedpeer rendezvous'  prints nothing.
Then re-run this same command.
EOF
			return 1
		fi
		bin="$(command -v pipedpeer)"
		mkdir -p "$dir"
		cat >"$dir/$unit.service" <<EOF
[Unit]
Description=pipedpeer rendezvous (introduces the demo machines to each other)

[Service]
ExecStart=$bin rendezvous
Restart=on-failure
RestartSec=2

[Install]
WantedBy=default.target
EOF
		systemctl --user daemon-reload
		systemctl --user enable --now "$unit"
		sleep 1
		if ! systemctl --user is-active --quiet "$unit"; then
			cat >&2 <<EOF

PROBLEM: the $unit service did not stay up.
Fix:
  1. Read why: journalctl --user -u $unit -n 20
  2. "address already in use" (38443-38445/udp): another rendezvous runs; stop it.
Check: systemctl --user is-active $unit  prints active.
Then re-run this same command.
EOF
			return 1
		fi
		echo "started $unit: closing this terminal does not stop it"
	fi
	# Without linger, systemd stops user services at logout (not at screen lock).
	if [[ "$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null)" != yes ]]; then
		echo "note: logging out stops it (locking the screen does not). To keep it across logout: loginctl enable-linger"
	fi
	echo "status: systemctl --user status $unit    stop: systemctl --user disable --now $unit"
}

echo "==> [$role] PATH"
ensure_local_bin_path

echo "==> [$role] install"
if command -v pipedpeer >/dev/null 2>&1; then
	echo "already installed: $(command -v pipedpeer)"
else
	curl -fsSL https://raw.githubusercontent.com/vinayakyadav2709/Pipedpeer/dev/scripts/install-pipedpeer.sh |
		bash -s -- --channel nightly
fi

# A warning, not a stop: without the driver this machine still joins, as CPU.
if [[ "$role" == "gpu-laptop" ]] && ! gpu_driver_ok; then
	gpu_driver_msg
fi

echo "==> [$role] setup (nix, crun, identity, firewall, daemon)"
# Opens the ports itself (sudo): 38080/tcp 38099/udp 38447/udp 38445/udp 29500-29509/tcp (setup.FirewallRules). If the firewall still blocks peers on Ubuntu: sudo ufw disable
pipedpeer setup -y

echo "==> [$role] cluster secret"
if [[ -n "$token" ]]; then
	pipedpeer auth set "$token"
else
	pipedpeer auth set
	write_join_command "$(pipedpeer auth show | head -1)"
fi

if [[ "$rendezvous" == 1 ]]; then
	echo "==> [$role] rendezvous (systemd user service pipedpeer-rendezvous)"
	start_rendezvous
fi

if [[ -n "$introducer" ]]; then
	echo "==> [$role] can this machine reach $introducer:38080?"
	if ! reach "$introducer" 38080; then
		reach_fail "LAPTOP-A ($introducer)" "$introducer" 38080
		exit 1
	fi
	echo "reachable"
	echo "==> [$role] join through $introducer"
	pipedpeer join "$introducer"
else
	# The daemon reads the secret at start; without a restart it keeps answering
	# without proving the token, and peers refuse it (join restarts it for us).
	echo "==> [$role] restarting the daemon so it uses the secret"
	pipedpeer stop >/dev/null 2>&1 || echo "note: no daemon was running"
	pipedpeer start
	echo "==> [$role] no --introducer: waiting on LAN auto-discovery (see 'pipedpeer nodes' in ~30 s)"
fi

if [[ "$prep" == 1 ]]; then
	"$here/demo/prep-4-common.sh"
	[[ "$role" == "gpu-laptop" ]] && "$here/demo/prep-3-gpu.sh"
fi
echo "Done. Check: pipedpeer status; pipedpeer nodes"
