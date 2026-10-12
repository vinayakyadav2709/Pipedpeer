#!/usr/bin/env bash
# Checks shared by setup-machine.sh and demo/prep-1-check.sh. Sourced, never run;
# sourcing changes nothing. Every problem message has one shape: what is wrong,
# numbered steps to fix it, how to check, then "re-run this same command".

# The line the shell startup files get. Single quotes: $HOME and $PATH expand
# when the shell starts, not now.
PP_PATH_LINE='export PATH="$HOME/.local/bin:$PATH"'

# ensure_local_bin_path: the installer puts pipedpeer in ~/.local/bin, which
# some distros leave off PATH. Adds PP_PATH_LINE to ~/.bashrc and ~/.zshrc
# (only those that exist, never twice) and exports it for the rest of the run.
ensure_local_bin_path() {
	case ":$PATH:" in *":$HOME/.local/bin:"*) return 0 ;; esac
	local rc added=0
	for rc in "$HOME/.bashrc" "$HOME/.zshrc"; do
		[[ -f "$rc" ]] || continue
		added=1
		if grep -qxF "$PP_PATH_LINE" "$rc"; then
			echo "PATH: $rc already adds ~/.local/bin (open a new terminal to pick it up)"
		else
			printf '\n# added by pipedpeer-demo/setup-machine.sh\n%s\n' "$PP_PATH_LINE" >>"$rc"
			echo "PATH: added ~/.local/bin to $rc"
		fi
	done
	if [[ "$added" == 0 ]]; then
		echo "PATH: no ~/.bashrc or ~/.zshrc here; add this line to your shell's startup file: $PP_PATH_LINE" >&2
	fi
	export PATH="$HOME/.local/bin:$PATH"
	echo "PATH: on for this run. In this terminal, run: $PP_PATH_LINE"
}

# reach HOST PORT: 0 when a TCP connection opens within 2 s. Plain bash, so no
# nc or ping is needed (ping proves little: ICMP and TCP are filtered apart).
reach() { timeout 2 bash -c ': </dev/tcp/"$1"/"$2"' _ "$1" "$2" 2>/dev/null; }

# reach_fail NAME HOST PORT: the "can't reach" message, on stderr.
reach_fail() {
	local name="$1" host="$2" port="$3"
	cat >&2 <<EOF

PROBLEM: this machine can't reach $name at $host:$port (no TCP connection within 2 s).
Most likely the Wi-Fi blocks devices from talking to each other (client isolation).
pipedpeer relays nothing, so every machine needs a direct path to the others.

Fix, in order (stop at the first that works):
  1. On $name: pipedpeer status
     If the daemon is not running: pipedpeer start
  2. $name's firewall: re-run ./demo/prep-0-install.sh there (its setup opens 38080/tcp).
  3. Move every machine to a network you control (a phone hotspot or your own router).
     On LAPTOP-A run  ip -4 addr  to find its new IP, and join with that IP
     (./setup-machine.sh <role> --token <secret> --introducer <new IP>).

Check, from this machine:
  timeout 2 bash -c ': </dev/tcp/$host/$port' && echo reachable

Then re-run this same command.
EOF
}

# gpu_driver_ok: nvidia-smi exists and can talk to the driver.
gpu_driver_ok() { command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi -L >/dev/null 2>&1; }

# gpu_driver_msg: the "no NVIDIA driver" warning, on stderr. The caller goes on.
gpu_driver_msg() {
	cat >&2 <<'EOF'

WARNING: no working NVIDIA driver here (nvidia-smi is missing or fails).
Setup goes on: this machine still joins, as a CPU-only node. But the gpu-laptop role
is here for its GPU: until the driver works, the GPU step (5) cannot use this machine.

Fix on Fedora:
  1. Enable RPM Fusion:
     sudo dnf install https://mirrors.rpmfusion.org/free/fedora/rpmfusion-free-release-$(rpm -E %fedora).noarch.rpm https://mirrors.rpmfusion.org/nonfree/fedora/rpmfusion-nonfree-release-$(rpm -E %fedora).noarch.rpm
  2. sudo dnf install akmod-nvidia
  3. Wait for the module build (about 5 min): until  modinfo -F version nvidia  prints a version.
  4. Reboot. (With Secure Boot on, the module loads only once its key is enrolled:
     https://rpmfusion.org/Howto/Secure%20Boot)
Fix on Ubuntu:
  1. sudo ubuntu-drivers install
  2. Reboot.

Check: nvidia-smi lists the GPU.

Then re-run this same command.
EOF
}
