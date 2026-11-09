#!/usr/bin/env bash
# wipe.sh: removes what the lab setup created.
# Refuses to run without a results bundle whose MANIFEST.sha256 verifies. The
# bundle, and everything else in ~/Documents/pipedpeer, is never deleted.
set -uo pipefail
shopt -s nullglob

usage() {
	cat <<-EOF
		usage: wipe.sh [--keep DIR] [--dry-run]

		Options:
		  --keep DIR    Folder with the results bundle and MANIFEST.sha256 (default: ~/Documents/pipedpeer/lab-bundle)
		  --dry-run     Show what would be removed without deleting anything
	EOF
}

KEEP_DIR=""
DRY_RUN=0

while [[ $# -gt 0 ]]; do
	case "$1" in
	--keep)
		if [[ $# -lt 2 ]]; then
			echo "Error: --keep requires a directory argument" >&2
			exit 1
		fi
		KEEP_DIR="$2"
		shift 2
		;;
	--dry-run)
		DRY_RUN=1
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		echo "Unknown argument: $1" >&2
		usage >&2
		exit 1
		;;
	esac
done

# 1. Default to the folder collect.sh writes
KEEP_DIR="${KEEP_DIR:-$HOME/Documents/pipedpeer/lab-bundle}"

# 2. Require keep DIR to exist and contain MANIFEST.sha256
if [[ ! -d "$KEEP_DIR" ]]; then
	echo "Error: no results bundle at '$KEEP_DIR'. Run scripts/campaign/collect.sh first, or name the bundle with --keep DIR." >&2
	exit 1
fi

if [[ ! -f "$KEEP_DIR/MANIFEST.sha256" ]]; then
	echo "Error: '$KEEP_DIR' has no MANIFEST.sha256, so it is not a finished bundle. Run scripts/campaign/collect.sh first." >&2
	exit 1
fi

# 3. Verify MANIFEST.sha256 in keep DIR
manifest_output=""
if ! manifest_output="$( (cd "$KEEP_DIR" && sha256sum -c MANIFEST.sha256) 2>&1 )"; then
	bad_file="$(grep -E 'FAILED|FAILED open or read' <<<"$manifest_output" | head -1 | sed 's/:.*//')"
	echo "Error: a file in keep dir changed since manifest: ${bad_file:-unknown file}" >&2
	exit 1
fi

# 4. Read setup record if present
STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer-lab-setup"
RECORD_FILE="$STATE_DIR/record"
RECORD_REPO=""
NIX_PREEXISTED=""

if [[ -f "$RECORD_FILE" ]]; then
	while IFS='=' read -r key val || [[ -n "$key" ]]; do
		case "$key" in
		repo) RECORD_REPO="$val" ;;
		nix_preexisted) NIX_PREEXISTED="$val" ;;
		esac
	done <"$RECORD_FILE"
fi

# 5. Check if DIR is inside the recorded repository
if [[ -n "$RECORD_REPO" && -d "$RECORD_REPO" ]]; then
	can_keep="$(cd "$KEEP_DIR" && pwd -P)"
	can_repo="$(cd "$RECORD_REPO" && pwd -P)"
	if [[ "$can_keep" == "$can_repo" || "$can_keep" == "$can_repo"/* ]]; then
		echo "Error: keep directory '$KEEP_DIR' is inside the recorded repo '$RECORD_REPO'" >&2
		exit 1
	fi
fi

# The recorded path is deleted whole, so only a real checkout may go. Empty means
# it may be deleted; otherwise the reason it stays.
repo_refusal=""
if [[ -n "$RECORD_REPO" && -d "$RECORD_REPO" ]]; then
	can_repo="$(cd "$RECORD_REPO" && pwd -P)"
	can_home="$(cd "$HOME" && pwd -P)"
	if [[ "$can_repo" == / || "$can_home" == "$can_repo" || "$can_home" == "$can_repo"/* ]]; then
		repo_refusal="it holds the home folder"
	elif [[ ! -e "$can_repo/.git" || ! -f "$can_repo/scripts/campaign/wipe.sh" ]]; then
		repo_refusal="it is not a Pipedpeer checkout"
	fi
fi

# 6. Check that keep DIR is not inside, and has no symlink to, a path being deleted
PATHS_TO_CHECK=(
	"${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer"
	"${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
	"${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer-campaign-tests"
	"${XDG_CACHE_HOME:-$HOME/.cache}/pipedpeer"
	"${XDG_CACHE_HOME:-$HOME/.cache}/pipedpeer-lab-dev"
	"$RECORD_FILE"
)
if [[ -n "$RECORD_REPO" ]]; then
	PATHS_TO_CHECK+=("$RECORD_REPO")
fi

can_keep="$(cd "$KEEP_DIR" && pwd -P)"
for dp in "${PATHS_TO_CHECK[@]}"; do
	[[ -e "$dp" ]] || continue
	can_dp="$(readlink -f "$dp")"
	if [[ "$can_keep" == "$can_dp" || "$can_keep" == "$can_dp"/* ]]; then
		echo "Error: keep directory '$KEEP_DIR' is inside '$dp', which is being deleted" >&2
		exit 1
	fi
done

while IFS= read -r -d '' link; do
	target="$(readlink -f "$link" 2>/dev/null || true)"
	[[ -n "$target" ]] || continue
	for dp in "${PATHS_TO_CHECK[@]}"; do
		if [[ -e "$dp" || -L "$dp" ]]; then
			can_dp="$(readlink -f "$dp" 2>/dev/null || true)"
			if [[ -n "$can_dp" && ( "$target" == "$can_dp" || "$target" == "$can_dp"/* ) ]]; then
				echo "Error: symlink '$link' points to path being deleted ('$target')" >&2
				exit 1
			fi
		fi
	done
	if [[ "$target" == "$HOME/.ssh/pipedpeer-lab"* ]]; then
		echo "Error: symlink '$link' points to path being deleted ('$target')" >&2
		exit 1
	fi
done < <(find "$KEEP_DIR" -type l -print0)

# 7. List what would be removed
NIX_INSTALLER="${NIX_INSTALLER:-/nix/nix-installer}"
NIX_DIR="$(dirname "$NIX_INSTALLER")"
IMAGES=(docker.io/nixos/nix:2.31.3 lab-worker1:latest lab-worker2:latest lab-worker3:latest
	pp-net:latest pp-emu:latest pp-net-nix:latest pp-ubu:22.04 pp-ubu:24.04 pp-ubudrv:latest)

echo "These will be deleted:"
if command -v pipedpeer >/dev/null 2>&1; then
	echo "  pipedpeer daemon and program (via pipedpeer uninstall -y)"
fi
systemd_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
for u in pipedpeer.service pipedpeer-agents.slice; do
	if [[ -e "$systemd_dir/$u" ]]; then
		echo "  user systemd unit $systemd_dir/$u"
	fi
done

for p in "${PATHS_TO_CHECK[@]}"; do
	if [[ -n "$repo_refusal" && "$p" == "$RECORD_REPO" ]]; then
		continue
	fi
	if [[ -e "$p" || -L "$p" ]]; then
		echo "  path $p"
	fi
done
for k in "$HOME"/.ssh/pipedpeer-lab*; do
	if [[ -e "$k" || -L "$k" ]]; then
		echo "  ssh key $k"
	fi
done

for rt in podman docker; do
	if command -v "$rt" >/dev/null 2>&1; then
		for img in "${IMAGES[@]}"; do
			if "$rt" image inspect "$img" >/dev/null 2>&1; then
				echo "  $rt image $img and its containers"
			fi
		done
	fi
done

if [[ ! -f "$RECORD_FILE" ]]; then
	echo "Setup record missing: no nix uninstaller call, repo will not be deleted."
elif [[ "$NIX_PREEXISTED" == "yes" ]]; then
	echo "Nix was kept because record indicates nix_preexisted=yes."
elif [[ "$NIX_PREEXISTED" == "no" ]]; then
	if [[ -x "$NIX_INSTALLER" ]]; then
		echo "Nix will be uninstalled via $NIX_INSTALLER uninstall --no-confirm."
	elif [[ -d "$NIX_DIR" ]]; then
		echo "Nix is at $NIX_DIR without its installer, so it will be left and reported."
	else
		echo "Nix was never installed here: nothing to remove."
	fi
fi
if [[ -n "$repo_refusal" ]]; then
	echo "Recorded repo $RECORD_REPO will not be deleted: $repo_refusal."
fi

if ((DRY_RUN)); then
	echo
	echo "Dry run: nothing was deleted."
	exit 0
fi

# 8. Prompt for explicit confirmation
echo
read -r -p "Type DELETE EVERYTHING to go on: " answer
if [[ "$answer" != "DELETE EVERYTHING" ]]; then
	echo "Stopped; nothing was deleted." >&2
	exit 1
fi

# 9. Perform cleanup with problem tracking
problems=0
fail() {
	echo "PROBLEM: $*" >&2
	problems=$((problems + 1))
}
run() {
	local what="$1"
	shift
	echo "==> $what"
	"$@" || fail "$what failed (exit $?): $*"
}

# (a) pipedpeer uninstall
if command -v pipedpeer >/dev/null 2>&1; then
	run "pipedpeer uninstall" pipedpeer uninstall -y
fi

# (b) systemd units
for u in pipedpeer.service pipedpeer-agents.slice; do
	if [[ -e "$systemd_dir/$u" ]]; then
		if command -v systemctl >/dev/null 2>&1; then
			if [[ "$u" == *.service ]]; then
				systemctl --user disable --now "$u" || fail "could not disable $u"
			fi
		fi
		rm -f "$systemd_dir/$u" || fail "could not remove $systemd_dir/$u"
	fi
done
if command -v systemctl >/dev/null 2>&1; then
	systemctl --user daemon-reload || fail "systemctl --user daemon-reload failed"
fi

# (c) Leftover binary in /usr/local/bin
bin_loc="${PIPEDPEER_USR_LOCAL_BIN:-/usr/local/bin/pipedpeer}"
if [[ -e "$bin_loc" ]]; then
	run "remove leftover $bin_loc" sudo rm -f "$bin_loc"
fi

# (d) Nix uninstall (only if recorded nix_preexisted=no)
if [[ ! -f "$RECORD_FILE" ]]; then
	echo "Record missing: skipping Nix uninstall."
elif [[ "$NIX_PREEXISTED" == "yes" ]]; then
	echo "Nix was kept because record indicates nix_preexisted=yes."
elif [[ "$NIX_PREEXISTED" == "no" ]]; then
	if [[ -x "$NIX_INSTALLER" ]]; then
		run "uninstall nix" sudo "$NIX_INSTALLER" uninstall --no-confirm
	elif [[ -d "$NIX_DIR" ]]; then
		fail "nix at $NIX_DIR has no executable $NIX_INSTALLER; left alone. To remove it by hand: https://nixos.org/manual/nix/stable/installation/uninstall.html"
	else
		echo "Nix was never installed here: nothing to remove."
	fi
fi

# (e) Containers and images
for rt in podman docker; do
	if command -v "$rt" >/dev/null 2>&1; then
		for img in "${IMAGES[@]}"; do
			if "$rt" image inspect "$img" >/dev/null 2>&1; then
				while IFS= read -r c; do
					if [[ -n "$c" ]]; then
						run "$rt rm -f $c" "$rt" rm -f "$c"
					fi
				done < <("$rt" ps -aq --filter "ancestor=$img" 2>/dev/null)
				run "$rt rmi -f $img" "$rt" rmi -f "$img"
			fi
		done
	fi
done

# (f) Directories and SSH keys
PATHS_TO_DELETE=(
	"${XDG_DATA_HOME:-$HOME/.local/share}/pipedpeer"
	"${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
	"${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer-campaign-tests"
	"${XDG_CACHE_HOME:-$HOME/.cache}/pipedpeer"
	"${XDG_CACHE_HOME:-$HOME/.cache}/pipedpeer-lab-dev"
)
for p in "${PATHS_TO_DELETE[@]}"; do
	if [[ -e "$p" || -L "$p" ]]; then
		echo "==> delete $p"
		rm -rf -- "$p" || fail "could not delete $p"
	fi
done

for k in "$HOME"/.ssh/pipedpeer-lab*; do
	if [[ -e "$k" || -L "$k" ]]; then
		echo "==> delete $k"
		rm -f -- "$k" || fail "could not delete $k"
	fi
done

# (g) Recorded repository (deleted LAST)
if [[ ! -f "$RECORD_FILE" ]]; then
	echo "Setup record missing: repo will not be deleted."
elif [[ -n "$repo_refusal" ]]; then
	fail "recorded repo $RECORD_REPO was not deleted: $repo_refusal"
elif [[ -n "$RECORD_REPO" && -d "$RECORD_REPO" ]]; then
	echo "==> delete recorded repo $RECORD_REPO"
	rm -rf -- "$RECORD_REPO" || fail "could not delete recorded repo $RECORD_REPO"
fi

# (h) Record file
if [[ -f "$RECORD_FILE" ]]; then
	echo "==> delete record file $RECORD_FILE"
	rm -f "$RECORD_FILE" || fail "could not delete $RECORD_FILE"
	rmdir "$(dirname "$RECORD_FILE")" 2>/dev/null || true
fi

echo
if ((problems > 0)); then
	echo "Finished with $problems problems; see the PROBLEM lines above." >&2
	exit 1
fi
echo "Done. Pipedpeer lab cleanup complete."
exit 0
