#!/usr/bin/env bash
# shellcheck disable=SC2015,SC2030,SC2031,SC2088,SC2317
# Case table tests for wipe.sh.
# Tests use a throwaway HOME and XDG directories with stubs for pipedpeer,
# systemctl, podman, docker, and nix-installer.
set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CAMP="$REPO/scripts/campaign"
WIPE="$CAMP/wipe.sh"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer-campaign-tests"
mkdir -p "$STATE"

RESULTS="$(mktemp)"
problems=()
ROOT="$(mktemp -d "$STATE/wipe-test.XXXXXX")"

cleanup() {
	if [[ "${LAB_TEST_KEEP:-0}" != 1 ]]; then
		rm -rf "$ROOT"
	fi
	rm -f "$RESULTS"
}
trap cleanup EXIT

ok() { echo ok >>"$RESULTS"; printf 'ok   %s\n' "$1"; }
bad() { echo bad >>"$RESULTS"; printf 'FAIL %s%s\n' "$1" "${2:+ -- $2}"; }
pnote() { problems+=("$*"); }
verdict() {
	if ((${#problems[@]} == 0)); then
		ok "$1"
	else
		bad "$1" "${problems[*]}"
	fi
	problems=()
}

newdir() {
	mktemp -d "$ROOT/case.XXXXXX"
}

setup_env() {
	local d="$1"
	mkdir -p "$d/home" "$d/bin" "$d/logs" "$d/fake_repo" "$d/keep" "$d/nix"

	# Pipedpeer and lab paths to be removed
	mkdir -p "$d/home/.local/share/pipedpeer" && echo "share" >"$d/home/.local/share/pipedpeer/data"
	mkdir -p "$d/home/.local/state/pipedpeer" && echo "state" >"$d/home/.local/state/pipedpeer/state"
	mkdir -p "$d/home/.local/state/pipedpeer-campaign-tests" && echo "camp" >"$d/home/.local/state/pipedpeer-campaign-tests/camp"
	mkdir -p "$d/home/.cache/pipedpeer" && echo "cache" >"$d/home/.cache/pipedpeer/cache"
	mkdir -p "$d/home/.cache/pipedpeer-lab-dev" && echo "lab-dev" >"$d/home/.cache/pipedpeer-lab-dev/dev"
	mkdir -p "$d/home/.ssh"
	touch "$d/home/.ssh/pipedpeer-lab" "$d/home/.ssh/pipedpeer-lab.pub" "$d/home/.ssh/pipedpeer-lab-test"
	mkdir -p "$d/home/.config/systemd/user"
	touch "$d/home/.config/systemd/user/pipedpeer.service"
	touch "$d/home/.config/systemd/user/pipedpeer-agents.slice"

	# User paths that MUST stay untouched
	mkdir -p "$d/home/coding/other" && echo "other code" >"$d/home/coding/other/code.txt"
	mkdir -p "$d/home/.cache/other" && echo "other cache" >"$d/home/.cache/other/cache.txt"
	touch "$d/home/.ssh/id_ed25519"
	mkdir -p "$d/home/.local/share/keep" && echo "keep share" >"$d/home/.local/share/keep/file.txt"

	# Fake repo
	echo "fake repo content" >"$d/fake_repo/main.txt"
	mkdir -p "$d/fake_repo/.git" "$d/fake_repo/scripts/campaign"
	touch "$d/fake_repo/scripts/campaign/wipe.sh"

	# Setup record
	mkdir -p "$d/home/.local/state/pipedpeer-lab-setup"
	cat >"$d/home/.local/state/pipedpeer-lab-setup/record" <<-EOF
		repo=$d/fake_repo
		nix_preexisted=no
	EOF

	# Valid keep directory with manifest
	echo "bundle A" >"$d/keep/fileA.txt"
	echo "bundle B" >"$d/keep/fileB.txt"
	if [[ -f "$WIPE" ]]; then
		cp "$WIPE" "$d/keep/wipe.sh"
	else
		touch "$d/keep/wipe.sh"
	fi
	chmod +x "$d/keep/wipe.sh"
	(cd "$d/keep" && sha256sum fileA.txt fileB.txt wipe.sh >MANIFEST.sha256)

	# Stubs
	cat >"$d/bin/pipedpeer" <<-EOF
		#!/usr/bin/env bash
		echo "pipedpeer \$*" >>"$d/logs/pipedpeer.log"
		exit 0
	EOF
	chmod +x "$d/bin/pipedpeer"

	cat >"$d/bin/systemctl" <<-EOF
		#!/usr/bin/env bash
		echo "systemctl \$*" >>"$d/logs/systemctl.log"
		exit 0
	EOF
	chmod +x "$d/bin/systemctl"

	cat >"$d/bin/podman" <<-EOF
		#!/usr/bin/env bash
		echo "podman \$*" >>"$d/logs/podman.log"
		exit 0
	EOF
	chmod +x "$d/bin/podman"

	cat >"$d/bin/docker" <<-EOF
		#!/usr/bin/env bash
		echo "docker \$*" >>"$d/logs/docker.log"
		exit 0
	EOF
	chmod +x "$d/bin/docker"

	cat >"$d/bin/sudo" <<-EOF
		#!/usr/bin/env bash
		echo "sudo \$*" >>"$d/logs/sudo.log"
		"\$@"
	EOF
	chmod +x "$d/bin/sudo"

	cat >"$d/nix/nix-installer" <<-EOF
		#!/usr/bin/env bash
		echo "nix-installer \$*" >>"$d/logs/nix.log"
		exit 0
	EOF
	chmod +x "$d/nix/nix-installer"
}

run_wipe() {
	local d="$1"
	shift
	env -u XDG_DATA_HOME -u XDG_STATE_HOME -u XDG_CACHE_HOME -u XDG_CONFIG_HOME \
		HOME="$d/home" PATH="$d/bin:$PATH" NIX_INSTALLER="$d/nix/nix-installer" \
		PIPEDPEER_USR_LOCAL_BIN="$d/fake_usr_local_pipedpeer" \
		bash "$WIPE" "$@"
}

# --- 1. no --keep: refuse, delete nothing -----------------------------------
(d=$(newdir)
setup_env "$d"
rc=0
run_wipe "$d" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "wipe exited 0 without --keep"
[[ -d "$d/home/.local/share/pipedpeer" ]] || pnote "deleted share dir"
[[ -f "$d/home/.ssh/pipedpeer-lab" ]] || pnote "deleted ssh key"
verdict "no --keep: refuse, delete nothing")

# --- 2. keep DIR missing, or no MANIFEST.sha256: refuse, delete nothing ----
(d=$(newdir)
setup_env "$d"
rc1=0; rc2=0
run_wipe "$d" --keep "$d/nonexistent" >"$d/out1" 2>"$d/err1" || rc1=$?
((rc1 != 0)) || pnote "wipe exited 0 with missing keep DIR"
mkdir -p "$d/no_manifest"
run_wipe "$d" --keep "$d/no_manifest" >"$d/out2" 2>"$d/err2" || rc2=$?
((rc2 != 0)) || pnote "wipe exited 0 with missing MANIFEST.sha256"
[[ -d "$d/home/.local/share/pipedpeer" ]] || pnote "deleted share dir"
verdict "keep DIR missing, or no MANIFEST.sha256: refuse, delete nothing")

# --- 3. a file in DIR changed since manifest: refuse, name file, delete nothing
(d=$(newdir)
setup_env "$d"
echo "tampered" >>"$d/keep/fileA.txt"
rc=0
run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "wipe exited 0 with tampered file in keep DIR"
grep -q "fileA.txt" "$d/err" || grep -q "fileA.txt" "$d/out" || pnote "did not name the changed file"
[[ -d "$d/home/.local/share/pipedpeer" ]] || pnote "deleted share dir"
verdict "a file in DIR changed since the manifest: refuse, name the file, delete nothing")

# --- 4. DIR inside recorded repo: refuse, delete nothing ---------------------
(d=$(newdir)
setup_env "$d"
mkdir -p "$d/fake_repo/keep"
echo "inside repo" >"$d/fake_repo/keep/f.txt"
(cd "$d/fake_repo/keep" && sha256sum f.txt >MANIFEST.sha256)
rc=0
run_wipe "$d" --keep "$d/fake_repo/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "wipe exited 0 with DIR inside recorded repo"
[[ -d "$d/home/.local/share/pipedpeer" ]] || pnote "deleted share dir"
[[ -d "$d/fake_repo" ]] || pnote "deleted repo"
verdict "DIR inside the recorded repo: refuse, delete nothing")

# --- 5. DIR has symlink to a path being deleted: refuse, delete nothing ------
(d=$(newdir)
setup_env "$d"
ln -s "$d/home/.cache/pipedpeer/cache" "$d/keep/badlink"
(cd "$d/keep" && sha256sum fileA.txt fileB.txt wipe.sh badlink >MANIFEST.sha256)
rc=0
run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "wipe exited 0 with symlink to path being deleted"
[[ -d "$d/home/.cache/pipedpeer" ]] || pnote "deleted cache dir"
verdict "DIR has symlink to a path being deleted: refuse, delete nothing")

# --- 6. valid DIR, --dry-run: list, delete nothing, call only read-only -----
(d=$(newdir)
setup_env "$d"
rc=0
run_wipe "$d" --keep "$d/keep" --dry-run >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "wipe --dry-run exited $rc (stderr: $(cat "$d/err"))"
[[ -d "$d/home/.local/share/pipedpeer" ]] || pnote "deleted share dir in dry-run"
[[ -f "$d/home/.ssh/pipedpeer-lab" ]] || pnote "deleted ssh key in dry-run"
[[ ! -f "$d/logs/pipedpeer.log" ]] || pnote "pipedpeer uninstall called in dry-run"
[[ ! -f "$d/logs/nix.log" ]] || pnote "nix uninstaller called in dry-run"
verdict "valid DIR, --dry-run: list; delete nothing; call nothing but read-only commands")

# --- 7. valid DIR, answer no: delete nothing, exit non-zero -----------------
(d=$(newdir)
setup_env "$d"
rc=0
printf 'no\n' | run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "wipe exited 0 when answered no"
[[ -d "$d/home/.local/share/pipedpeer" ]] || pnote "deleted share dir"
[[ -f "$d/home/.ssh/pipedpeer-lab" ]] || pnote "deleted ssh key"
verdict "valid DIR, answer no: delete nothing, exit non-zero")

# --- 8. valid DIR, DELETE EVERYTHING: listed items gone; DIR byte-identical -
(d=$(newdir)
setup_env "$d"
keep_hash_before="$(find "$d/keep" -type f -exec sha256sum {} + | sort)"
rc=0
printf 'DELETE EVERYTHING\n' | run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "wipe failed: $(cat "$d/err")"
# check listed items gone
[[ ! -d "$d/home/.local/share/pipedpeer" ]] || pnote "share dir still exists"
[[ ! -d "$d/home/.local/state/pipedpeer" ]] || pnote "state dir still exists"
[[ ! -d "$d/home/.local/state/pipedpeer-campaign-tests" ]] || pnote "tests dir still exists"
[[ ! -d "$d/home/.cache/pipedpeer" ]] || pnote "cache dir still exists"
[[ ! -d "$d/home/.cache/pipedpeer-lab-dev" ]] || pnote "lab-dev cache still exists"
[[ ! -f "$d/home/.ssh/pipedpeer-lab" ]] || pnote "lab ssh key still exists"
[[ ! -f "$d/home/.ssh/pipedpeer-lab.pub" ]] || pnote "lab pubkey still exists"
[[ ! -f "$d/home/.ssh/pipedpeer-lab-test" ]] || pnote "lab-test key still exists"
[[ ! -f "$d/home/.config/systemd/user/pipedpeer.service" ]] || pnote "service still exists"
[[ ! -f "$d/home/.config/systemd/user/pipedpeer-agents.slice" ]] || pnote "slice still exists"
[[ ! -d "$d/fake_repo" ]] || pnote "fake repo still exists"
[[ ! -f "$d/home/.local/state/pipedpeer-lab-setup/record" ]] || pnote "record still exists"
# check calls
grep -q "uninstall -y" "$d/logs/pipedpeer.log" 2>/dev/null || pnote "pipedpeer uninstall not called"
grep -q "systemctl" "$d/logs/systemctl.log" 2>/dev/null || pnote "systemctl not called"
# check keep dir byte-identical
keep_hash_after="$(find "$d/keep" -type f -exec sha256sum {} + | sort)"
[[ "$keep_hash_before" == "$keep_hash_after" ]] || pnote "keep dir changed after wipe"
verdict "valid DIR, DELETE EVERYTHING: listed items gone; DIR byte-identical to before")

# --- 9. same: ~/coding/other, ~/.cache/other, ~/.ssh/id_ed25519 untouched ----
(d=$(newdir)
setup_env "$d"
rc=0
printf 'DELETE EVERYTHING\n' | run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "wipe failed: $(cat "$d/err")"
[[ -f "$d/home/coding/other/code.txt" ]] || pnote "~/coding/other was touched"
[[ -f "$d/home/.cache/other/cache.txt" ]] || pnote "~/.cache/other was touched"
[[ -f "$d/home/.ssh/id_ed25519" ]] || pnote "~/.ssh/id_ed25519 was touched"
[[ -f "$d/home/.local/share/keep/file.txt" ]] || pnote "~/.local/share/keep was touched"
verdict "same: ~/coding/other, ~/.cache/other, ~/.ssh/id_ed25519, ~/.local/share/keep untouched")

# --- 10. record nix_preexisted=no: nix uninstaller called -------------------
(d=$(newdir)
setup_env "$d"
rc=0
printf 'DELETE EVERYTHING\n' | run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "wipe failed: $(cat "$d/err")"
grep -q "uninstall --no-confirm" "$d/logs/nix.log" 2>/dev/null || pnote "nix uninstaller not called"
verdict "record nix_preexisted=no: nix uninstaller called")

# --- 11. record nix_preexisted=yes: no nix call; says Nix was kept ---------
(d=$(newdir)
setup_env "$d"
sed -i 's/nix_preexisted=no/nix_preexisted=yes/' "$d/home/.local/state/pipedpeer-lab-setup/record"
rc=0
printf 'DELETE EVERYTHING\n' | run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "wipe failed: $(cat "$d/err")"
[[ ! -f "$d/logs/nix.log" ]] || pnote "nix uninstaller was called when nix_preexisted=yes"
grep -qi "Nix" "$d/out" || pnote "did not mention Nix was kept in output"
verdict "record nix_preexisted=yes: no nix call; says Nix was kept")

# --- 12. record missing: no nix call, repo not deleted, says why -----------
(d=$(newdir)
setup_env "$d"
rm -f "$d/home/.local/state/pipedpeer-lab-setup/record"
rc=0
printf 'DELETE EVERYTHING\n' | run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "wipe failed: $(cat "$d/err")"
[[ ! -f "$d/logs/nix.log" ]] || pnote "nix uninstaller called when record missing"
[[ -d "$d/fake_repo" ]] || pnote "repo was deleted despite missing record"
grep -qi "record" "$d/out" || grep -qi "record" "$d/err" || pnote "did not say why repo was kept"
verdict "record missing: no nix call, repo not deleted, says why")

# --- 13. a stub step fails: PROBLEM: shown, later steps ran, exit non-zero --
(d=$(newdir)
setup_env "$d"
# Make pipedpeer fail
cat >"$d/bin/pipedpeer" <<-EOF
	#!/usr/bin/env bash
	echo "pipedpeer failure" >>"$d/logs/pipedpeer.log"
	exit 1
EOF
chmod +x "$d/bin/pipedpeer"
rc=0
printf 'DELETE EVERYTHING\n' | run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "wipe exited 0 when a step failed"
grep -q "PROBLEM:" "$d/err" || grep -q "PROBLEM:" "$d/out" || pnote "PROBLEM: not shown on failure"
# Later steps must still run: state dir removed, repo removed
[[ ! -d "$d/home/.local/state/pipedpeer" ]] || pnote "later steps were skipped (state dir still exists)"
[[ ! -d "$d/fake_repo" ]] || pnote "later steps were skipped (fake repo still exists)"
verdict "a stub step fails: PROBLEM: shown, later steps ran, exit non-zero")

# --- 14. run from the copy inside DIR: same result, finishes ----------------
(d=$(newdir)
setup_env "$d"
# Update keep/wipe.sh with the latest WIPE script
if [[ -f "$WIPE" ]]; then
	cp "$WIPE" "$d/keep/wipe.sh"
else
	touch "$d/keep/wipe.sh"
fi
chmod +x "$d/keep/wipe.sh"
(cd "$d/keep" && sha256sum fileA.txt fileB.txt wipe.sh >MANIFEST.sha256)
rc=0
printf 'DELETE EVERYTHING\n' | (
	env -u XDG_DATA_HOME -u XDG_STATE_HOME -u XDG_CACHE_HOME -u XDG_CONFIG_HOME \
		HOME="$d/home" PATH="$d/bin:$PATH" NIX_INSTALLER="$d/nix/nix-installer" \
		PIPEDPEER_USR_LOCAL_BIN="$d/fake_usr_local_pipedpeer" \
		bash "$d/keep/wipe.sh" --keep "$d/keep"
) >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "wipe from inside keep DIR failed: $(cat "$d/err")"
[[ ! -d "$d/fake_repo" ]] || pnote "fake repo still exists"
[[ ! -d "$d/home/.local/share/pipedpeer" ]] || pnote "share dir still exists"
verdict "run from the copy inside DIR: same result, finishes")

set_record_repo() {
	sed -i "s|^repo=.*|repo=$2|" "$1/home/.local/state/pipedpeer-lab-setup/record"
}

# run_wipe without --keep, for the default bundle location.
default_bundle() {
	local d="$1"
	mkdir -p "$d/home/Documents/pipedpeer"
	mv "$d/keep" "$d/home/Documents/pipedpeer/lab-bundle"
	mkdir -p "$d/keep"
}

# --- 15. no --keep, no bundle at the default: refuse, say to run collect.sh --
(d=$(newdir)
setup_env "$d"
rc=0
run_wipe "$d" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "wipe exited 0 with no bundle at the default"
grep -q "collect.sh" "$d/err" || pnote "refusal does not say to run collect.sh: $(cat "$d/err")"
[[ -d "$d/home/.local/share/pipedpeer" ]] || pnote "deleted share dir"
verdict "no --keep, no bundle at the default: refuse, say to run collect.sh")

# --- 16. no --keep, valid bundle at the default: runs -----------------------
(d=$(newdir)
setup_env "$d"
default_bundle "$d"
rc=0
printf 'DELETE EVERYTHING\n' | run_wipe "$d" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "wipe failed: $(cat "$d/err")"
[[ ! -d "$d/home/.local/share/pipedpeer" ]] || pnote "share dir still exists"
[[ -f "$d/home/Documents/pipedpeer/lab-bundle/fileA.txt" ]] || pnote "bundle was touched"
verdict "no --keep, valid bundle at the default: runs, bundle kept")

# --- 17. other files beside the bundle stay ---------------------------------
(d=$(newdir)
setup_env "$d"
default_bundle "$d"
mkdir -p "$d/home/Documents/pipedpeer/notes"
echo mine >"$d/home/Documents/pipedpeer/notes/n.txt"
echo mine >"$d/home/Documents/pipedpeer/top.txt"
rc=0
printf 'DELETE EVERYTHING\n' | run_wipe "$d" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "wipe failed: $(cat "$d/err")"
[[ -f "$d/home/Documents/pipedpeer/notes/n.txt" ]] || pnote "notes/n.txt gone"
[[ -f "$d/home/Documents/pipedpeer/top.txt" ]] || pnote "top.txt gone"
verdict "other files in ~/Documents/pipedpeer untouched")

# --- 18. record repo=$HOME: not deleted, PROBLEM, says so in the listing -----
(d=$(newdir)
setup_env "$d"
set_record_repo "$d" "$d/home"
rc=0
run_wipe "$d" --keep "$d/keep" --dry-run >"$d/dry" 2>&1 || rc=$?
grep -q "will not be deleted" "$d/dry" || pnote "dry run does not say the repo stays: $(cat "$d/dry")"
rc=0
printf 'DELETE EVERYTHING\n' | run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "wipe exited 0 with repo=HOME"
[[ -f "$d/home/coding/other/code.txt" ]] || pnote "HOME was deleted"
grep -q "PROBLEM:.*repo" "$d/err" || pnote "no PROBLEM about the repo: $(cat "$d/err")"
verdict "record repo=HOME: not deleted, PROBLEM, listing says it stays")

# --- 19. record repo=/ : never handed to rm ---------------------------------
(d=$(newdir)
setup_env "$d"
set_record_repo "$d" /
real_rm="$(command -v rm)"
cat >"$d/bin/rm" <<-EOF
	#!/usr/bin/env bash
	if [[ "\${!#}" == / ]]; then
		echo "rm of /" >>"$d/logs/rm.log"
		exit 1
	fi
	exec "$real_rm" "\$@"
EOF
chmod +x "$d/bin/rm"
rc=0
printf 'DELETE EVERYTHING\n' | run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "wipe exited 0 with repo=/"
[[ ! -f "$d/logs/rm.log" ]] || pnote "rm was asked to delete /"
grep -q "PROBLEM:.*repo" "$d/err" || pnote "no PROBLEM about the repo: $(cat "$d/err")"
verdict "record repo=/: never handed to rm, PROBLEM")

# --- 20. record repo is a folder that is not the checkout: not deleted -------
(d=$(newdir)
setup_env "$d"
mkdir -p "$d/home/photos"
echo photo >"$d/home/photos/a.jpg"
set_record_repo "$d" "$d/home/photos"
rc=0
printf 'DELETE EVERYTHING\n' | run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "wipe exited 0 with a non-repo record"
[[ -f "$d/home/photos/a.jpg" ]] || pnote "non-repo folder was deleted"
grep -q "PROBLEM:.*repo" "$d/err" || pnote "no PROBLEM about the repo: $(cat "$d/err")"
verdict "record repo is not the checkout: not deleted, PROBLEM")

# --- 21. record repo is the checkout: deleted, no PROBLEM -------------------
(d=$(newdir)
setup_env "$d"
rc=0
printf 'DELETE EVERYTHING\n' | run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "wipe failed: $(cat "$d/err")"
[[ ! -d "$d/fake_repo" ]] || pnote "checkout still exists"
! grep -q "PROBLEM:" "$d/err" || pnote "unexpected PROBLEM: $(cat "$d/err")"
verdict "record repo is the checkout: deleted, no PROBLEM")

# --- 22. nix_preexisted=no, Nix never installed: no PROBLEM ------------------
(d=$(newdir)
setup_env "$d"
rm -rf "$d/nix"
rc=0
printf 'DELETE EVERYTHING\n' | run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "wipe failed: $(cat "$d/err")"
! grep -q "PROBLEM:" "$d/err" || pnote "false PROBLEM: $(cat "$d/err")"
[[ ! -f "$d/logs/nix.log" ]] || pnote "nix uninstaller called"
verdict "nix_preexisted=no and Nix never installed: no PROBLEM, no call")

# --- 23. nix_preexisted=no, Nix there without its installer: PROBLEM ---------
(d=$(newdir)
setup_env "$d"
rm -f "$d/nix/nix-installer"
rc=0
printf 'DELETE EVERYTHING\n' | run_wipe "$d" --keep "$d/keep" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "wipe exited 0 with Nix left behind"
grep -q "PROBLEM:.*nix" "$d/err" || pnote "no PROBLEM about nix: $(cat "$d/err")"
verdict "nix_preexisted=no, Nix there without its installer: PROBLEM")

# --- 24. DIR inside a path being deleted: refuse, delete nothing -------------
(d=$(newdir)
setup_env "$d"
mv "$d/keep" "$d/home/.cache/pipedpeer/bundle"
rc=0
run_wipe "$d" --keep "$d/home/.cache/pipedpeer/bundle" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "wipe accepted a DIR inside a deleted path"
[[ -f "$d/home/.ssh/pipedpeer-lab" ]] || pnote "deleted ssh key"
grep -q "being deleted" "$d/err" || pnote "does not say why: $(cat "$d/err")"
verdict "DIR inside a path being deleted: refuse")

# --- 25. DIR whose name only starts like a deleted path: accepted ------------
(d=$(newdir)
setup_env "$d"
mv "$d/keep" "$d/home/.cache/pipedpeer-bundle"
rc=0
run_wipe "$d" --keep "$d/home/.cache/pipedpeer-bundle" --dry-run >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "near-miss name refused: $(cat "$d/err")"
verdict "DIR name only starts like a deleted path: accepted")

nrows=$(wc -l <"$RESULTS")
nfail=$(grep -c '^bad$' "$RESULTS" || true)
if ((nfail == 0 && nrows > 0)); then
	printf 'PASS %d/%d\n' "$nrows" "$nrows"
	exit 0
fi
printf 'FAIL %d of %d rows\n' "$nfail" "$nrows"
exit 1
