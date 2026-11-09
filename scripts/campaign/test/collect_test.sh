#!/usr/bin/env bash
# shellcheck disable=SC2015,SC2030,SC2031,SC2088,SC2317
# Case table tests for collect.sh and setup-driver.sh record creation.
set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CAMP="$REPO/scripts/campaign"
COLLECT="$CAMP/collect.sh"
SETUP_DRIVER="$CAMP/setup-driver.sh"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer-campaign-tests"
mkdir -p "$STATE"

RESULTS="$(mktemp)"
FEDORA_OS="$(mktemp)"
echo 'ID=fedora' >"$FEDORA_OS"
problems=()
ROOT="$(mktemp -d "$STATE/collect-test.XXXXXX")"

cleanup() {
	if [[ "${LAB_TEST_KEEP:-0}" != 1 ]]; then
		rm -rf "$ROOT"
	fi
	rm -f "$RESULTS" "$FEDORA_OS"
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

setup_lab_dir() {
	local d="$1"
	mkdir -p "$d/lab/analysis-input" "$d/lab/figures"
	cat >"$d/lab/runs.jsonl" <<-EOF
		{"experiment": "E4", "workload": "W1", "system": "pipedpeer", "n": 2, "wall_s": 10.5, "result_ok": 1}
	EOF
	cat >"$d/lab/analysis-input/e4.csv" <<-EOF
		system,repeat,wall_s,result_ok,share_pc00
		pipedpeer,1,10,1,0.5
	EOF
	echo "fake png" >"$d/lab/figures/fig1.png"
}

# --- 1. no runs.jsonl under results: refuse, create no folder ---------------
(d=$(newdir)
empty_lab="$d/empty_lab"
mkdir -p "$empty_lab"
bundle_target="$d/bundle1"
rc=0
LAB_RESULTS="$empty_lab" bash "$COLLECT" --to "$bundle_target" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "collect.sh exited 0 without runs.jsonl"
[[ ! -d "$bundle_target" ]] || pnote "created bundle folder despite missing runs.jsonl"
verdict "no runs.jsonl under results: refuse, create no folder")

# --- 2. normal lab dir: every listed item present; manifest verifies with sha256sum -c
(d=$(newdir)
setup_lab_dir "$d"
bundle_target="$d/bundle2"
rc=0
out="$(LAB_RESULTS="$d/lab" bash "$COLLECT" --to "$bundle_target" 2>"$d/err")" || rc=$?
((rc == 0)) || pnote "collect.sh failed: $(cat "$d/err")"

# Every listed item:
# - all of results/lab
[[ -f "$bundle_target/results/lab/runs.jsonl" ]] || pnote "missing results/lab/runs.jsonl"
[[ -f "$bundle_target/results/lab/analysis-input/e4.csv" ]] || pnote "missing results/lab/analysis-input/e4.csv"
# - output of analyse.py as analysis.txt
[[ -f "$bundle_target/analysis.txt" ]] || pnote "missing analysis.txt"
# - with its figures
[[ -d "$bundle_target/figures" ]] || pnote "missing figures/ folder"
# - docs/paper
[[ -d "$bundle_target/docs/paper" ]] || pnote "missing docs/paper"
# - docs/report
[[ -d "$bundle_target/docs/report" ]] || pnote "missing docs/report"
# - docs/benchmarks/campaign-cases.md
[[ -f "$bundle_target/docs/benchmarks/campaign-cases.md" ]] || pnote "missing campaign-cases.md"
# - scripts/campaign/experiments.tsv
[[ -f "$bundle_target/scripts/campaign/experiments.tsv" ]] || pnote "missing experiments.tsv"
# - PAPER-UPDATE.md
[[ -f "$bundle_target/PAPER-UPDATE.md" ]] || pnote "missing PAPER-UPDATE.md"
# - copy of wipe.sh
[[ -f "$bundle_target/wipe.sh" ]] || pnote "missing wipe.sh"
# - source.txt (commit id, git status --short)
[[ -f "$bundle_target/source.txt" ]] || pnote "missing source.txt"
# - MANIFEST.sha256 over every file
[[ -f "$bundle_target/MANIFEST.sha256" ]] || pnote "missing MANIFEST.sha256"

# Verify manifest with sha256sum -c
if [[ -f "$bundle_target/MANIFEST.sha256" ]]; then
	(cd "$bundle_target" && sha256sum --quiet -c MANIFEST.sha256) || pnote "MANIFEST.sha256 failed sha256sum -c"
fi

# Prints the folder and 'upload this whole folder to Google Drive'
grep -qi "upload this whole folder to Google Drive" <<<"$out" || pnote "did not print upload instruction: $out"
grep -q "$bundle_target" <<<"$out" || pnote "did not print bundle folder path in output: $out"

verdict "normal lab dir: every listed item present; manifest verifies with sha256sum -c")

# --- 3. DIR exists and is non-empty: refuse, change nothing -----------------
(d=$(newdir)
setup_lab_dir "$d"
bundle_target="$d/nonempty"
mkdir -p "$bundle_target"
echo "already here" >"$bundle_target/preexisting.txt"
rc=0
LAB_RESULTS="$d/lab" bash "$COLLECT" --to "$bundle_target" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "collect.sh exited 0 when target exists and is non-empty"
[[ -f "$bundle_target/preexisting.txt" ]] || pnote "preexisting file was removed"
[[ ! -f "$bundle_target/analysis.txt" ]] || pnote "wrote files into non-empty DIR"
verdict "DIR exists and is non-empty: refuse, change nothing")

# --- 4. analyse.py fails: results still copied; says ANALYSIS-FAILED with reason; exit non-zero
(d=$(newdir)
setup_lab_dir "$d"
bundle_target="$d/bundle4"
mkdir -p "$d/bin"
cat >"$d/bin/python3" <<-EOF
	#!/usr/bin/env bash
	if [[ "\$*" == *"analyse.py"* ]]; then
		echo "mock analysis error: syntax error or bad data" >&2
		exit 42
	fi
	exec /usr/bin/python3 "\$@"
EOF
chmod +x "$d/bin/python3"

rc=0
out="$(PATH="$d/bin:$PATH" LAB_RESULTS="$d/lab" bash "$COLLECT" --to "$bundle_target" 2>"$d/err")" || rc=$?
((rc != 0)) || pnote "collect.sh exited 0 when analyse.py failed"
# results still copied
[[ -f "$bundle_target/results/lab/runs.jsonl" ]] || pnote "results/lab not copied when analyse.py failed"
# says ANALYSIS-FAILED with reason
full_output="$out $(cat "$d/err")"
grep -q "ANALYSIS-FAILED" <<<"$full_output" || pnote "did not say ANALYSIS-FAILED (got: $full_output)"
grep -q "mock analysis error" <<<"$full_output" || pnote "did not include failure reason (got: $full_output)"
verdict "analyse.py fails: results still copied; says ANALYSIS-FAILED with the reason; exit non-zero")

# --- 5. --to path with spaces: works ----------------------------------------
(d=$(newdir)
setup_lab_dir "$d"
bundle_target="$d/bundle with spaces"
rc=0
LAB_RESULTS="$d/lab" bash "$COLLECT" --to "$bundle_target" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "collect.sh failed with spaces in path: $(cat "$d/err")"
[[ -f "$bundle_target/results/lab/runs.jsonl" ]] || pnote "missing results in space path"
[[ -f "$bundle_target/MANIFEST.sha256" ]] || pnote "missing manifest in space path"
if [[ -f "$bundle_target/MANIFEST.sha256" ]]; then
	(cd "$bundle_target" && sha256sum --quiet -c MANIFEST.sha256) || pnote "manifest failed sha256sum -c in space path"
fi
verdict "--to path with spaces: works")

# --- 6. setup-driver.sh record: no /nix, first run -> nix_preexisted=no -----
(d=$(newdir)
mkdir -p "$d/home"
rc=0
env -u XDG_STATE_HOME -u XDG_DATA_HOME -u XDG_CACHE_HOME \
	HOME="$d/home" PIPEDPEER_OS_RELEASE="$FEDORA_OS" PIPEDPEER_SETUP_RECORD_ONLY=1 PIPEDPEER_TEST_NIX_DIR="$d/nonexistent_nix" \
	bash "$SETUP_DRIVER" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "setup-driver.sh exited $rc: $(cat "$d/err")"
rec="$d/home/.local/state/pipedpeer-lab-setup/record"
[[ -f "$rec" ]] || pnote "record file not created"
grep -q "^nix_preexisted=no$" "$rec" 2>/dev/null || pnote "record does not have nix_preexisted=no (got: $(cat "$rec" 2>/dev/null))"
grep -q "^repo=" "$rec" 2>/dev/null || pnote "record missing repo= line"
verdict "setup-driver.sh: no /nix, first run: nix_preexisted=no")

# --- 7. setup-driver.sh record: /nix present, first run -> nix_preexisted=yes
(d=$(newdir)
mkdir -p "$d/home" "$d/fake_nix"
rc=0
env -u XDG_STATE_HOME -u XDG_DATA_HOME -u XDG_CACHE_HOME \
	HOME="$d/home" PIPEDPEER_OS_RELEASE="$FEDORA_OS" PIPEDPEER_SETUP_RECORD_ONLY=1 PIPEDPEER_TEST_NIX_DIR="$d/fake_nix" \
	bash "$SETUP_DRIVER" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "setup-driver.sh exited $rc: $(cat "$d/err")"
rec="$d/home/.local/state/pipedpeer-lab-setup/record"
[[ -f "$rec" ]] || pnote "record file not created"
grep -q "^nix_preexisted=yes$" "$rec" 2>/dev/null || pnote "record does not have nix_preexisted=yes (got: $(cat "$rec" 2>/dev/null))"
verdict "setup-driver.sh: /nix present, first run: nix_preexisted=yes")

# --- 8. setup-driver.sh record: rerun after Nix was added -> record unchanged
(d=$(newdir)
mkdir -p "$d/home"
rec_dir="$d/home/.local/state/pipedpeer-lab-setup"
mkdir -p "$rec_dir" "$d/fake_nix"
cat >"$rec_dir/record" <<-EOF
	repo=$REPO
	nix_preexisted=no
EOF
initial_content="$(cat "$rec_dir/record")"
rc=0
env -u XDG_STATE_HOME -u XDG_DATA_HOME -u XDG_CACHE_HOME \
	HOME="$d/home" PIPEDPEER_OS_RELEASE="$FEDORA_OS" PIPEDPEER_SETUP_RECORD_ONLY=1 PIPEDPEER_TEST_NIX_DIR="$d/fake_nix" \
	bash "$SETUP_DRIVER" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "setup-driver.sh exited $rc on rerun: $(cat "$d/err")"
new_content="$(cat "$rec_dir/record")"
[[ "$initial_content" == "$new_content" ]] || pnote "record was modified on rerun (was '$initial_content', now '$new_content')"
verdict "setup-driver.sh: rerun after Nix was added: record unchanged")

stub_analyse_fail() {
	mkdir -p "$1/bin"
	cat >"$1/bin/python3" <<-EOF
		#!/usr/bin/env bash
		if [[ "\$*" == *"analyse.py"* ]]; then
			echo "mock analysis error" >&2
			exit 42
		fi
		exec /usr/bin/python3 "\$@"
	EOF
	chmod +x "$1/bin/python3"
}

# --- 9. no --to: bundle goes to ~/Documents/pipedpeer/lab-bundle -------------
(d=$(newdir)
setup_lab_dir "$d"
mkdir -p "$d/home"
rc=0
out="$(HOME="$d/home" LAB_RESULTS="$d/lab" bash "$COLLECT" 2>"$d/err")" || rc=$?
((rc == 0)) || pnote "collect.sh failed: $(cat "$d/err")"
[[ -f "$d/home/Documents/pipedpeer/lab-bundle/MANIFEST.sha256" ]] || pnote "no bundle at the default ($out)"
[[ ! -e "$d/home/pipedpeer-lab-bundle" ]] || pnote "used the old default"
verdict "no --to: bundle at ~/Documents/pipedpeer/lab-bundle")

# --- 10. other files in ~/Documents/pipedpeer stay ---------------------------
(d=$(newdir)
setup_lab_dir "$d"
mkdir -p "$d/home/Documents/pipedpeer/notes"
echo mine >"$d/home/Documents/pipedpeer/notes/n.txt"
rc=0
HOME="$d/home" LAB_RESULTS="$d/lab" bash "$COLLECT" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "collect.sh failed: $(cat "$d/err")"
[[ -f "$d/home/Documents/pipedpeer/lab-bundle/MANIFEST.sha256" ]] || pnote "no bundle beside other files"
[[ -f "$d/home/Documents/pipedpeer/notes/n.txt" ]] || pnote "other file gone"
verdict "other files in ~/Documents/pipedpeer untouched")

# --- 11. analysis fails: no manifest, tells how to redo it, wipe refuses -----
(d=$(newdir)
setup_lab_dir "$d"
stub_analyse_fail "$d"
b="$d/bundle11"
rc=0
PATH="$d/bin:$PATH" LAB_RESULTS="$d/lab" bash "$COLLECT" --to "$b" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "exited 0"
[[ ! -f "$b/MANIFEST.sha256" ]] || pnote "manifest written for an incomplete bundle"
grep -q "wipe.sh will refuse" "$d/err" || pnote "does not say wipe refuses: $(cat "$d/err")"
grep -qF "delete $b" "$d/err" || pnote "does not say to delete the folder and rerun: $(cat "$d/err")"
rc=0
HOME="$d" bash "$CAMP/wipe.sh" --keep "$b" --dry-run >"$d/w" 2>&1 || rc=$?
((rc != 0)) || pnote "wipe accepted the incomplete bundle"
verdict "analysis fails: no manifest, says how to redo it, wipe refuses")

# --- 12. analysis works: figures reach the bundle ----------------------------
(d=$(newdir)
setup_lab_dir "$d"
b="$d/bundle12"
rc=0
LAB_RESULTS="$d/lab" bash "$COLLECT" --to "$b" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "collect.sh failed: $(cat "$d/err")"
[[ -f "$b/figures/fig1.png" ]] || pnote "figure not in the bundle"
verdict "analysis works: figures reach the bundle")

# --- 13. figures cannot be copied: stop visibly, no manifest -----------------
(d=$(newdir)
setup_lab_dir "$d"
b="$d/bundle13"
mkdir -p "$d/bin"
real_cp="$(command -v cp)"
cat >"$d/bin/cp" <<-EOF
	#!/usr/bin/env bash
	if [[ "\$*" == *"/figures"* ]]; then
		echo "stub cp refused" >&2
		exit 3
	fi
	exec "$real_cp" "\$@"
EOF
chmod +x "$d/bin/cp"
rc=0
PATH="$d/bin:$PATH" LAB_RESULTS="$d/lab" bash "$COLLECT" --to "$b" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "exited 0 when figures could not be copied"
[[ ! -f "$b/MANIFEST.sha256" ]] || pnote "manifest written without figures"
grep -q "stub cp refused" "$d/err" || pnote "cp error hidden: $(cat "$d/err")"
verdict "figures cannot be copied: stops visibly, no manifest")

# --- 14. setup-driver.sh outside a git checkout: refuses, no record ----------
(d=$(newdir)
mkdir -p "$d/home" "$d/notrepo"
cp "$SETUP_DRIVER" "$d/notrepo/setup-driver.sh"
rc=0
(cd "$d/notrepo" && env -u XDG_STATE_HOME HOME="$d/home" PIPEDPEER_OS_RELEASE="$FEDORA_OS" \
	PIPEDPEER_SETUP_RECORD_ONLY=1 bash ./setup-driver.sh) >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "exited 0 outside a checkout"
[[ ! -f "$d/home/.local/state/pipedpeer-lab-setup/record" ]] || pnote "recorded a non-repo: $(cat "$d/home/.local/state/pipedpeer-lab-setup/record")"
verdict "setup-driver.sh outside a checkout: refuses, records nothing")

nrows=$(wc -l <"$RESULTS")
nfail=$(grep -c '^bad$' "$RESULTS" || true)
if ((nfail == 0 && nrows > 0)); then
	printf 'PASS %d/%d\n' "$nrows" "$nrows"
	exit 0
fi
printf 'FAIL %d of %d rows\n' "$nfail" "$nrows"
exit 1
