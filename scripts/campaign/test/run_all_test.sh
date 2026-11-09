#!/usr/bin/env bash
# shellcheck disable=SC2016  # the test file bodies are single-quoted on purpose
# run_all_test.sh: the isolation guards of run_all.sh, on throwaway test files.
set -uo pipefail
REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
root="${HOME}/.local/state"
mkdir -p "$root"
t="$(mktemp -d "$root/run-all-test.XXXXXX")" || exit 1
trap 'rm -rf "$t"' EXIT
pass=0 total=0

# row <name> <want rc> <want grep> <test file body>
row() {
	local name="$1" want="$2" pat="$3" body="$4" h="$t/h$total" rc log ok=1
	mkdir -p "$h/.local/state/pipedpeer" "$t/test"
	: >"$h/.local/state/pipedpeer/keep"
	printf '%s\n' "$body" >"$t/test/case$total.sh"
	log="$(env -u XDG_STATE_HOME HOME="$h" RUN_ALL_RECIPES="bash $t/test/case$total.sh" \
		bash "$REPO/scripts/campaign/test/run_all.sh" 2>&1)"
	rc=$?
	[[ $rc == "$want" ]] || ok=0
	grep -q "$pat" <<<"$log" || ok=0
	# Whatever the file did, the outer HOME's state folder must be untouched.
	[[ -e "$h/.local/state/pipedpeer/keep" ]] || ok=0
	total=$((total + 1))
	if ((ok)); then pass=$((pass + 1)); echo "ok $name"; else echo "FAIL $name: rc=$rc $log"; fi
}

row "deletes its state folder -> fails, named, outer state kept" 1 'FAILED case0.sh' \
	'rm -rf "$HOME/.local/state/pipedpeer"'
row "leaves its state folder -> passes" 0 '1 files, 0 failing' \
	'mkdir -p "$HOME/.local/state/pipedpeer/x"'
row "reaches systemctl -> fails" 1 'reached the real systemctl' \
	'systemctl --user stop some-unit || true'
row "does not reach systemctl -> passes" 0 '0 failing' 'true'
row "fails alone too -> FAILED" 1 'FAILED case4.sh' 'exit 3'

# An empty list is an error, never "0 failing".
log="$(env -u XDG_STATE_HOME HOME="$t/h9" RUN_ALL_RECIPES='' bash "$REPO/scripts/campaign/test/run_all.sh" 2>&1)"
rc=$?
total=$((total + 1))
if [[ $rc != 0 && $log == *"no test files"* ]]; then pass=$((pass + 1)); echo "ok empty list -> error"; else echo "FAIL empty list: rc=$rc $log"; fi

echo "PASS $pass/$total"
((pass == total))
