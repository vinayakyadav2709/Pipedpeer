#!/usr/bin/env bash
# run_all.sh: every recipe line of `make campaign-test-files`, a few at once, without
# stopping at the first failing file. One result line per file. A file that
# fails is run again alone, so a timing row that only lost out to the load of
# the parallel run is reported as such and does not count as a failure.
#
# Each file runs under its own HOME, with no XDG dirs and no session bus, so it
# cannot reach this machine's pipedpeer state or another file's. A canary in
# that HOME's state folder fails the file that deletes it, and a guard
# systemctl fails any file that reaches for the real service manager.
# Usage: run_all.sh    (CAMPAIGN_TEST_JOBS sets how many at once, default 3;
#        RUN_ALL_RECIPES replaces the list, for run_all_test.sh)
set -uo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
cd "$REPO" || exit 1
# Beside pipedpeer's own state folder, never inside it.
out="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer-campaign-tests"
rm -rf "$out"
mkdir -p "$out/guard"
real_cache="${XDG_CACHE_HOME:-$HOME/.cache}"
# Python packages live in the real user site; only HOME moves.
PYTHONUSERBASE="$(python3 -m site --user-base)" || exit 1
cat >"$out/guard/systemctl" <<GUARD
#!/usr/bin/env bash
printf 'systemctl %s\n' "\$*" >>"$out/guard/reached"
echo "run_all guard: a test reached the real systemctl: \$*" >&2
exit 1
GUARD
chmod +x "$out/guard/systemctl"

# run_one <recipe line>: runs it, writes <file>.log and <file>.rc. Stdin is
# closed because some tests read it, which would eat the list of lines.
run_one() {
	local name home canary rc
	name="$(printf '%s' "$1" | sed -E 's#.*/test/##; s/ .*//')"
	home="$out/home/$name"
	canary="$home/.local/state/pipedpeer/canary"
	rm -rf "$home"
	mkdir -p "$(dirname "$canary")"
	: >"$canary"
	# ports_test.sh's dev venv is too slow to build per run; the link is removed
	# with the home, never followed.
	mkdir -p "$home/.cache"
	ln -s "$real_cache/pipedpeer-lab-dev" "$home/.cache/pipedpeer-lab-dev"
	env -u XDG_STATE_HOME -u XDG_DATA_HOME -u XDG_CACHE_HOME -u XDG_CONFIG_HOME \
		-u XDG_RUNTIME_DIR -u DBUS_SESSION_BUS_ADDRESS \
		HOME="$home" PATH="$out/guard:$PATH" \
		timeout 900 bash -c "$1" >"$out/$name.log" 2>&1 </dev/null
	rc=$?
	if [[ ! -e "$canary" ]]; then
		echo "run_all: $name deleted its own HOME's pipedpeer state folder" >>"$out/$name.log"
		[[ $rc == 0 ]] && rc=98
	fi
	printf '%s\n' "$rc" >"$out/$name.rc"
	printf '%s\t%s\n' "$name" "$1" >>"$out/lines.tsv"
	printf '%s rc=%s %s\n' "$name" "$rc" \
		"$(grep -E 'PASS [0-9]+/[0-9]+|FAIL [0-9]+ of|PORTS rows|TOTAL:|[0-9]+ passed|[0-9]+ failed|^run_all:' "$out/$name.log" | tail -1 | cut -c1-70)"
}
export -f run_one
export out PYTHONUSERBASE real_cache

if [[ -n "${RUN_ALL_RECIPES+set}" ]]; then
	mapfile -t recipes < <(printf '%s\n' "$RUN_ALL_RECIPES" | grep -E '^(bash|python3) ')
else
	mapfile -t recipes < <(make --no-print-directory -n campaign-test-files | grep -E '^(bash|python3) ')
fi
if ((${#recipes[@]} == 0)); then
	echo "run_all: no test files listed" >&2
	exit 1
fi

# shellcheck disable=SC2016  # $1 belongs to the inner bash.
printf '%s\n' "${recipes[@]}" |
	xargs -d '\n' -P "${CAMPAIGN_TEST_JOBS:-3}" -I{} bash -c 'run_one "$1"' _ {}

ran="$(sort -u "$out/lines.tsv" 2>/dev/null | wc -l)"
if ((ran != ${#recipes[@]})); then
	echo "run_all: ${#recipes[@]} test files listed but $ran reported a result; see $out" >&2
	exit 1
fi

failed=0
while IFS=$'\t' read -r name line; do
	[[ "$(cat "$out/$name.rc")" == 0 ]] && continue
	echo "rerunning alone: $name"
	cp "$out/$name.log" "$out/$name.parallel.log"
	run_one "$line"
	if [[ "$(cat "$out/$name.rc")" == 0 ]]; then
		echo "LOAD-ONLY $name failed in the parallel run and passed alone; see $out/$name.parallel.log"
	else
		echo "FAILED $name; see $out/$name.log"
		failed=$((failed + 1))
	fi
done < <(sort -u "$out/lines.tsv")

if [[ -e "$out/guard/reached" ]]; then
	echo "FAILED a test reached the real systemctl; see $out/guard/reached"
	failed=$((failed + 1))
fi
# A skip is not a failure, but "all passed" must not hide one.
skips="$(grep -hE '^SKIP' "$out"/*.log 2>/dev/null | sort -u)"
[[ -n "$skips" ]] && printf 'SKIPPED rows:\n%s\n' "$skips" | cut -c1-120
# The homes hold only what the tests left behind; kept on failure to look at.
((failed == 0)) && rm -rf "${out:?}/home"
echo "campaign tests: $ran files, $failed failing; logs in $out"
((failed == 0))
