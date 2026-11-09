#!/usr/bin/env bash
# shellcheck disable=SC2030,SC2031,SC2317
# Case table for `collect.sh --bug`: one folder with everything needed to debug
# a failed campaign run, and no secrets. The PCs are the fake lab
# (test/fakelab.sh); their pipedpeer is a stub that prints an invite code.
#
#   B1 a run that died before any result (no runs.jsonl): the bundle is made
#      (ssh here reads its stdin as a real one does; sizes.env is kept)
#      anyway with the driver's logs, versions, and one report per PC (version,
#      status, journals, warm-up log, Ray logs, memory evidence); analyse.py's
#      outcome is recorded, not fatal; prints BUG-BUNDLE <dir>; exit 0
#   B2 secrets: invite codes (driver logs and PC output) are redacted; a PEM
#      private key, r2.env and cluster.json in the lab dir are left out and
#      named in SUMMARY.txt; no secret string anywhere in the bundle
#   B3 a PC that does not answer: its report says so, the others are there,
#      SUMMARY.txt lists it, exit 0
#   B4 ordinary use unchanged: without --bug and without runs.jsonl collect.sh
#      still refuses (row 1 of collect_test.sh) and makes no folder
set -euo pipefail
REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
COLLECT="$REPO/scripts/campaign/collect.sh"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer-campaign-tests"
mkdir -p "$STATE"
ROOT="$(mktemp -d "$STATE/collect-bug-test.XXXXXX")"
RESULTS="$ROOT/results"
: >"$RESULTS"
trap '[[ "${LAB_TEST_KEEP:-0}" == 1 ]] || rm -rf "$ROOT"' EXIT
problems=()
pnote() { problems+=("$*"); }
verdict() {
	if ((${#problems[@]} == 0)); then
		echo ok >>"$RESULTS"
		printf 'ok   %s\n' "$1"
	else
		echo bad >>"$RESULTS"
		printf 'FAIL %s -- %s\n' "$1" "${problems[*]}"
	fi
	problems=()
}
# shellcheck source=/dev/null
source "$REPO/scripts/campaign/test/fakelab.sh"

# lab <dir>: a fake lab of 2 PCs whose pipedpeer stub prints an invite code
lab() {
	local d="$1" pc
	fakelab_init "$d/lab" 2 >/dev/null || exit 90
	cat >"$d/lab/bin/pipedpeer" <<'EOF'
#!/usr/bin/env bash
case "${1:-}" in
	--version | version) echo "pipedpeer version=0.0.1-test" ;;
	status) printf 'NAME STATE\npc00.lab healthy\n\ninvite: pipedpeer connect PCSECRET9abc\n' ;;
	*) echo "stub: $*" ;;
esac
EOF
	chmod +x "$d/lab/bin/pipedpeer"
	for pc in pc00 pc01; do
		mkdir -p "$d/lab/$pc/.local/state/pipedpeer"
		printf '2026-11-09T00:00:00Z building build=test torch=CPU\nfailed (exit 1); WARMUP-MARK-%s\n' "$pc" >"$d/lab/$pc/.local/state/pipedpeer/warmup.log"
	done
	mkdir -p "$d/ray/logs"
	echo "RAYLET-MARK memory pressure" >"$d/ray/logs/raylet.out"
	echo "$(date -Is) e17.sh: E17: connect on pc03 with pipedpeer connect DRVSECRET77xyz failed" >>"$d/lab/campaign.log"
	# a real ssh reads its stdin; the fake one does not, which hid a loop over
	# hosts.tsv losing every PC after the first (found on the container lab)
	mv "$d/lab/bin/ssh" "$d/lab/bin/ssh.fake"
	printf '#!/usr/bin/env bash\ncat >/dev/null\nexec "%s" "$@"\n' "$d/lab/bin/ssh.fake" >"$d/lab/bin/ssh"
	chmod +x "$d/lab/bin/ssh"
	printf 'W1_ITEMS=48\n' >"$d/lab/sizes.env"
	export LAB_RESULTS="$d/lab" LAB_RAY_SESSION="$d/ray" LAB_RETRY_SLEEP=0
}

# --- B1 ---------------------------------------------------------------------------
(d="$(mktemp -d "$ROOT/b1.XXXX")"
lab "$d"
rc=0
out="$(bash "$COLLECT" --bug --to "$d/bundle" 2>"$d/err")" || rc=$?
((rc == 0)) || pnote "exit $rc: $(tail -3 "$d/err")"
[[ "$out" == *"BUG-BUNDLE $d/bundle"* ]] || pnote "no BUG-BUNDLE line: $out"
[[ -f "$d/bundle/results/lab/campaign.log" ]] || pnote "no campaign.log"
[[ -f "$d/bundle/driver.txt" ]] && grep -q '^## git' "$d/bundle/driver.txt" || pnote "driver.txt missing or without the git section"
for pc in pc00 pc01; do
	f="$d/bundle/pcs/$pc.txt"
	[[ -f "$f" ]] || { pnote "no pcs/$pc.txt"; continue; }
	grep -q 'pipedpeer version=0.0.1-test' "$f" || pnote "$pc: no pipedpeer version"
	grep -q "WARMUP-MARK-$pc" "$f" || pnote "$pc: no warm-up log"
	grep -q 'RAYLET-MARK' "$f" || pnote "$pc: no raylet.out"
	grep -q '^## memory' "$f" || pnote "$pc: no memory section"
done
grep -qE '^analyse: ' "$d/bundle/SUMMARY.txt" 2>/dev/null || pnote "SUMMARY.txt does not record analyse.py's outcome"
[[ -f "$d/bundle/results/lab/sizes.env" ]] || pnote "sizes.env (no secret, needed to debug) was left out"
(cd "$d/bundle" && sha256sum --quiet -c MANIFEST.sha256) || pnote "manifest does not verify"
verdict "B1 no runs.jsonl: bug bundle with driver logs, versions, one report per PC; analyse outcome recorded")

# --- B2 ---------------------------------------------------------------------------
(d="$(mktemp -d "$ROOT/b2.XXXX")"
lab "$d"
printf -- '-----BEGIN OPENSSH PRIVATE KEY-----\nKEYSECRET\n-----END OPENSSH PRIVATE KEY-----\n' >"$d/lab/stray-key"
echo 'RCLONE_CONFIG_R2_SECRET_ACCESS_KEY=R2SECRET' >"$d/lab/r2.env"
echo '{"token":"CLUSTERTOKEN"}' >"$d/lab/cluster.json"
mkdir -p "$d/lab/logs/connect"
echo "token=LOGTOKEN42 sent" >"$d/lab/logs/connect/pc01"
bash "$COLLECT" --bug --to "$d/bundle" >"$d/out" 2>"$d/err" || pnote "exit $?: $(tail -3 "$d/err")"
for s in DRVSECRET77xyz PCSECRET9abc KEYSECRET R2SECRET CLUSTERTOKEN LOGTOKEN42 'PRIVATE KEY'; do
	if grep -rqF -- "$s" "$d/bundle" 2>/dev/null; then pnote "secret [$s] in $(grep -rlF -- "$s" "$d/bundle" | head -2 | tr '\n' ' ')"; fi
done
grep -rq 'pipedpeer connect <redacted>' "$d/bundle" || pnote "no redaction marker"
for f in stray-key r2.env cluster.json; do
	grep -q "left out: .*$f" "$d/bundle/SUMMARY.txt" 2>/dev/null || pnote "SUMMARY.txt does not name $f as left out"
done
verdict "B2 secrets: codes redacted, keys/r2.env/cluster.json left out and named")

# --- B3 ---------------------------------------------------------------------------
(d="$(mktemp -d "$ROOT/b3.XXXX")"
lab "$d"
rc=0
FAKELAB_FAIL=pc01 bash "$COLLECT" --bug --to "$d/bundle" >"$d/out" 2>"$d/err" || rc=$?
((rc == 0)) || pnote "exit $rc"
grep -q 'pipedpeer version' "$d/bundle/pcs/pc00.txt" 2>/dev/null || pnote "pc00 report missing"
grep -qi 'no answer' "$d/bundle/pcs/pc01.txt" 2>/dev/null || pnote "pc01 report does not say it gave no answer"
grep -q 'pc01' "$d/bundle/SUMMARY.txt" 2>/dev/null || pnote "SUMMARY.txt does not list pc01"
verdict "B3 a PC that does not answer: said so, the others collected, exit 0")

# --- B4 ---------------------------------------------------------------------------
(d="$(mktemp -d "$ROOT/b4.XXXX")"
mkdir -p "$d/lab"
rc=0
LAB_RESULTS="$d/lab" bash "$COLLECT" --to "$d/bundle" >"$d/out" 2>"$d/err" || rc=$?
((rc != 0)) || pnote "plain collect.sh exited 0 without runs.jsonl"
[[ ! -d "$d/bundle" ]] || pnote "plain collect.sh made a folder"
verdict "B4 ordinary collect without --bug still refuses a lab with no runs.jsonl")

pass="$(grep -c '^ok$' "$RESULTS" || true)"
total="$(wc -l <"$RESULTS")"
printf 'PASS %d/%d\n' "$pass" "$total"
((pass == total && total > 0))
