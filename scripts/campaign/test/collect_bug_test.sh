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
# the redaction and the PC script, to test them on their own
# shellcheck source=/dev/null
source "$REPO/scripts/campaign/collect-bug.sh"

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

# --- B5 a name with key/token/secret/password/credential, a bearer token, a
# URL with user:pass@, and quoted invite codes: near-miss names go too
(d="$(mktemp -d "$ROOT/b5.XXXX")"
mkdir -p "$d/f"
cat >"$d/f/secrets.txt" <<'EOF'
sentry_key=0c7a3f7635bd9f9e92056c3c6e5f0b3a
token=LOGTOKEN42 sent
--api_key=APIKEY
AWS_SESSION_TOKEN=AWSTOK
sentry_token: COLONTOKEN
Authorization: Bearer BEARER123
check https://user:pass@example.com/path
pipedpeer connect PPCODE123
pipedpeer connect "QUOTED456"
monkey=1
EOF
_bug_redact "$d/f"
for s in 0c7a3f7635bd9f9e92056c3c6e5f0b3a LOGTOKEN42 APIKEY AWSTOK COLONTOKEN BEARER123 user:pass PPCODE123 QUOTED456; do
	grep -qF -- "$s" "$d/f/secrets.txt" && pnote "[$s] not redacted: $(grep -F -- "$s" "$d/f/secrets.txt")"
done
grep -q 'pipedpeer connect <redacted>' "$d/f/secrets.txt" || pnote "plain invite code not redacted"
grep -q '^monkey=<redacted>$' "$d/f/secrets.txt" || pnote "a name containing key but not a secret word was left readable: $(cat "$d/f/secrets.txt")"
verdict "B5 near-miss secret names, bearer values, user:pass URLs and quoted codes are redacted")

# --- B6 opposite: ordinary words stay readable
(d="$(mktemp -d "$ROOT/b6.XXXX")"
mkdir -p "$d/f"
cat >"$d/f/plain.txt" <<'EOF'
keyboard layout=us
the worker joined
keyboard
notes: nothing secret here
EOF
cp "$d/f/plain.txt" "$d/before.txt"
_bug_redact "$d/f"
cmp -s "$d/before.txt" "$d/f/plain.txt" || pnote "ordinary text was changed: $(cat "$d/f/plain.txt")"
verdict "B6 opposite: keyboard and plain prose stay readable")

# --- B9 cold2 findings 2 and 3: Authorization schemes, quoted secrets with spaces, token URLs, and setup/env invite codes
(d="$(mktemp -d "$ROOT/b9.XXXX")"
mkdir -p "$d/f"
cat >"$d/f/secrets2.txt" <<'EOF'
Authorization: Basic dXNlcjpwYXNzd29yZA==
Authorization: Token ghp_secret123
Authorization: ApiKey secretkey
password="correct horse battery staple"
api_key: 'key with spaces'
https://ghp_token123@github.com/repo.git
pipedpeer setup --connect PPCONNECT1
setup --connect "PPCONNECT2"
--connect=PPCONNECT3
--connect="PPCONNECT4"
PIPEDPEER_CONNECT_CODE=PPCONNECT5
PIPEDPEER_CONNECT_CODE="PPCONNECT6"
EOF
_bug_redact "$d/f"
for s in dXNlcjpwYXNzd29yZA== ghp_secret123 secretkey "correct horse battery staple" "key with spaces" ghp_token123 PPCONNECT1 PPCONNECT2 PPCONNECT3 PPCONNECT4 PPCONNECT5 PPCONNECT6; do
	grep -qF -- "$s" "$d/f/secrets2.txt" && pnote "[$s] not redacted: $(grep -F -- "$s" "$d/f/secrets2.txt")"
done
grep -q 'Authorization: <redacted>' "$d/f/secrets2.txt" || pnote "Authorization header not fully redacted"
verdict "B9 Authorization schemes, quoted passwords with spaces, token URLs and connect codes are redacted")

# --- B10 cold2 near-misses: https://github.com/x, user@host, --connect-timeout 5 stay readable
(d="$(mktemp -d "$ROOT/b10.XXXX")"
mkdir -p "$d/f"
cat >"$d/f/plain2.txt" <<'EOF'
visit https://github.com/x for details
contact user@host for questions
curl --connect-timeout 5 https://example.com
EOF
cp "$d/f/plain2.txt" "$d/before2.txt"
_bug_redact "$d/f"
cmp -s "$d/before2.txt" "$d/f/plain2.txt" || pnote "near-misses were modified: $(cat "$d/f/plain2.txt")"
verdict "B10 opposite: https://github.com/x, user@host and --connect-timeout stay readable")

# --- B7 cgroup memory files are collected when the remote shell is dash,
# on cgroup v2 and v1 (the brace expansion dash does not do left this empty)
(d="$(mktemp -d "$ROOT/b7.XXXX")"
mkdir -p "$d/cg/user.slice/run.scope" "$d/cg/memory/docker/abc"
echo 123456 >"$d/cg/user.slice/run.scope/memory.current"
echo 654321 >"$d/cg/user.slice/run.scope/memory.max"
echo 222222 >"$d/cg/memory/docker/abc/memory.usage_in_bytes"
echo 7 >"$d/cg/memory/docker/abc/memory.failcnt"
printf '0::/user.slice/run.scope\n5:memory:/docker/abc\n' >"$d/proc_cgroup"
cgmem_body="$(sed -n '/^cgmem() {/,/^}/p' <( _bug_pc_script ))"
[[ -n "$cgmem_body" ]] || pnote "cgmem is not in the PC script"
out="$(PP_CG_ROOT="$d/cg" PP_PROC_CGROUP="$d/proc_cgroup" sh -c "$cgmem_body
cgmem" 2>&1)" || pnote "cgmem failed under sh: $out"
grep -q -- "--- $d/cg/user.slice/run.scope/memory.current" <<<"$out" || pnote "v2 memory.current not collected: $out"
grep -q '^123456$' <<<"$out" || pnote "v2 memory.current value missing"
grep -q -- "--- $d/cg/memory/docker/abc/memory.usage_in_bytes" <<<"$out" || pnote "v1 usage_in_bytes not collected: $out"
grep -q '^222222$' <<<"$out" || pnote "v1 usage_in_bytes value missing"
verdict "B7 cgroup memory files collected under dash: v2 and v1")

# --- B8 opposite: no cgroup files: a header line, no error
(d="$(mktemp -d "$ROOT/b8.XXXX")"
mkdir -p "$d/cg"
r=0
out="$(PP_CG_ROOT="$d/cg" PP_PROC_CGROUP=/dev/null sh -c "$(sed -n '/^cgmem() {/,/^}/p' <( _bug_pc_script ))
cgmem" 2>&1)" || r=$?
((r == 0)) || pnote "cgmem exited $r with no cgroup files"
grep -q '^cgroup: v2 path ' <<<"$out" || pnote "no cgroup header: $out"
grep -q '^--- ' <<<"$out" && pnote "invented a cgroup file: $out"
verdict "B8 opposite: no cgroup files, a header and no error")

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
