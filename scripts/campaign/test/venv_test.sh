#!/usr/bin/env bash
# Rows for venv.sh's remote build command, run with the real uv in a private
# HOME (the command is the one venv.sh sends, with ~ = that HOME and uv copied
# to ~/pipedpeer-lab/bin/uv as venv.sh does). Needs uv and network once for the
# CPython download: without them the rows SKIP, named.
set -u
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
uv=$(command -v uv) || { echo "SKIP venv rows: uv not installed"; echo "PASS 0/0 (skipped)"; exit 0; }
t=$(mktemp -d)
trap 'rm -rf "$t"' EXIT
mkdir -p "$t/home/pipedpeer-lab/bin" "$t/home/pipedpeer-lab/work"
cp "$uv" "$t/home/pipedpeer-lab/bin/uv"
: >"$t/home/pipedpeer-lab/requirements.txt"
cp "$here/../../../docs/benchmarks/workloads/versions.py" "$t/home/pipedpeer-lab/work/"
# shellcheck source=/dev/null
LAB="$t/lab" . "$here/../venv.sh"
set +eo pipefail
cmd=$(venv_command 3.11)
out=$(HOME="$t/home" UV_CACHE_DIR="$t/uvcache" timeout 300 bash -c "$cmd" 2>"$t/err"); rc=$?
pass=0
total=0
row() { total=$((total + 1)); if [ "$2" = 0 ]; then pass=$((pass + 1)); echo "ok $1"; else echo "FAIL $1: $3"; fi; }
if [ "$rc" != 0 ] && grep -qiE 'error sending request|dns|network|connect' "$t/err"; then
	echo "SKIP venv rows: no network for the CPython download"; sed -n 1,3p "$t/err"; exit 0
fi
ok=1; [ "$rc" = 0 ] && ok=0
row "build command exits 0" $ok "rc=$rc $(tail -2 "$t/err" | tr '\n' ' ')"
ok=1; grep -q '^VERSIONS python=3\.11\.' <<<"$out" && ok=0
row "venv python prints its VERSIONS line" $ok "out=[$out]"
ok=1; [ -x "$t/home/pipedpeer-lab/venv/bin/pip" ] && ok=0
row "venv has its own pip" $ok "no venv/bin/pip"
ok=1; [ ! -e "$t/home/.local/bin/python3.11" ] && ok=0
row "no python3.11 link left in ~/.local/bin" $ok "uv wrote a bin link"
# rerun over the venv the first run left (OPERATOR.md: fix, then run the same
# script again): newer uv refuses an existing venv unless told to replace it
out2=$(HOME="$t/home" UV_CACHE_DIR="$t/uvcache" timeout 300 bash -c "$cmd" 2>"$t/err2"); rc2=$?
ok=1; [ "$rc2" = 0 ] && grep -q '^VERSIONS python=3\.11\.' <<<"$out2" && ok=0
row "a second run over the existing venv exits 0 with its VERSIONS line" $ok "rc=$rc2 $(tail -2 "$t/err2" | tr '\n' ' ')"
echo "PASS $pass/$total"
[ "$pass" = "$total" ]
