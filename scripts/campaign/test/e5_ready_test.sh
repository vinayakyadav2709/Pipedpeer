#!/usr/bin/env bash
# Case table for E5 ready definition (unit E5F).
set -euo pipefail

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
CAMP="$REPO/scripts/campaign"
WORKLOADS="$REPO/docs/benchmarks/workloads"
STATE="${XDG_STATE_HOME:-$HOME/.local/state}/pipedpeer"
mkdir -p "$STATE"
pass=0; failed=0; problems=()

DIRS_FILE="$(mktemp)"
cleanup() {
  local d
  while IFS= read -r d; do
    [[ -n "$d" ]] || continue
    rm -rf "$d"
  done <"$DIRS_FILE"
  rm -f "$DIRS_FILE"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

ok() { pass=$((pass + 1)); echo "ok   $1"; }
bad() { failed=$((failed + 1)); echo "FAIL $1 -- $2"; }
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
  local d
  d="$(mktemp -d "$STATE/e-e5ready-test.XXXXXX")"
  echo "$d" >>"$DIRS_FILE"
  echo "$d"
}

# ---- Case 1: pipedpeer timeline -> ready 13 (earliest start+dur)
d="$(newdir)"
printf '#id\tip\tuser\thostname\tnproc\tram_gib\n' >"$d/hosts.tsv"
printf 'pc00\t127.0.0.1\tlab\tpc00.lab\t8\t31\n' >>"$d/hosts.tsv"
printf 'id,offset_ms,rtt_ms,measured_at\npc00,0,0,x\n' >"$d/clocks.csv"
printf '0' >"$d/t0"
cat >"$d/timeline.json" <<'JSON'
{"names":{"pc00.lab":"pc00"},"steps":[
 {"step":"compute","node":"","machine":"pc00.lab","start_ns":10000000000,"dur_ns":5000000000},
 {"step":"compute","node":"","machine":"pc00.lab","start_ns":12000000000,"dur_ns":1000000000}
]}
JSON
export E5_DIR="$d" E5_HOSTS="$d/hosts.tsv" E5_CLOCKS="$d/clocks.csv" E5_IDS="pc00"
out="$(cd "$REPO" && bash -c 'source scripts/campaign/e5.sh; e5_py pipedpeer' 2>"$d/err")" || pnote "e5_py pipedpeer exit $?: $(cat "$d/err")"
val="$(printf '%s' "$out" | awk '{print $2}')"
python3 -c "import sys; v=float(sys.argv[1]); assert abs(v-13.0)<1e-9, v" "$val" 2>/dev/null || pnote "ready=$val, want 13; out=[$out] err=[$(cat "$d/err")]"
[[ -s "$d/err" ]] && ! grep -qi "no ready time" "$d/err" || true
verdict "timeline steps start=10,dur=5 and start=12,dur=1 -> ready 13"

# ---- Case 2: PC with no compute step -> no ready, warning, empty
d="$(newdir)"
printf '#id\tip\tuser\thostname\tnproc\tram_gib\n' >"$d/hosts.tsv"
printf 'pc00\t127.0.0.1\tlab\tpc00.lab\t8\t31\n' >>"$d/hosts.tsv"
printf 'id,offset_ms,rtt_ms,measured_at\npc00,0,0,x\n' >"$d/clocks.csv"
printf '0' >"$d/t0"
cat >"$d/timeline.json" <<'JSON'
{"names":{"pc00.lab":"pc00"},"steps":[{"step":"script","node":"","machine":"pc00.lab","start_ns":1000000000,"dur_ns":100000000}]}
JSON
export E5_DIR="$d" E5_HOSTS="$d/hosts.tsv" E5_CLOCKS="$d/clocks.csv" E5_IDS="pc00"
out="$(cd "$REPO" && bash -c 'source scripts/campaign/e5.sh; e5_py pipedpeer' 2>"$d/err")" || true
[[ -z "$out" ]] || pnote "expected no ready line, got: $out"
grep -qi "no ready time" "$d/err" || pnote "expected warning on stderr, got: $(cat "$d/err")"
verdict "PC with no compute step: no ready, warning, empty ready_s"

# ---- Case 3: pip probe fails on one PC -> row empty, logged, others kept
d="$(newdir)"
printf 'id,offset_ms,rtt_ms,measured_at\npc00,0,0,x\npc01,0,0,x\n' >"$d/clocks.csv"
export E5_DIR="$d" E5_START="0" E5_PIP="pc01 1000000000" E5_CLOCKS="$d/clocks.csv" E5_HOSTS="$d/hosts.tsv" E5_IDS="pc00 pc01"
printf '#id\tip\tuser\thostname\tnproc\tram_gib\n' >"$d/hosts.tsv"
printf 'pc00\t127.0.0.1\tlab\tpc00.lab\t8\t31\npc01\t127.0.0.1\tlab\tpc01.lab\t8\t31\n' >>"$d/hosts.tsv"
out="$(cd "$REPO" && bash -c 'source scripts/campaign/e5.sh; e5_py pip' 2>"$d/err")" || true
printf '%s' "$out" | grep -q "pc01" || pnote "pc01 missing from output: $out"
! printf '%s' "$out" | grep -q "pc00" || pnote "pc00 should be absent: $out"
grep -qi "no ready time" "$d/err" || pnote "expected warning for pc00: $(cat "$d/err")"
verdict "pip probe fails on one PC: row empty, logged, others kept"

# ---- Case 4: ray requirements containing --extra-index-url passed through
d="$(newdir)"
printf -- '--extra-index-url x\nnumpy==1.0\n' >"$d/req.txt"
printf '# a comment line\n' >>"$d/req.txt"
res="$(cd "$REPO" && python3 -c "
import sys; sys.path.insert(0, '$WORKLOADS')
import e5_ray
print(e5_ray.requirements('$d/req.txt'))
" 2>&1)"
printf '%s' "$res" | grep -q "extra-index-url x" || pnote "extra-index-url not kept: $res"
printf '%s' "$res" | grep -q "numpy==1.0" || pnote "numpy dropped: $res"
# comment line should not appear as a non-empty entry
python3 -c "
import sys; sys.path.insert(0, '$WORKLOADS')
import e5_ray
r = e5_ray.requirements('$d/req.txt')
assert all('a comment line' not in x for x in r), r
" 2>"$d/pyerr" || pnote "comment line not dropped: $(cat "$d/pyerr")"
verdict "ray requirements: --extra-index-url intact, comment line dropped"

# ---- Cases 6-7: the real e5_ray.py against a stub ray whose second probe fails
ray_stub() { # <dir> <fail: 0|1>
  mkdir -p "$1/stub/ray/util"
  : >"$1/stub/ray/util/__init__.py"
  cat >"$1/stub/ray/util/scheduling_strategies.py" <<'EOF'
class NodeAffinitySchedulingStrategy:
    def __init__(self, node_id, soft):
        self.node_id = node_id
EOF
  cat >"$1/stub/ray/__init__.py" <<EOF
FAIL = $2
_nodes = [{"Alive": True, "NodeID": "a", "NodeManagerAddress": "10.0.0.1"},
          {"Alive": True, "NodeID": "b", "NodeManagerAddress": "10.0.0.2"}]
def init(**kw): pass
def nodes(): return _nodes
class _R:
    def options(self, scheduling_strategy=None, **kw):
        self.n = scheduling_strategy.node_id
        return self
    def remote(self, *a):
        return self.n
def remote(**kw):
    return lambda f: _R()
def get(ref):
    if FAIL and ref == "b":
        raise RuntimeError("ModuleNotFoundError: sklearn")
    return ("10.0.0.1" if ref == "a" else "10.0.0.2", 123)
EOF
  : >"$1/req.txt"
}
d="$(newdir)"; ray_stub "$d" 1; rc=0
PYTHONPATH="$d/stub" python3 "$WORKLOADS/e5_ray.py" --nodes 2 --requirements "$d/req.txt" >"$d/out" 2>"$d/err" || rc=$?
grep -q 'E5-RAY node=10.0.0.1 ' "$d/out" || pnote "healthy PC lost: $(cat "$d/out" "$d/err")"
! grep -q 'E5-RAY node=10.0.0.2 ' "$d/out" || pnote "failed PC has a ready line"
grep -q '10.0.0.2.*sklearn' "$d/err" || pnote "no stderr line naming the PC and the error: $(cat "$d/err")"
[[ "$rc" == 1 ]] || pnote "exit should be 1 when a PC failed, got $rc"
verdict "Ray probe import fails on one PC: that PC has no ready, error on stderr, the other kept"

d="$(newdir)"; ray_stub "$d" 0; rc=0
PYTHONPATH="$d/stub" python3 "$WORKLOADS/e5_ray.py" --nodes 2 --requirements "$d/req.txt" >"$d/out" 2>"$d/err" || rc=$?
[[ "$(grep -c '^E5-RAY ' "$d/out")" == 2 && "$rc" == 0 ]] || pnote "rc=$rc $(cat "$d/out" "$d/err")"
grep -q 'E5-OK nodes=2' "$d/out" || pnote "no E5-OK nodes=2"
verdict "Ray probe works on both PCs: two ready lines, exit 0"

echo "TOTAL: pass=$pass failed=$failed"
exit "$failed"