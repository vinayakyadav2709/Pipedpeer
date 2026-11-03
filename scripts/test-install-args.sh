#!/usr/bin/env bash
# Case table §6 of docs/peer-cache-cases.md, run against scripts/install-pipedpeer.sh.
# The installer runs end to end against a local file:// release, with a stub
# `pipedpeer` that records its argv and whether its stdin is a terminal.
# HOME and TMPDIR are private to this run; nothing real is installed.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
installer="$here/install-pipedpeer.sh"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
export HOME="$work/home" TMPDIR="$work/tmp"
mkdir -p "$HOME" "$TMPDIR"
unset PIPEDPEER_VERSION PIPEDPEER_CHANNEL PIPEDPEER_INSTALL_DIR TERMUX_VERSION PREFIX

# Artifact name as the installer computes it on this machine.
plat="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$(uname -m)" in
  x86_64|amd64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *) arch="$(uname -m)" ;;
esac
artifact="pipedpeer-${plat}-${arch}"

# Fake release: download/nightly-v0.9.1/ holds the stub and its checksum.
rel="$work/rel"
mkdir -p "$rel/download/nightly-v0.9.1"
stub_src="$work/stub-src"
cat > "$stub_src" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" > "$STUB_OUT"
printf '%s' "${PIPEDPEER_CONNECT_CODE:-}" > "$STUB_OUT.env"
if [ -t 0 ]; then echo tty > "$STUB_OUT.stdin"; else echo notty > "$STUB_OUT.stdin"; fi
exit "${STUB_EXIT:-0}"
EOF
cp "$stub_src" "$rel/download/nightly-v0.9.1/$artifact"
(cd "$rel/download/nightly-v0.9.1" && sha256sum "$artifact" > checksums.txt)
# The default (no --version) stream is download/nightly; the same build serves it.
mkdir -p "$rel/download/nightly"
cp "$rel/download/nightly-v0.9.1/"* "$rel/download/nightly/"

pass=0; fail=0
ok()  { pass=$((pass+1)); echo "ok   $1"; }
bad() { fail=$((fail+1)); echo "FAIL $1: $2"; }

# run_case NAME [installer args...]: stdin is /dev/null; sets rc, out, args, stdin_kind.
run_case() {
  local name=$1; shift
  local dir="$work/case-$name"
  mkdir -p "$dir/bin"
  export STUB_OUT="$dir/args"
  export PIPEDPEER_RELEASE_BASE_URL="file://$rel" PIPEDPEER_INSTALL_DIR="$dir/bin"
  bash "$installer" "$@" </dev/null >"$dir/out" 2>&1
  rc=$?
  out="$(cat "$dir/out")"
  args="$(cat "$STUB_OUT" 2>/dev/null || true)"
  stdin_kind="$(cat "$STUB_OUT.stdin" 2>/dev/null || echo missing)"
}

# Argv of the stub as one line, arguments sorted (for "any order" rows).
sorted_args() { printf '%s\n' "$args" | sort | tr '\n' ' '; }

# 6.1 --version v0.9.1 → setup with no extra args
run_case 6.1 --version v0.9.1
if [[ $rc -eq 0 && "$args" == "setup" && "$out" == *"Installed:"* ]]; then
  ok 6.1
else
  bad 6.1 "rc=$rc args=[$(echo "$args" | tr '\n' ' ')]"
fi

# 6.2 --version v0.9.1 --connect PP1-abc → setup runs with the code in its
# environment, not on its command line (M5)
run_case 6.2 --version v0.9.1 --connect PP1-abc
if [[ $rc -eq 0 && "$args" == "setup" && "$(cat "$STUB_OUT.env")" == "PP1-abc" \
      && "$out" == *"joined; \`pipedpeer status\`"* && "$out" != *"Next:"* ]]; then
  ok 6.2
else
  bad 6.2 "rc=$rc args=[$(echo "$args" | tr '\n' ' ')]"
fi

# 6.2b on Termux the code stays in argv (proot re-run) and not in the env
TERMUX_VERSION=0.118 PREFIX="$work/prefix" run_case 6.2b --connect PP1-abc
if [[ "$args" == "$(printf 'setup\n--connect\nPP1-abc')" && -z "$(cat "$STUB_OUT.env" 2>/dev/null)" ]]; then
  ok 6.2b
else
  bad 6.2b "rc=$rc args=[$(echo "$args" | tr '\n' ' ')]"
fi

# 6.2c setup exits 1 with --connect (declined or failed) → installer fails, no "joined" (H1)
STUB_EXIT=1 run_case 6.2c --connect PP1-abc
if [[ $rc -eq 1 && "$out" != *"joined;"* && "$out" == *"NOT joined"* ]]; then ok 6.2c; else bad 6.2c "rc=$rc out=$out"; fi
# ... and exit 2 (partly) is passed through
STUB_EXIT=2 run_case 6.2d --connect PP1-abc
if [[ $rc -eq 2 && "$out" == *"NOT joined"* ]]; then ok 6.2d; else bad 6.2d "rc=$rc out=$out"; fi

# 6.3 --connect=PP1-abc -y → setup gets -y and the code in its environment
run_case 6.3 --connect=PP1-abc -y
want_sorted="$(printf '%s\n' setup -y | sort | tr '\n' ' ')"
if [[ $rc -eq 0 && "$(sorted_args)" == "$want_sorted" && "$(cat "$STUB_OUT.env")" == "PP1-abc" ]]; then
  ok 6.3
else
  bad 6.3 "rc=$rc args=[$(echo "$args" | tr '\n' ' ')]"
fi

# 6.4 --connect with no value, or followed by another flag → exit 1
run_case 6.4a --connect
if [[ $rc -eq 1 && "$out" == *"--connect requires a code"* ]]; then ok 6.4a; else bad 6.4a "rc=$rc out=$out"; fi
run_case 6.4b --connect --version v0.9.1
if [[ $rc -eq 1 && "$out" == *"--connect requires a code"* ]]; then ok 6.4b; else bad 6.4b "rc=$rc out=$out"; fi

# 6.5 --bogus → exit 1 as today
run_case 6.5 --bogus
if [[ $rc -eq 1 && "$out" == *"Unknown argument: --bogus"* ]]; then ok 6.5; else bad 6.5 "rc=$rc out=$out"; fi

# 6.6 curl | bash with a terminal → setup reads from /dev/tty. A pty is made
# by python3 (pty.fork), so the installer has a controlling terminal.
if command -v python3 >/dev/null 2>&1; then
  dir6="$work/case-6.6"; mkdir -p "$dir6/bin"
  export STUB_OUT="$dir6/args"; rm -f "$STUB_OUT" "$STUB_OUT.stdin"
  export PIPEDPEER_RELEASE_BASE_URL="file://$rel" PIPEDPEER_INSTALL_DIR="$dir6/bin"
  # The child runs `cat installer | bash -s`, as curl | bash does, so bash's
# stdin is the pipe and only /dev/tty can reach a terminal.
pty_out="$(python3 -I -c '
import os, pty, sys
pid, fd = pty.fork()
if pid == 0:
    os.execvp("sh", ["sh", "-c", "cat \"$1\" | bash -s -- \"${@:2}\"", "sh"] + sys.argv[1:])
buf = b""
while True:
    try:
        d = os.read(fd, 4096)
    except OSError:
        break
    if not d:
        break
    buf += d
_, st = os.waitpid(pid, 0)
sys.stdout.buffer.write(buf)
sys.exit(os.waitstatus_to_exitcode(st))
' "$installer" --version v0.9.1 2>&1)"
  rc=$?
  out="$pty_out"
  args="$(cat "$STUB_OUT" 2>/dev/null || true)"
  stdin_kind="$(cat "$STUB_OUT.stdin" 2>/dev/null || echo missing)"
  if [[ $rc -eq 0 && "$args" == "setup" && "$stdin_kind" == "tty" ]]; then
    ok 6.6
  else
    bad 6.6 "rc=$rc stdin=$stdin_kind args=[$(echo "$args" | tr '\n' ' ')]"
  fi
else
  echo "skip 6.6 (no python3 to make a pty)"
fi

# 6.7 no terminal (setsid: no controlling tty) → setup gets stdin as today
dir7="$work/case-6.7"; mkdir -p "$dir7/bin"
export STUB_OUT="$dir7/args"
export PIPEDPEER_RELEASE_BASE_URL="file://$rel" PIPEDPEER_INSTALL_DIR="$dir7/bin"
if ! command -v setsid >/dev/null 2>&1; then
  echo "skip 6.7 (no setsid)"
else
setsid -w sh -c 'cat "$1" | bash -s -- "${@:2}"' sh "$installer" --version v0.9.1 </dev/null >"$dir7/out" 2>&1
rc=$?
args="$(cat "$STUB_OUT" 2>/dev/null || true)"
stdin_kind="$(cat "$STUB_OUT.stdin" 2>/dev/null || echo missing)"
if [[ $rc -eq 0 && "$args" == "setup" && "$stdin_kind" == "notty" ]]; then
  ok 6.7
else
  bad 6.7 "rc=$rc stdin=$stdin_kind args=[$(echo "$args" | tr '\n' ' ')] out=$(cat "$dir7/out")"
fi
fi

echo "---"
echo "pass=$pass fail=$fail"
[[ $fail -eq 0 ]]
