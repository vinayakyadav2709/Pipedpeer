#!/usr/bin/env bash
# Serve lab.sh to the lab PCs from a laptop on the lab network: the fallback
# to the published copy (HEADLESS.md), for worker setup only.
#   scripts/campaign/serve.sh [ip]   start (ip: the driver's lab-facing address;
#                                    default: the source address of the default route)
#   scripts/campaign/serve.sh stop   stop it
# Creates the campaign key ~/.ssh/pipedpeer-lab_ed25519 when missing, fills its
# public half, this checkout's commit and the server's URL into
# results/lab/serve/lab.sh, runs python3 -m http.server from there in the
# background, and prints the one line the operator types on each PC.
# Env: LAB_KEY (key path), LAB_RESULTS (default <repo>/results/lab), LAB_PORT (default 8000).
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$here/../.." && pwd)
key=${LAB_KEY:-$HOME/.ssh/pipedpeer-lab_ed25519}
results=${LAB_RESULTS:-$repo_root/results/lab}
port=${LAB_PORT:-8000}
dir=$results/serve
pidf=$results/serve.pid
logf=$results/serve.log

die() { echo "serve.sh: $*" >&2; exit 1; }

running() { [[ -f $pidf ]] && kill -0 "$(cat "$pidf")" 2>/dev/null; }

stop() {
  if running; then kill "$(cat "$pidf")"; echo "stopped http.server (pid $(cat "$pidf"))"; fi
  rm -f "$pidf"
}

if [[ ${1:-} == stop ]]; then stop; exit 0; fi
[[ $# -le 1 ]] || die "usage: serve.sh [ip] | stop"

ip=${1:-}
if [[ -z $ip ]]; then
  ip=$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1);exit}}' || true)
  [[ -n $ip ]] || die "cannot tell the lab-facing IP; pass it: serve.sh <ip>"
fi
[[ $ip =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "not an IPv4 address: $ip"
command -v python3 >/dev/null || die "python3 not found"

if [[ ! -f $key ]]; then
  mkdir -p "$(dirname "$key")"; chmod 700 "$(dirname "$key")"
  ssh-keygen -q -t ed25519 -N '' -C "pipedpeer-lab@$(uname -n)" -f "$key"
  echo "created campaign key $key"
fi
[[ -f $key.pub ]] || ssh-keygen -y -f "$key" >"$key.pub"

stop >/dev/null
mkdir -p "$dir"
commit=$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || echo 0000000000000000000000000000000000000000)
bash "$here/lab-fill.sh" fill "$here/lab.sh" "$dir/lab.sh" "$(cat "$key.pub")" "$commit" "http://$ip:$port"

(cd "$dir" && exec nohup python3 -m http.server "$port" --bind "$ip" >"$logf" 2>&1 </dev/null) &
echo $! >"$pidf"

# Wait until it answers, so the line printed below works when the operator types it.
for _ in $(seq 50); do
  if python3 -c "import urllib.request,sys; urllib.request.urlopen(sys.argv[1], timeout=1)" "http://$ip:$port/lab.sh" 2>/dev/null; then
    echo "serving $dir on http://$ip:$port (pid $(cat "$pidf"); stop: scripts/campaign/serve.sh stop)"
    echo "On each lab PC, as the lab user, type:"
    echo
    echo "  curl -fsSL http://$ip:$port/lab.sh | sudo bash"
    echo
    exit 0
  fi
  running || break
  sleep 0.2
done
cat "$logf" >&2 || true
stop >/dev/null
die "http.server on $ip:$port did not come up"
