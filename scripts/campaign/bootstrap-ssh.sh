#!/usr/bin/env bash
# Set up lab PCs that already run sshd with a shared account and password,
# without visiting them ("zero PC visits"):
#   scripts/campaign/serve.sh [ip]                 # first: serves lab-pc.sh and the key
#   scripts/campaign/bootstrap-ssh.sh <subnet|host[,host...]|@file> --user <labuser> [--jobs N]
# It asks for the account's password once (no echo; read from stdin when
# stdin is not a terminal), then on every host, in parallel, logs in with it
# and runs exactly the operator's line,
#   curl -fsS http://DRIVER:PORT/lab-pc.sh | sudo bash -s -- setup
# after one `sudo -S -v` that reads the password from the ssh session's
# stdin. The result is what typing the line at each PC gives: the campaign
# key in authorized_keys, passwordless sudo, the ports open, and one LAB-PC
# line per host, printed and appended to results/lab/bootstrap.txt.
#
# The password is never written to disk or to a log, and is never in a
# command line. It reaches ssh through SSH_ASKPASS (OpenSSH >= 8.4, with
# SSH_ASKPASS_REQUIRE=force; the helper script holds no secret and reads it
# from its environment), or through sshpass -e when ssh is older, and the
# remote sudo through the ssh session's stdin, read by the shell builtin
# `read`. Both environments are this user's own processes only.
#
# A subnet is swept for 22/tcp first (as scan.sh does). Afterwards, run
# scan.sh <subnet> --user <labuser> to build the inventory.
# Env: LAB_RESULTS (default <repo>/results/lab), LAB_SSH_OPTS (extra ssh
# options, word-split), LAB_BOOT_MODE=askpass|sshpass (force one; tests).
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$here/../.." && pwd)
results=${LAB_RESULTS:-$repo_root/results/lab}

die() { echo "bootstrap-ssh: $*" >&2; exit 1; }
usage() { die "usage: bootstrap-ssh.sh <subnet|host[,host...]|@file> --user <labuser> [--jobs N]"; }

target="" user="" jobs=32
while (($#)); do
	case "$1" in
	--user) user=${2:-}; [[ -n $user ]] || usage; shift 2 ;;
	--jobs) jobs=${2:-}; [[ $jobs =~ ^[0-9]+$ ]] || usage; shift 2 ;;
	-*) usage ;;
	*) [[ -z $target ]] || usage; target=$1; shift ;;
	esac
done
[[ -n $target && -n $user ]] || usage
[[ $user =~ ^[a-z_][a-z0-9_.-]*$ ]] || die "bad user name: $user"

served=$results/serve/lab-pc.sh
[[ -f $served ]] || die "no $served; run scripts/campaign/serve.sh first"
driver=$(sed -n 's/^DRIVER="\(.*\)"$/\1/p' "$served" | head -n1)
port=$(sed -n 's/^PORT="\(.*\)"$/\1/p' "$served" | head -n1)
[[ $driver =~ ^[0-9A-Za-z.:-]+$ && $port =~ ^[0-9]+$ ]] || die "DRIVER/PORT not filled in in $served; rerun serve.sh"
url="http://$driver:$port/lab-pc.sh"
curl -fsS -o /dev/null "$url" || die "$url does not answer; is serve.sh running?"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# 1. Hosts.
case $target in
@*) grep -Eo '^[^#[:space:]]+' "${target#@}" >"$work/hosts" || true ;;
*/*)
	python3 -c 'import ipaddress,sys; ipaddress.ip_network(sys.argv[1], strict=False)' "$target" 2>/dev/null || die "not a subnet: $target"
	echo "bootstrap-ssh: sweeping $target for 22/tcp"
	python3 - "$target" >"$work/hosts" <<'EOF'
import asyncio, ipaddress, sys
net = ipaddress.ip_network(sys.argv[1], strict=False)
hosts = [str(h) for h in (net.hosts() if net.num_addresses > 2 else net)]
sem = asyncio.Semaphore(512)
async def probe(h):
    async with sem:
        try:
            _, w = await asyncio.wait_for(asyncio.open_connection(h, 22), 1.5)
            w.close()
            return h
        except Exception:
            return None
async def main():
    for h in await asyncio.gather(*(probe(h) for h in hosts)):
        if h:
            print(h)
asyncio.run(main())
EOF
	;;
*) tr ', ' '\n\n' <<<"$target" | grep -v '^$' >"$work/hosts" ;;
esac
sort -u -o "$work/hosts" "$work/hosts"
n=$(wc -l <"$work/hosts")
((n > 0)) || die "no hosts with 22/tcp open in $target"
echo "bootstrap-ssh: $n host(s): $(tr '\n' ' ' <"$work/hosts")"

# 2. How ssh gets the password.
ssh_ver=$(ssh -V 2>&1 | sed -n 's/^OpenSSH_\([0-9]*\)\.\([0-9]*\).*/\1 \2/p')
read -r maj min <<<"${ssh_ver:-0 0}"
mode=${LAB_BOOT_MODE:-}   # askpass|sshpass forces one (tests)
if [[ $mode == askpass ]] || [[ -z $mode ]] && ((maj > 8 || (maj == 8 && min >= 4))); then
	mode=askpass
	printf '#!/bin/sh\nprintf "%%s\\n" "$LAB_BOOT_PW"\n' >"$work/askpass"
	chmod 700 "$work/askpass"
elif [[ $mode != askpass ]] && command -v sshpass >/dev/null; then
	mode=sshpass
else
	die "need OpenSSH >= 8.4 (SSH_ASKPASS_REQUIRE) or sshpass; have $(ssh -V 2>&1)"
fi

# 3. The password, once.
if [[ -t 0 ]]; then
	read -rsp "password for $user on the lab PCs: " pw </dev/tty
	echo
else
	IFS= read -r pw || true
fi
[[ -n $pw ]] || die "empty password"

# The remote side: the password is the first line of stdin, read by the
# builtin `read` (no process sees it in argv); `sudo -S -v` caches the
# credential, then the operator's own line runs under `sudo -n`. With an
# account that already has passwordless sudo, -v reads nothing.
# shellcheck disable=SC2016  # expanded on the PC
remote='IFS= read -r pw; printf "%s\n" "$pw" | sudo -S -p "" -v || { echo "ERROR: sudo refused the password"; exit 1; }; unset pw; curl -fsS '"$url"' | sudo -n bash -s -- setup'

# shellcheck disable=SC2086  # LAB_SSH_OPTS word-splits on purpose
one() {
	local host=$1 out=$work/out.$1
	{
		if [[ $mode == askpass ]]; then
			printf '%s\n' "$pw" | LAB_BOOT_PW=$pw SSH_ASKPASS=$work/askpass SSH_ASKPASS_REQUIRE=force DISPLAY=none \
				ssh ${LAB_SSH_OPTS:-} -o PubkeyAuthentication=no -o PreferredAuthentications=keyboard-interactive,password \
				-o NumberOfPasswordPrompts=1 -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 \
				"$user@$host" "$remote"
		else
			printf '%s\n' "$pw" | SSHPASS=$pw sshpass -e ssh ${LAB_SSH_OPTS:-} -o PubkeyAuthentication=no \
				-o PreferredAuthentications=keyboard-interactive,password -o NumberOfPasswordPrompts=1 \
				-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 "$user@$host" "$remote"
		fi
	} >"$out" 2>&1 </dev/null || echo "exit $?" >>"$out"
}

# 4. All hosts, $jobs at a time.
echo "bootstrap-ssh: logging in as $user ($mode), running lab-pc.sh setup from $url"
running=0
while IFS= read -r h; do
	one "$h" &
	running=$((running + 1))
	if ((running >= jobs)); then wait -n || true; running=$((running - 1)); fi
done <"$work/hosts"
wait
unset pw

# 5. One line per host.
mkdir -p "$results"
{
	echo "# bootstrap-ssh $(date -u +%FT%TZ) $target user=$user"
	while IFS= read -r h; do
		line=$(grep -a '^LAB-PC ' "$work/out.$h" | tail -n1 || true)
		if [[ -n $line ]]; then
			echo "$h $line"
		else
			echo "$h ERROR $(grep -a -v '^\s*$' "$work/out.$h" | tail -n3 | tr '\n' ' ')"
		fi
	done <"$work/hosts"
} | tee -a "$results/bootstrap.txt"
ok=$(grep -c ' LAB-PC ' <(tail -n "$n" "$results/bootstrap.txt") || true)
echo "bootstrap-ssh: $ok of $n set up; LAB-PC lines in $results/bootstrap.txt"
((ok == n))
