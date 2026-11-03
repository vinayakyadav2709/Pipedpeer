#!/usr/bin/env bash
# The campaign's only way to reach a lab PC: Ansible, from a venv at
# $HOME/.local/share/pipedpeer-lab/ansible (no root). Sourced or executed:
#
#   lib/ansible.sh install                     # venv + ansible-core + community.docker + ansible.posix + matplotlib
#   lib/ansible.sh sh    <pattern> <command>   # run command (sh -c) on every matching host
#   lib/ansible.sh bg    <pattern> <log> <cmd> # start command detached; output to <log> on the host
#   lib/ansible.sh put   <pattern> <src> <dest># copy a driver file to the hosts
#   lib/ansible.sh ping  [pattern]
#
# PL_INVENTORY picks the inventory (default results/lab/inventory.ini).
# `sh` prints, for a single host, its stdout exactly; for several, each line
# prefixed "<host>: ". Its exit status is the first failing host's rc.
# Everything goes through the raw module, so a host needs only a POSIX shell;
# `put` and `ping` need python3 on the host (every lab PC has it; rehearse.sh
# installs it in the containers).
#
# PL_ANSIBLE_FAKE=<script> replaces ansible for tests: the script gets
# "sh|bg|put|ping" plus the resolved host list and arguments.

PL_REPO="${PL_REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}"
PL_STATE="${PL_STATE:-$HOME/.local/share/pipedpeer-lab}"
PL_VENV="${PL_VENV:-$PL_STATE/ansible}"
PL_INVENTORY="${PL_INVENTORY:-$PL_REPO/results/lab/inventory.ini}"

# pl_q: single-quote a string for any POSIX sh (printf %q may emit bash-only $'..').
pl_q() { local s=${1//\'/\'\\\'\'}; printf "'%s'" "$s"; }

pl_ansible_env() {
	export ANSIBLE_COLLECTIONS_PATH="$PL_VENV/collections"
	export ANSIBLE_HOST_KEY_CHECKING=False
	export ANSIBLE_PYTHON_INTERPRETER=auto_silent
	export ANSIBLE_DEPRECATION_WARNINGS=False
	export ANSIBLE_LOCALHOST_WARNING=False
	export ANSIBLE_FORKS="${ANSIBLE_FORKS:-64}"
	export ANSIBLE_TIMEOUT="${ANSIBLE_TIMEOUT:-20}"
	export ANSIBLE_SSH_ARGS="${ANSIBLE_SSH_ARGS:--o ControlMaster=auto -o ControlPersist=120s -o ServerAliveInterval=15}"
	export ANSIBLE_LOCAL_TEMP="$PL_STATE/ansible-tmp"
	mkdir -p "$ANSIBLE_LOCAL_TEMP"
}

pl_install() {
	mkdir -p "$PL_STATE"
	if [[ ! -x $PL_VENV/bin/ansible ]]; then
		if command -v uv >/dev/null; then
			uv venv -q "$PL_VENV"
		elif ! python3 -m venv "$PL_VENV" >/dev/null 2>&1; then
			# Stock Ubuntu has no python3-venv (no ensurepip); virtualenv.pyz needs neither.
			rm -rf "$PL_VENV"
			curl -fsSL --retry 3 https://bootstrap.pypa.io/virtualenv.pyz -o "$PL_STATE/virtualenv.pyz" &&
				python3 "$PL_STATE/virtualenv.pyz" -q "$PL_VENV" || { rm -f "$PL_STATE/virtualenv.pyz"; return 1; }
			rm -f "$PL_STATE/virtualenv.pyz"
		fi
	fi
	if command -v uv >/dev/null; then
		uv pip install -q --python "$PL_VENV/bin/python" ansible-core 'docker>=7' matplotlib
	else
		"$PL_VENV/bin/pip" install -q ansible-core 'docker>=7' matplotlib
	fi
	pl_ansible_env
	# The json callback _pl_adhoc reads lives in ansible.posix: ansible-core has none.
	local c
	for c in community.docker ansible.posix; do
		[[ -d $PL_VENV/collections/ansible_collections/${c/.//} ]] ||
			"$PL_VENV/bin/ansible-galaxy" collection install -p "$PL_VENV/collections" "$c" >/dev/null
	done
	"$PL_VENV/bin/ansible" --version | head -1
}

# pl_hosts <pattern>: the inventory hosts a pattern matches, one per line.
pl_hosts() {
	pl_ansible_env
	"$PL_VENV/bin/ansible" -i "$PL_INVENTORY" "$1" --list-hosts 2>/dev/null |
		sed -n 's/^ \{4,\}//p'
}

# _pl_adhoc <pattern> <module> <args>: run ad hoc with the json callback and
# print "<host>\t<rc>\t<base64 stdout>" per host.
_pl_adhoc() {
	pl_ansible_env
	ANSIBLE_LOAD_CALLBACK_PLUGINS=1 ANSIBLE_STDOUT_CALLBACK=json \
		"$PL_VENV/bin/ansible" -i "$PL_INVENTORY" "$1" -m "$2" -a "$3" 2>/dev/null |
		"$PL_VENV/bin/python" -c '
import base64, json, sys
doc = json.load(sys.stdin)
for play in doc.get("plays", []):
    for task in play.get("tasks", []):
        for host, r in task.get("hosts", {}).items():
            rc = r.get("rc", 1 if (r.get("failed") or r.get("unreachable")) else 0)
            out = r.get("stdout", "")
            if r.get("unreachable") or (r.get("failed") and not out):
                out = r.get("msg", "") + "\n" + r.get("stderr", "")
            print(host, rc, base64.b64encode(out.encode()).decode(), sep="\t")
'
}

pl_sh() {
	local pattern="$1" cmd="$2"
	if [[ -n ${PL_ANSIBLE_FAKE:-} ]]; then
		"$PL_ANSIBLE_FAKE" sh "$pattern" "$cmd"
		return
	fi
	# raw runs the string through the login shell; wrap it so it is POSIX sh
	# with a known PATH whatever the host's shell is.
	local wrapped
	wrapped="sh -c $(pl_q "export PATH=\$HOME/.local/bin:/usr/local/bin:\$PATH; $cmd") </dev/null"
	local lines n rc=0 host hrc out
	lines="$(_pl_adhoc "$pattern" raw "$wrapped")"
	n="$(printf '%s\n' "$lines" | grep -c . || true)"
	if [[ $n -eq 0 ]]; then
		echo "ansible: no host matched '$pattern' in $PL_INVENTORY" >&2
		return 2
	fi
	while IFS=$'\t' read -r host hrc out; do
		[[ -z $host ]] && continue
		if [[ $n -eq 1 ]]; then
			printf '%s' "$out" | base64 -d
		else
			printf '%s' "$out" | base64 -d | sed "s/^/$host: /"
		fi
		[[ $hrc -ne 0 && $rc -eq 0 ]] && rc=$hrc
	done <<<"$lines"
	return "$rc"
}

pl_bg() {
	local pattern="$1" log="$2" cmd="$3"
	if [[ -n ${PL_ANSIBLE_FAKE:-} ]]; then
		"$PL_ANSIBLE_FAKE" bg "$pattern" "$log" "$cmd"
		return
	fi
	# setsid: over ssh the raw module's pty session ends with the command, and
	# takes a plain nohup'd child with it (the Dask workers never started).
	pl_sh "$pattern" "mkdir -p \"\$(dirname $log)\"; if command -v setsid >/dev/null; then setsid -f sh -c $(pl_q "$cmd") >$log 2>&1 </dev/null; else nohup sh -c $(pl_q "$cmd") >$log 2>&1 </dev/null & fi; echo started"
}

pl_put() {
	local pattern="$1" src="$2" dest="$3"
	if [[ -n ${PL_ANSIBLE_FAKE:-} ]]; then
		"$PL_ANSIBLE_FAKE" put "$pattern" "$src" "$dest"
		return
	fi
	pl_ansible_env
	"$PL_VENV/bin/ansible" -i "$PL_INVENTORY" "$pattern" -o -m ansible.builtin.copy \
		-a "src=$src dest=$dest mode=preserve" >/dev/null
}

pl_ping() {
	if [[ -n ${PL_ANSIBLE_FAKE:-} ]]; then
		"$PL_ANSIBLE_FAKE" ping "${1:-all}"
		return
	fi
	pl_ansible_env
	"$PL_VENV/bin/ansible" -i "$PL_INVENTORY" "${1:-all}" -o -m ansible.builtin.ping
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
	set -uo pipefail
	verb="${1:-}"
	shift || true
	case "$verb" in
	install) pl_install ;;
	sh) pl_sh "$@" ;;
	bg) pl_bg "$@" ;;
	put) pl_put "$@" ;;
	ping) pl_ping "$@" ;;
	hosts) pl_hosts "${1:-all}" ;;
	*)
		echo "usage: lib/ansible.sh install|sh|bg|put|ping|hosts ..." >&2
		exit 2
		;;
	esac
fi
