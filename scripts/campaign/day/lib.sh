# shellcheck shell=bash disable=SC2034  # the other day/ files use these
# Shared by lab-day.sh and day/*.sh: paths, the step ledger, progress, and
# the Ansible fan-out. Source it after setting CAMP (scripts/campaign).
#
# The ledger ($DAY/done) has one line per finished step; a step in it is
# never run again, which is what makes the day resumable. Everything a step
# does must be safe to repeat when it was cut off before its line was written.

REPO=$(cd "$CAMP/../.." && pwd)
DAY=${LAB_DAY_DIR:-$HOME/pipedpeer-lab}
RESULTS=$REPO/results/lab
INV=$RESULTS/inventory.ini        # live: PCs that die are dropped from it
INV_ALL=$DAY/inventory-all.ini    # every PC the day found, for cleanup
LEDGER=$DAY/done
PROGRESS=$DAY/progress.txt
HOSTREC=$DAY/hostrec              # per PC: what the day installed there
VENV=$HOME/.local/share/pipedpeer-lab/ansible
LABSH=/var/lib/pipedpeer-lab/lab.sh
DIST_PORT=8137
SESSION=${LAB_SESSION:-s1}        # lab.session on every telemetry record
mkdir -p "$DAY" "$HOSTREC" "$DAY/out" "$DAY/logs"

say() { printf '[day %(%H:%M:%S)T] %s\n' -1 "$*"; }

# SSH as the campaign: the decrypted key, a known_hosts of the day's own, no
# user ssh config. Exported so scan.sh, clocks.sh and run.sh's Ansible use it.
SSH_COMMON="-F /dev/null -o UserKnownHostsFile=$DAY/known_hosts -o StrictHostKeyChecking=no -o LogLevel=ERROR -o IdentitiesOnly=yes -i ${LAB_KEY:-/nonexistent}"
export LAB_SSH_OPTS="$SSH_COMMON"
export ANSIBLE_SSH_ARGS="$SSH_COMMON -o ControlMaster=auto -o ControlPersist=120s -o ServerAliveInterval=15"
export ANSIBLE_SSH_CONTROL_PATH_DIR=$DAY/cp
export ANSIBLE_HOST_KEY_CHECKING=False ANSIBLE_PYTHON_INTERPRETER=auto_silent
export ANSIBLE_DEPRECATION_WARNINGS=False ANSIBLE_LOCALHOST_WARNING=False ANSIBLE_SYSTEM_WARNINGS=False
export ANSIBLE_FORKS=64 ANSIBLE_TIMEOUT=20 ANSIBLE_LOCAL_TEMP=$DAY/ansible-tmp
export PIPEDPEER_LAB_HOME=$HOME/.local/share/pipedpeer-lab
export PATH=$HOME/.local/bin:/usr/local/bin:/nix/var/nix/profiles/default/bin:$HOME/.nix-profile/bin:$PATH

is_done() { grep -qxF -- "$1" "$LEDGER" 2>/dev/null; }
mark_done() { is_done "$1" || printf '%s\n' "$1" >>"$LEDGER"; }

# blocker <text>: one line in results/lab/BLOCKERS.md (and the log).
blocker() {
  say "BLOCKER: $*"
  mkdir -p "$RESULTS"
  [[ -f $RESULTS/BLOCKERS.md ]] || printf '# Blockers of the lab day\n\n' >"$RESULTS/BLOCKERS.md"
  printf -- '- %(%Y-%m-%d %H:%M:%S)T: %s\n' -1 "$*" >>"$RESULTS/BLOCKERS.md"
}

# progress <phase> <what>: the file `lab.sh status` prints, with an ETA from
# the estimates of the steps not yet done (STEPS: "name minutes" lines).
progress() {
  local phase=$1 what=$2 total=0 left=0 n=0 d=0 name min
  while read -r name min; do
    [[ -n $name ]] || continue
    n=$((n + 1)); total=$((total + min))
    if is_done "$name"; then d=$((d + 1)); else left=$((left + min)); fi
  done <<<"$STEPS"
  {
    printf 'lab day on %s, started %s\n' "$(uname -n)" "$(date -d "@$DAY_START" '+%H:%M')"
    printf 'phase:    %s\n' "$phase"
    printf 'now:      %s\n' "$what"
    printf 'progress: %d of %d steps done\n' "$d" "$n"
    printf 'ETA:      about %s (%d min left, estimated)\n' "$(date -d "+$left min" '+%H:%M')" "$left"
    printf 'updated:  %(%H:%M:%S)T\n' -1
    [[ -s $RESULTS/BLOCKERS.md ]] && printf 'problems so far: %d (results/lab/BLOCKERS.md)\n' "$(grep -c '^- ' "$RESULTS/BLOCKERS.md")"
  } >"$PROGRESS.new"
  mv -f "$PROGRESS.new" "$PROGRESS"
}

# hosts_of <group> [inventory]: pc ids in an inventory group, in order.
hosts_of() {
  [[ -f ${2:-$INV} ]] || return 0
  awk -v g="[$1]" '/^\[/{on=($0==g); next} on && /^pc[0-9]+ /{print $1}' "${2:-$INV}"
}
host_ip() { awk -v h="$1" '$1==h{for(i=2;i<=NF;i++) if($i ~ /^ansible_host=/){print substr($i,14); exit}}' "${2:-$INV_ALL}"; }
workers() { hosts_of workers "${1:-$INV}"; }

# drop_host <pc> <why>: take a PC out of the live inventory for the rest of
# the day (it stays in inventory-all.ini, so cleanup still tries it).
drop_host() {
  grep -q "^$1 " "$INV" || return 0
  sed -i "/^$1 /d" "$INV"
  [[ -f $RESULTS/dropped.csv ]] || echo "pc_id,time,step,reason" >"$RESULTS/dropped.csv"
  printf '%s,%(%Y-%m-%dT%H:%M:%S%z)T,%s,"%s"\n' "$1" -1 "${CUR_STEP:-}" "${2//\"/\'}" >>"$RESULTS/dropped.csv"
  blocker "$1 dropped from the day during ${CUR_STEP:-?}: $2"
}

# q: single-quote for any POSIX sh.
q() { local s=${1//\'/\'\\\'\'}; printf "'%s'" "$s"; }

# ans <tag> <pattern> <command> [inventory]: run command with sh on every
# matching host (raw: the host needs only sh), as the lab user. Per host,
# $DAY/out/<tag>/<host>.out holds stdout+stderr and <host>.rc the exit code
# (255: unreachable). Sets OK_HOSTS and BAD_HOSTS; returns 1 if any failed.
ans() {
  local tag=$1 pat=$2 cmd=$3 inv=${4:-$INV} dir=$DAY/out/$1 tree
  LAST_TAG=$tag
  rm -rf "$dir"; mkdir -p "$dir"; tree=$dir/.tree
  local wrapped
  wrapped="sh -c $(q "export PATH=\$HOME/.local/bin:/usr/local/bin:/nix/var/nix/profiles/default/bin:\$HOME/.nix-profile/bin:\$PATH; $cmd") </dev/null 2>&1"
  "$VENV/bin/ansible" -i "$inv" "$pat" -m raw -a "$wrapped" -t "$tree" >"$dir/.ansible.log" 2>&1 || true
  OK_HOSTS=() BAD_HOSTS=()
  local h rc
  while read -r h rc; do
    [[ -n $h ]] || continue
    if [[ $rc == 0 ]]; then OK_HOSTS+=("$h"); else BAD_HOSTS+=("$h"); fi
  done < <(python3 - "$tree" "$dir" "$("$VENV/bin/ansible" -i "$inv" "$pat" --list-hosts 2>/dev/null | sed -n 's/^ \{4,\}//p' | tr '\n' ' ')" <<'PY'
import json, os, re, sys
tree, out, hosts = sys.argv[1], sys.argv[2], sys.argv[3].split()
for h in hosts:
    try:
        r = json.load(open(os.path.join(tree, h)))
    except (OSError, ValueError):
        r = {"unreachable": True, "msg": "no result"}
    rc = 255 if r.get("unreachable") else r.get("rc", 1 if r.get("failed") else 0)
    # raw runs ssh with a pty, so lines end in CRLF.
    text = (r.get("stdout", "") + r.get("stderr", "")).replace("\r\n", "\n")
    # Terminal escapes: Fedora's sudo tags each session with OSC 3008 sequences.
    text = re.sub(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b\[[0-9;?]*[A-Za-z]", "", text)
    if r.get("unreachable") or (r.get("failed") and not text):
        text += r.get("msg", "") + "\n"
    open(os.path.join(out, h + ".out"), "w").write(text)
    open(os.path.join(out, h + ".rc"), "w").write("%d\n" % rc)
    print(h, rc)
PY
)
  if ((${#BAD_HOSTS[@]})); then
    say "$tag: ok on ${#OK_HOSTS[@]}, failed on ${BAD_HOSTS[*]}"
    return 1
  fi
  say "$tag: ok on ${#OK_HOSTS[@]}"
}
out_of() { cat "$DAY/out/$1/$2.out" 2>/dev/null; }
rc_of() { cat "$DAY/out/$1/$2.rc" 2>/dev/null || echo 255; }

# drop_bad <why>: drop every BAD_HOSTS worker (never pc00) after an ans call.
# Returns 1 when pc00 itself failed.
drop_bad() {
  local h head_bad=0
  for h in "${BAD_HOSTS[@]}"; do
    if [[ $h == pc00 ]]; then head_bad=1; continue; fi
    drop_host "$h" "$1 (rc $(rc_of "$LAST_TAG" "$h")): $(out_of "$LAST_TAG" "$h" | tail -n 3 | tr '\n' ' ' | cut -c1-300)"
  done
  return "$head_bad"
}
# ans_drop <tag> <pattern> <command> <why>: ans, then drop the workers it failed on.
ans_drop() { ans "$1" "$2" "$3" || drop_bad "$4"; }
