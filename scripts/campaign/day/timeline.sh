# shellcheck shell=bash
# E14, curl to cluster: when each PC got through each step of the day, as one
# JSON line per (PC, event) in results/lab/timeline.jsonl. Sourced by
# lab-day.sh after day/lib.sh.
#
#   {"pc": "pc03", "event": "install_end", "t_ns": 1793..., "clock": "pc", "machine_id": "...", "source": "..."}
#
# clock "pc" is that PC's own clock, "pc00" is pc00's (clocks.txt has the
# offsets; chrony is set where they exceeded 10 ms). A resumed day may write
# an event twice; analyse_headline.py takes the first per (machine, event).
#
# Events, in the order of a PC's day:
#   head_start    pc00 only: `lab.sh head` began (lab.sh stamp)
#   setup_start   lab.sh began on the PC (lab.sh stamp)
#   labpc         the LAB-PC line printed: the PC is set up (lab.sh stamp)
#   ssh_ok        the first discover scan that logged in with the campaign
#                 key; pc00's clock, at most one scan (15 s + the scan) late.
#                 Keyed by machine_id: pc numbers settle only when discover ends
#   pc_map        machine_id and ip -> the final pc id, once per PC
#   install_start, install_end   install-pipedpeer.sh on the PC
#   join_end      `pipedpeer connect` returned on the PC
#   cluster_ready pc00: its /v1/nodes lists every worker healthy (source: n)
#   first_work    the first compute step of the first distributed job that
#                 ran on the PC (experiments/e14.py, from the shim's receipt)
TIMELINE=$RESULTS/timeline.jsonl

# tl <pc> <event> <t_ns> <clock> [machine_id] [source]
tl() {
  printf '{"pc": "%s", "event": "%s", "t_ns": %s, "clock": "%s", "machine_id": "%s", "source": "%s"}\n' \
    "$1" "$2" "$3" "$4" "${5:-}" "${6//\"/\'}" >>"$TIMELINE"
}

# tl_wrap <event> <command>: the command, timed on the PC; its output ends in
# "PLTL <event> <start ns> <end ns> <rc>" and it exits with the command's rc.
tl_wrap() {
  printf 's=$(date +%%s%%N); %s; rc=$?; echo "PLTL %s $s $(date +%%s%%N) $rc"; exit $rc' "$2" "$1"
}

# tl_from_out <tag> <event>: the PLTL lines of an ans call, as <event>_start
# and <event>_end per PC.
tl_from_out() {
  local h line
  for h in $(hosts_of head "$INV") $(workers); do
    line=$(out_of "$1" "$h" | grep "^PLTL $2 " | tail -n1)
    [[ -n $line ]] || continue
    read -r _ _ s e rc <<<"$line"
    tl "$h" "$2_start" "$s" pc "" "rc=$rc"
    tl "$h" "$2_end" "$e" pc "" "rc=$rc"
  done
}

# tl_seen: after a discover scan, ssh_ok for every machine not seen before.
tl_seen() {
  local now id mid ip; now=$(date +%s%N)
  while read -r id rest; do
    mid=$(grep -o 'machine_id=[^ ]*' <<<"$rest" | cut -d= -f2)
    ip=$(grep -o 'ansible_host=[^ ]*' <<<"$rest" | cut -d= -f2)
    [[ -n $mid ]] || continue
    grep -q "\"event\": \"ssh_ok\", .*\"machine_id\": \"$mid\"" "$TIMELINE" 2>/dev/null && continue
    tl "$id" ssh_ok "$now" pc00 "$mid" "ip=$ip"
  done < <(grep '^pc[0-9]* ' "$INV" 2>/dev/null)
}

# tl_collect: the pc map and lab.sh's stamps from every PC, once the pc ids
# are final (after discover).
tl_collect() {
  local h line mid
  ans tl.times all 'echo "PLMID $(cat /etc/machine-id)"; sudo -n cat /var/lib/pipedpeer-lab/times 2>/dev/null; true' || true
  for h in $(hosts_of head "$INV") $(workers); do
    mid=$(out_of tl.times "$h" | grep '^PLMID ' | cut -d' ' -f2 | tr -d '\r')
    tl "$h" pc_map 0 pc "$mid" "ip=$(host_ip "$h" "$INV")"
    while read -r ev t; do
      [[ $ev =~ ^(head_start|setup_start|labpc)$ && $t =~ ^[0-9]+$ ]] && tl "$h" "$ev" "$t" pc "$mid" lab.sh
    done < <(out_of tl.times "$h" | tr -d '\r')
  done
}

# prep_firstjob: the first distributed job, right after the join, through
# run.sh (experiments/e14.py), so the day records when the cluster first ran
# work on every PC. It also builds the job's Nix closure that E1's first
# pipedpeer run would otherwise build. A failure is recorded, never fatal.
prep_firstjob() {
  tl_collect
  [[ -f $CAMP/run.sh ]] || { blocker "e14: run.sh missing; no first job"; return 0; }
  (cd "$REPO" && bash "$CAMP/run.sh" e14 --inventory "$INV") 2>&1 | tee "$RESULTS/logs/day/e14.log"
  ((PIPESTATUS[0] == 0)) || blocker "e14 (first distributed job) failed (results/lab/logs/day/e14.log)"
  return 0
}
