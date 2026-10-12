#!/usr/bin/env bash
# Failover - pull a machine out mid-job; the answer is still right (LAPTOP-A).
source "$(dirname "$0")/_lib.sh"

banner "Kill a machine mid-job"
note "Expected checksum (from the headline step):  checksum 4319999744"
note "When the 'remote ok' lines start, on PC-3 run:   pipedpeer stop"
note "Keep the dashboard / 'pipedpeer events -f' in view: PC-3 goes away there."
pause
typed pipedpeer run 00_pool.py
"$PIPE" run 00_pool.py 2>&1 | grep -E 'remote ok|remote fail|retry|resend|local [0-9]+ items|checksum|TOTAL|spread|^ *total'
echo
note "Same checksum as the undisturbed run."
note "A 'remote failed ... local covers it' line is a batch PC-3 was holding when it died - redone by the machines still up."
note "No such line: PC-3 had no batch in flight at that moment; it simply stopped being offered work."
note "Now bring PC-3 back - on PC-3:   pipedpeer start      (it rejoins the same cluster, no arguments)"
