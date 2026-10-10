#!/usr/bin/env bash
# Step 9 - pull a machine out mid-job; the answer is still right.
source "$(dirname "$0")/_lib.sh"

banner "Step 9 - kill a machine mid-job"
note "Expected checksum (from step 2):  checksum 4319999744"
note "When the remote batches start ('remote ok' lines), on box-c run:   pipedpeer stop"
pause
typed pipedpeer run 00_pool.py
"$PIPE" run 00_pool.py 2>&1 | grep -E 'remote ok|remote fail|retry|resend|local [0-9]+ items|checksum|TOTAL'
echo
note "Same checksum as the undisturbed run."
note "A 'remote failed ... local covers it' line is a batch box-c was holding when it died - redone by the machines still up."
note "No such line: box-c had no batch in flight at that moment; it simply stopped being offered work."
note "Now bring box-c back - on box-c:   pipedpeer start      (it rejoins the same cluster, no arguments)"
