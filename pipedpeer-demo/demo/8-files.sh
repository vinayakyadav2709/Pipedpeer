#!/usr/bin/env bash
# Step 8 - the job runs elsewhere; its file changes come back here.
source "$(dirname "$0")/_lib.sh"

banner "Step 8 - files come back"
# Fixtures, so the create / modify / delete are all visible every time.
echo "original line" >sync_note.txt
echo "this file will be deleted by the job" >delete_me.txt
rm -f created_by_job.txt
note "Before:"
typed ls -l sync_note.txt delete_me.txt created_by_job.txt
ls -l sync_note.txt delete_me.txt created_by_job.txt 2>&1 | sed 's/^/    /'
pause
typed pipedpeer run 05_file_sync.py --remote
"$PIPE" run 05_file_sync.py --remote 2>&1 | grep -E 'SYNC|Synced|completed'
echo
note "After - created, modified, deleted, all on THIS machine, by a job that ran on another:"
typed ls -l sync_note.txt delete_me.txt created_by_job.txt
ls -l sync_note.txt delete_me.txt created_by_job.txt 2>&1 | sed 's/^/    /'
show cat sync_note.txt
