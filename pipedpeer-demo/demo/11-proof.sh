#!/usr/bin/env bash
# Step 11 - the receipts: where the last run's time and work went, and what
# each other machine says it ran for somebody else.
source "$(dirname "$0")/_lib.sh"

banner "Step 11 - where the work actually ran"
show "$PIPE" jobs 2>/dev/null | head -8
pause
note "The last run, stage by stage: build, upload, seeding the peers, the script, the sync."
show "$PIPE" job --last --timeline
pause
note "What the shim sent to which machine, what it kept here, and the cost model's reason for each."
# The per-dispatch table runs to dozens of rows for a forest; the summary,
# per-peer counts and decisions are what the room can read.
typed pipedpeer job --last --receipt
"$PIPE" job --last --receipt | awk '/^Dispatches:/ {skip = 1; next} skip && /^$/ {skip = 0; next} !skip'
echo
note "That is this laptop's account. Now run this on box-b and box-c (not here):"
note "    pipedpeer traffic"
note "It is the RECEIVING machine's record of work it did for box-a, so box-a cannot fake it."
