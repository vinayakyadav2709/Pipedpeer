#!/usr/bin/env bash
# Step 10 - the receipts: what each machine says it ran for somebody else.
source "$(dirname "$0")/_lib.sh"

banner "Step 10 - where the work actually ran"
show "$PIPE" jobs 2>/dev/null | head -12
echo
note "Run this on box-b and box-c (not here): their own ledger of work received from box-a."
note "    pipedpeer traffic"
note "It is the RECEIVING machine's record, so box-a cannot fake it."
