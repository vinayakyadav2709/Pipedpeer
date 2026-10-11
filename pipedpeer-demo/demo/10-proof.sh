#!/usr/bin/env bash
# Receipts - what each machine says it ran, and for whom (any machine).
source "$(dirname "$0")/_lib.sh"

banner "Where the work actually ran"
typed pipedpeer jobs
"$PIPE" jobs 2>/dev/null | head -12
echo
typed pipedpeer events --limit 15
"$PIPE" events --limit 15
echo
note "Now on a PC that took work (PC-2):"
note "    pipedpeer traffic      # its own ledger of pool work received, per sender"
note "    pipedpeer events       # who joined, who sent what, how long it took"
note "It is the RECEIVING machine's record, so the sender cannot fake it."
