#!/usr/bin/env bash
# Step 2 of the run sheet - every machine in the room, one cluster (LAPTOP-A).
source "$(dirname "$0")/_lib.sh"

banner "The cluster"
show "$PIPE" nodes
echo
note "One row per machine, all 'healthy'. The '*' row is this laptop."
note "CPU = cores and how busy they are now; GPU = card, load and memory in use."
note "That is what the scheduler weighs when it places work."
note "PATH 'lan' = a direct link on this network. No VPN, no cluster manager."
note "LAPTOP-A only introduced the machines (rendezvous); it carries none of their traffic."
