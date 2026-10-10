#!/usr/bin/env bash
# Step 1 - three machines, three networks, one cluster.
source "$(dirname "$0")/_lib.sh"

banner "Step 1 - the cluster"
show "$PIPE" nodes
echo
note "Three rows, all healthy. HOST = three different machines."
note "PATH 'punched' / 'punched-in' = a direct link through both routers - no VPN, no port forwarding."
note "GPU column: the scheduler knows which machines can take GPU work."
note "The introducer only introduced them; it carries none of the traffic."
