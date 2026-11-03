#!/usr/bin/env bash
# Run one lab-campaign experiment and append one JSON line per run to
# results/lab/runs.jsonl (logs in results/lab/logs/<run_id>/):
#
#   scripts/campaign/run.sh <experiment> [--rehearsal] [--inventory FILE]
#                           [--n 1,2] [--repeats K] [--seed S]
#                           [--size W1:BENCH_ITEMS=48] [--workloads W1,W2]
#
# Experiments are scripts/campaign/experiments/<name>.py: e1 e1b e3 e4 e5 e6
# e7 e8 e9 e10 e11. Every PC is reached through lib/ansible.sh; the first run
# installs Ansible in $HOME/.local/share/pipedpeer-lab/ansible. README.md
# beside this file has the runs.jsonl schema and how each system is held to N.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
venv="${PL_VENV:-$HOME/.local/share/pipedpeer-lab/ansible}"
if [[ -z ${PL_ANSIBLE_FAKE:-} && ! -x $venv/bin/ansible ]]; then
	bash "$here/lib/ansible.sh" install >&2
fi
py="$venv/bin/python"
[[ -x $py ]] || py=python3
PYTHONPATH="$here/lib${PYTHONPATH:+:$PYTHONPATH}" exec "$py" -c \
	'import sys; from campaign.runner import main; sys.exit(main())' "$@"
