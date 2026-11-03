#!/usr/bin/env bash
# How results/lab/sample/data was produced: the real runner (run.sh) on the
# 3-container lab that `LAB_NAME=sample LAB_MEM=1g scripts/campaign/rehearse.sh
# --keep --only e7` left up, with the driver collector (telemetry.sh up) and a
# node collector in each container (otel/node.sh start pc0N
# host.containers.internal). Sizes: workloads-sample.json (smoke = sample size).
# Emulated, container lab, sample sizes; not a result.
set -uo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
export PL_RESULTS="$here/data" PL_NPROC=2 PL_WORKLOADS="$here/workloads-sample.json"
inv="$repo/results/lab/inventory-rehearsal.ini"
run() { echo "=== $* $(date +%T)"; printf '\n' | "$repo/scripts/campaign/run.sh" "$@" --rehearsal --inventory "$inv" --seed 7; echo "=== rc=$? $(date +%T)"; }
for e in ${SAMPLE_ONLY:-e1 e1b e4 e6 e7 e8}; do
	case $e in
	e1) run e1 --n 1,2 --repeats 3 --workloads "${SAMPLE_E1_WORKLOADS:-W1,W2,W3}" ;;
	e1b) run e1b --n 1,2 --repeats 3 ;;
	e4) run e4 --n 2 --repeats 3 ;;
	e6) run e6 --n 2 --repeats 1 ;;
	e7) run e7 --repeats 3 ;;
	e8) run e8 --n 2 --repeats 1 ;;
	esac
done
