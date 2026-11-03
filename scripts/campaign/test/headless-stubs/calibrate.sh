#!/usr/bin/env bash
# Rehearsal stand-in for scripts/campaign/calibrate.sh: small sizes, the same
# CALIBRATE line, so the day writes workloads.json and workloads.md for real.
set -euo pipefail
w=$1
case $w in
  W1) var=BENCH_ITEMS strong=240 weak='"weak_per_pc": {"BENCH_ITEMS": "40"}, "weak_target_s": 60.0,' ;;
  W2) var=CPU_ITEMS strong=8 weak="" ;;
  W3) var=RF_TREES strong=8 weak="" ;;
  W4) var=GS_SAMPLES strong=200 weak="" ;;
esac
echo "  $w $var=8: 1.00 s (best of 2)"
echo "CALIBRATE {\"workload\": \"$w\", \"host\": \"$(uname -n)\", \"nproc\": $(nproc), \"var\": \"$var\", \"c0_s\": 0.5, \"c1_s\": 0.01, \"points\": [[8, 0.6], [24, 0.74]], \"strong\": {\"$var\": \"$strong\"}, \"strong_target_s\": 125.0, $weak \"verify_s\": 120.0}"
