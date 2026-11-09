#!/usr/bin/env bash
# collect.sh: gathers everything needed to update the paper into a single folder
# for upload to Google Drive and offline processing.
#
# collect.sh --bug: one folder with everything needed to debug a failed run
# (default ~/Documents/pipedpeer/bug-<time>): the lab dir, the driver's tool
# versions, and from every PC in hosts.tsv its versions, status, daemon and
# Ray/Dask journals, warm-up log, Ray's raylet.out/gcs_server.out, memory
# evidence (cgroup v1 and v2 files, PSI, kernel OOM lines) and disk. It works
# without runs.jsonl and when analyse.py fails. Secrets stay out: private keys,
# r2.env and cluster.json are left out, invite codes and token=/secret= values
# are redacted, and SUMMARY.txt names what was left out or not reached.
set -euo pipefail

usage() {
	cat <<-EOF
		usage: collect.sh [--to DIR]

		Options:
		  --to DIR    Target directory for bundle (default: ~/Documents/pipedpeer/lab-bundle,
		              or ~/Documents/pipedpeer/bug-<time> with --bug)
		  --bug       Bug report: logs, versions and memory evidence from the driver
		              and every PC, secrets left out; send the folder, do not fix
	EOF
}

TO_DIR=""
BUG=0

while [[ $# -gt 0 ]]; do
	case "$1" in
	--to)
		if [[ $# -lt 2 ]]; then
			echo "Error: --to requires an argument" >&2
			exit 1
		fi
		TO_DIR="$2"
		shift 2
		;;
	--bug)
		BUG=1
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		echo "Unknown argument: $1" >&2
		usage >&2
		exit 1
		;;
	esac
done

REPO="$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)" || {
	echo "collect.sh: run this from a Pipedpeer checkout" >&2
	exit 1
}
LAB="${LAB_RESULTS:-$REPO/results/lab}"

if ((BUG)); then
	# shellcheck source=/dev/null
	source "$REPO/scripts/campaign/collect-bug.sh"
	collect_bug "${TO_DIR:-$HOME/Documents/pipedpeer/bug-$(date +%Y%m%d-%H%M%S)}"
	exit $?
fi
TO_DIR="${TO_DIR:-$HOME/Documents/pipedpeer/lab-bundle}"

# 1. Require runs.jsonl under results directory; create no folder if missing
if [[ ! -f "$LAB/runs.jsonl" ]]; then
	echo "Error: no runs.jsonl found under $LAB" >&2
	exit 1
fi

# 2. Refuse if target DIR exists and is non-empty
if [[ -d "$TO_DIR" ]] && [[ -n "$(ls -A "$TO_DIR" 2>/dev/null)" ]]; then
	echo "Error: target directory '$TO_DIR' exists and is not empty" >&2
	exit 1
fi

mkdir -p "$TO_DIR"

# 3. Copy results/lab
mkdir -p "$TO_DIR/results/lab"
if [[ -d "$LAB" ]]; then
	cp -a "$LAB"/. "$TO_DIR/results/lab/"
fi

# 4. Copy docs and scripts
mkdir -p "$TO_DIR/docs"
if [[ -d "$REPO/docs/paper" ]]; then
	cp -a "$REPO/docs/paper" "$TO_DIR/docs/"
fi
if [[ -d "$REPO/docs/report" ]]; then
	cp -a "$REPO/docs/report" "$TO_DIR/docs/"
fi
mkdir -p "$TO_DIR/docs/benchmarks"
if [[ -f "$REPO/docs/benchmarks/campaign-cases.md" ]]; then
	cp "$REPO/docs/benchmarks/campaign-cases.md" "$TO_DIR/docs/benchmarks/"
fi

if [[ -d "$REPO/docs/diagrams" ]]; then
	cp -a "$REPO/docs/diagrams" "$TO_DIR/docs/"
fi

mkdir -p "$TO_DIR/scripts/campaign"
if [[ -f "$REPO/scripts/campaign/experiments.tsv" ]]; then
	cp "$REPO/scripts/campaign/experiments.tsv" "$TO_DIR/scripts/campaign/"
fi
# The scripts that made the tables, so the offline paper build can redo them.
for f in "$REPO"/scripts/campaign/analyse*.py; do
	[[ -f "$f" ]] && cp "$f" "$TO_DIR/scripts/campaign/"
done

if [[ -f "$REPO/scripts/campaign/PAPER-UPDATE.md" ]]; then
	cp "$REPO/scripts/campaign/PAPER-UPDATE.md" "$TO_DIR/PAPER-UPDATE.md"
elif [[ -f "$REPO/PAPER-UPDATE.md" ]]; then
	cp "$REPO/PAPER-UPDATE.md" "$TO_DIR/PAPER-UPDATE.md"
fi

if [[ -f "$REPO/scripts/campaign/wipe.sh" ]]; then
	cp "$REPO/scripts/campaign/wipe.sh" "$TO_DIR/wipe.sh"
	chmod +x "$TO_DIR/wipe.sh"
fi

# 5. Record git source state (commit ID and git status --short)
if ! git -C "$REPO" rev-parse HEAD >"$TO_DIR/source.txt"; then
	echo "collect.sh: warning: could not read the commit of $REPO" >&2
	echo "unknown-commit" >"$TO_DIR/source.txt"
fi
git -C "$REPO" status --short >>"$TO_DIR/source.txt" ||
	echo "collect.sh: warning: could not record git status of $REPO" >&2

# 6. Run analyse.py
mkdir -p "$TO_DIR/figures"
analysis_tmp="$(mktemp)"
analysis_rc=0
python3 "$REPO/scripts/campaign/analyse.py" --lab "$LAB" >"$analysis_tmp" 2>&1 || analysis_rc=$?

if ((analysis_rc != 0)); then
	err_msg="$(cat "$analysis_tmp")"
	rm -f "$analysis_tmp"
	echo "ANALYSIS-FAILED: analyse.py exited $analysis_rc: $err_msg" >&2
	echo "The results were copied to $TO_DIR, but it has no MANIFEST.sha256, so wipe.sh will refuse it." >&2
	echo "Fix the cause, delete $TO_DIR, and run collect.sh again; the originals stay in $LAB." >&2
	exit 1
fi

mv "$analysis_tmp" "$TO_DIR/analysis.txt"
# analyse.py writes analysis/ after step 3 copied the lab dir: copy it now.
if [[ -d "$LAB/analysis" ]]; then
	mkdir -p "$TO_DIR/results/lab/analysis"
	cp -a "$LAB/analysis"/. "$TO_DIR/results/lab/analysis/" || {
		echo "collect.sh: could not copy $LAB/analysis; no manifest written, so the bundle is unfinished" >&2
		exit 1
	}
else
	echo "collect.sh: warning: analyse.py left no tables in $LAB/analysis" >&2
fi
if [[ -d "$LAB/figures" ]]; then
	cp -a "$LAB/figures"/. "$TO_DIR/figures/" || {
		echo "collect.sh: could not copy figures from $LAB/figures; no manifest written, so the bundle is unfinished" >&2
		exit 1
	}
else
	echo "collect.sh: warning: analyse.py left no figures in $LAB/figures" >&2
fi

# 7. Write MANIFEST.sha256 over every file in the bundle
(
	cd "$TO_DIR"
	# shellcheck disable=SC2094 # the manifest is excluded from what it lists
	find . -type f ! -name MANIFEST.sha256 -print0 | LC_ALL=C sort -z | xargs -0 sha256sum >MANIFEST.sha256
)

# 8. Print instructions
echo "Collected lab bundle to: $TO_DIR"
echo "Please upload this whole folder to Google Drive."
