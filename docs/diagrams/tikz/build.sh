#!/usr/bin/env bash
# Build every diagram: <name>.tex -> pdf/<name>.pdf -> png/<name>.png (600 dpi).
# Usage: build.sh [name ...]   (default: every *.tex but ppstyle.tex)
# Fails when a font is not embedded or is Type 3, or a figure is wider than
# its column (IEEEtran: \columnwidth 252pt, \textwidth 516pt; widths from
# diagrams.json). pdfinfo reports big points: 252pt = 250.9 bp, 516pt = 513.9 bp.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd "$here"
tectonic=${TECTONIC:-$(command -v tectonic || echo "$HOME/.local/bin/tectonic")}
work=$(mktemp -d)
trap 'rm -r -- "$work"' EXIT
mkdir -p pdf png

if (($#)); then names=("$@"); else
  names=()
  for f in *.tex; do [[ $f == ppstyle.tex ]] || names+=("${f%.tex}"); done
fi

fail=0
for n in "${names[@]}"; do
  cp "$n.tex" ppstyle.tex "$work/"
  if ! (cd "$work" && "$tectonic" -X compile --outdir "$work" "$n.tex" >"$work/$n.out" 2>&1); then
    echo "FAIL $n: tectonic"; grep -E 'error|PP:' "$work/$n.out" | head -5; fail=1; continue
  fi
  cp "$work/$n.pdf" "pdf/$n.pdf"
  pdftoppm -r 600 -png -singlefile "pdf/$n.pdf" "png/$n"

  size=$(pdfinfo "pdf/$n.pdf" | awk '/Page size/ {printf "%.4f x %.4f", $3/72, $5/72}')
  fonts=$(pdffonts "pdf/$n.pdf" | tail -n +3)
  if grep -q 'Type 3' <<<"$fonts" || awk '$(NF-4)!="yes"{bad=1} END{exit !bad}' <<<"$fonts"; then
    echo "FAIL $n: fonts"; echo "$fonts"; fail=1
  fi
  want=$(python3 -c 'import json,sys; d={e["name"]:e["width"] for e in json.load(open("diagrams.json"))}; print({"single":252/72.27,"double":516/72.27}.get(d.get(sys.argv[1],""),0))' "$n" 2>/dev/null || echo 0)
  w=${size%% *}
  if [[ $want != 0 ]] && ! python3 -c "import sys; sys.exit(not ($w <= $want + 0.002 and $w >= $want - 0.15))"; then
    echo "FAIL $n: width $w in, column is $want in"; fail=1
  fi
  pt=$(pdfinfo "pdf/$n.pdf" | awk '/Page size/ {printf "%.1f x %.1f", $3*72.27/72, $5*72.27/72}')
  echo "ok   $n  $size in  ($pt TeX pt)"
done
exit $fail
