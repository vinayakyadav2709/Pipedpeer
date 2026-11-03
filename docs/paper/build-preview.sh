#!/usr/bin/env bash
# Regenerate every paper figure and compile the preview document.
#
#   docs/paper/build-preview.sh            the container sample, stamped "emulated"
#   docs/paper/build-preview.sh --lab      the lab day (results/lab), no stamp
#
# Writes docs/paper/figures-draft/ (or figures/ with --lab), the TikZ diagram
# PDFs, docs/paper/FIGURES.md, figures-preview.pdf, and one PNG per page in
# docs/paper/preview-pages/. Needs python3 with matplotlib, tectonic and
# pdftoppm (poppler).
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"

if [[ -x "$root/docs/diagrams/tikz/build.sh" ]]; then
  "$root/docs/diagrams/tikz/build.sh"
fi
if [[ "${1:-}" == "--lab" ]]; then
  python3 "$root/scripts/campaign/paper_figures.py" --out "$root/docs/paper/figures-draft" --docs
  python3 "$root/scripts/campaign/paper_figures.py"
else
  python3 "$root/scripts/campaign/paper_figures.py" --sample --draft --docs
fi

build="$(mktemp -d)"
trap 'rm -rf -- "$build"' EXIT
( cd "$here" && tectonic --keep-logs --outdir "$build" figures-preview.tex >"$build/tectonic.out" 2>&1 ) \
  || { cat "$build/tectonic.out"; exit 1; }
grep -E "Overfull|Underfull \\\\hbox|Warning" "$build/figures-preview.log" | grep -v "Font shape" || true
cp "$build/figures-preview.pdf" "$here/figures-preview.pdf"

mkdir -p "$here/preview-pages"
find "$here/preview-pages" -maxdepth 1 -name 'page-*.png' -delete
pdftoppm -r 150 -png "$here/figures-preview.pdf" "$here/preview-pages/page"
echo "docs/paper/figures-preview.pdf: $(pdfinfo "$here/figures-preview.pdf" | awk '/^Pages/ {print $2}') pages"
