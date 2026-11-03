# Paper and report source

Everything needed to produce the major project report, the conference paper and
the journal manuscript. The documents are written against commit `4c91ef3`
(2026-09-01), were re-measured and checked against the code on 2026-10-02,
extended for the address-book phase after the 2026-10-14 demo, and had every
number traced to its record on 2026-11-02 (`77d194e`).

## Layout

| Path | What it is |
|---|---|
| `system-report.md` | The content document. Its chapters match `report/main.tex` one to one, with file references for every non-obvious claim. Start here. |
| `report/main.tex` | Major project report, `report` class. |
| `report/be_title.tex`, `report/be_certificate2.tex` | Title page and approval certificate. |
| `paper/paper.tex` | Conference paper, IEEEtran two-column (6 pages). |
| `paper/journal/manuscript.tex`, `highlights.txt`, `cover-letter.md` | Journal manuscript, Elsevier `elsarticle`, submitted to JPDC on 3 November 2026 as JPDC-D-26-02273 (under review); highlights and cover letter as submitted. Each number cites its record in a `% source:` comment. |
| `presentation/PipedPeer-ESE.pptx` (+ `.pdf`) | ESE slide deck, 31 slides with speaker notes, built by `presentation/build_deck.py` (`uv run --with python-pptx --with pillow python presentation/build_deck.py`). |
| `diagrams/*.png` | 21 figures at 300 dpi, numbered in order of appearance. |
| `diagrams/src/` | The Graphviz and matplotlib sources that generate them. |
| `benchmarks/RESULTS.md` | Measured results. Each one is tagged with its source (October run, August run, or commit message), and the test bed and its limits are stated. |
| `benchmarks/bench.py` | The lab harness. It runs against whatever cluster is up. |
| `benchmarks/raw/` | Raw output behind every number quoted. `C_commit_measurements.md` quotes the multi-machine measurements verbatim from their commits. |
| `known-issues.md` | Defects found while documenting, each marked when resolved. |
| `fixes-and-roadmap.md` | What was fixed after the report, how to check each fix, and the phases that come next. |
| `next-steps.md` | Where the project stands after the address-book phase, what is unverified, and the decisions not to reopen. |
| `hosted-introducer.md`, `phone.md` | Deploy guides: an introducer-only server; an Android phone as a pool worker. |
| `adr/` | Why things are the way they are. |

## Rebuilding

```sh
# Figures
python3 diagrams/src/plots_design.py
python3 diagrams/src/plots_results.py
# Nix's graphviz ignores system font aliases; this maps Helvetica to Nimbus Sans.
export FONTCONFIG_FILE="$PWD/diagrams/src/fonts.conf"
for f in diagrams/src/*.dot; do
  out="diagrams/$(basename "${f%.dot}").png"
  dot -Tpng -Gdpi=300 -Gmargin=0.14 "$f" -o "$out"
  # Graphviz leaves the -Gmargin area transparent, which pdflatex can render
  # as black. Flatten every figure onto white.
  magick "$out" -background white -alpha remove -alpha off "$out"
done

# Documents (tectonic fetches the packages it needs and runs the passes itself;
# each PDF lands next to its .tex)
tectonic report/main.tex
tectonic paper/paper.tex
tectonic paper/journal/manuscript.tex
```

When tectonic's package server refuses (HTTP 429), use a full TeX Live:

```sh
nix shell nixpkgs#texliveFull -c sh -c '
  (cd report && pdflatex main && pdflatex main && pdflatex main)
  (cd paper && pdflatex paper && pdflatex paper)
  (cd paper/journal && pdflatex manuscript && pdflatex manuscript)'
```

Graphviz and ImageMagick come from `nix shell nixpkgs#graphviz nixpkgs#imagemagick`;
the plot scripts need python3 with matplotlib and numpy.

## Re-running the measurements

```sh
cd src && go build -o ../bin/pipedpeer . && cd ..   # bench.py uses bin/pipedpeer
(cd src && go test -v -count=1 ./...)               # needs pandas, joblib, sklearn and torch
scripts/check-skips.sh
scripts/audit-tests.sh                              # edits src/ temporarily; run nothing else meanwhile
bash scripts/lab-up.sh                              # 3 worker containers
./bin/pipedpeer status                              # the workers appear by LAN discovery
python3 docs/benchmarks/bench.py --repeat 5
scripts/bench-shim-d2.sh
BENCH_ITEMS=48 scripts/bench-pool.sh
scripts/lab-fail.sh                                 # rebuilds and then tears down the lab
python3 docs/diagrams/src/plots_results.py          # redraw the results figures
```

If your home directory is a git repository, set
`XDG_CACHE_HOME=/var/tmp/pipedpeer-cache` first (`known-issues.md` item 1).
With podman, `lab-up.sh` needs the base image under its full name: run
`podman pull docker.io/nixos/nix:2.31.3` once.

The harnesses run unchanged against real machines. Real machines are what the
results chapter says is needed for speedup figures.

## Three things to know before reading

**There are two earlier descriptions, and both are now wrong in places.**
- The May 2026 report describes a system that no longer exists.
- The August 2026 version of these documents predates:
  - the shared secret, TLS and node keys;
  - joining across networks;
  - signed closure transfer;
  - cloudpickle shipping;
  - measured work sharing;
  - daemon-side gradient averaging;
  - memory ceilings.

  It also described a trusted-LAN, no-authentication design.

`system-report.md` Appendix B tabulates both sets of differences.

**No speedup is claimed from the plain lab.** The plain lab is three containers
on one host, so it proves work crosses nodes, never speedup. Speedup numbers
come from the emulated lab (`scripts/lab-emu.sh`: equal containers with fixed
CPU and memory quotas, links shaped by tc netem to LAN, Wi-Fi and 5G profiles
plus loss; speedup = 1 container vs N of the same size) and are always labelled
*emulated*; real-hardware points come from two laptops over Tailscale and the
demo-day runs (`pipedpeer jobs --export-all`). What the plain lab measures honestly:
- the environment cache (23.41 s first, 0.83 s once the closure is present);
- interception start-up (1.24×);
- that work crosses nodes;
- the test suite (615/617 pass, 0 fail, and 95/95 mutations caught).

The multi-machine figures are hand measurements quoted from their commits, and
are labelled that way everywhere.

**One experiment failed and is reported as failed.** The fault-injection
harness did not distribute work on a fresh lab, so it tested nothing. See
`benchmarks/RESULTS.md` R5.
