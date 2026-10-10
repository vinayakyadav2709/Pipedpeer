# Paper and report source

Everything needed to produce the major project report and the conference paper.
The documents are written against commit `4c91ef3` (2026-09-01) and were
re-measured and checked against the code on 2026-10-02.

## Layout

| Path | What it is |
|---|---|
| `system-report.md` | The content document. Its chapters match `report/main.tex` one to one, with file references for every non-obvious claim. Start here. |
| `report/main.tex` | Major project report, `report` class. |
| `report/be_title.tex`, `report/be_certificate2.tex` | Title page and approval certificate. |
| `paper/paper.tex` | Conference paper, IEEEtran two-column. |
| `diagrams/*.png` | 21 figures at 300 dpi, numbered in order of appearance. |
| `diagrams/src/` | The Graphviz and matplotlib sources that generate them. |
| `benchmarks/RESULTS.md` | Measured results. Each one is tagged with its source (October run, August run, or commit message), and the test bed and its limits are stated. |
| `benchmarks/bench.py` | The lab harness. It runs against whatever cluster is up. |
| `benchmarks/raw/` | Raw output behind every number quoted. `C_commit_measurements.md` quotes the multi-machine measurements verbatim from their commits. |
| `known-issues.md` | Defects found while documenting, recorded rather than fixed. |

## Rebuilding

```sh
# Figures
python3 diagrams/src/plots_design.py
python3 diagrams/src/plots_results.py
for f in diagrams/src/*.dot; do
  out="diagrams/$(basename "${f%.dot}").png"
  dot -Tpng -Gdpi=300 -Gmargin=0.14 "$f" -o "$out"
  # Graphviz leaves the -Gmargin area transparent, which pdflatex can render
  # as black. Flatten every figure onto white.
  magick "$out" -background white -alpha remove -alpha off "$out"
done

# Documents (three passes, for the table of contents and cross-references)
cd report && pdflatex main.tex && pdflatex main.tex && pdflatex main.tex
cd ../paper && pdflatex paper.tex && pdflatex paper.tex
```

You need a full TeX Live. `report/main.tex` uses `enumitem`, `multirow` and
`tocloft`, and the paper needs IEEEtran's Times fonts. Without a full
installation, prefix the commands with
`nix shell nixpkgs#texliveFull nixpkgs#graphviz -c`.

## Re-running the measurements

```sh
cd src && go build -o ../bin/pipedpeer . && cd ..   # bench.py uses bin/pipedpeer
(cd src && go test -v -count=1 ./...)               # needs pandas, joblib, sklearn and torch
scripts/check-skips.sh
scripts/audit-tests.sh                              # edits src/ temporarily; run nothing else meanwhile
bash scripts/lab-up.sh                              # 3 worker containers
for p in 38081 38082 38083; do ./bin/pipedpeer nodes add 127.0.0.1 $p; done
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

**No speedup is claimed from the lab.** The lab is three containers on one host.
What it measures honestly:
- the environment cache (23.41 s first, 0.83 s once the closure is present);
- interception start-up (1.24×);
- that work crosses nodes;
- the test suite (615/617 pass, 0 fail, and 95/95 mutations caught).

The multi-machine figures are hand measurements quoted from their commits, and
are labelled that way everywhere.

**One experiment failed and is reported as failed.** The fault-injection
harness did not distribute work on a fresh lab, so it tested nothing. See
`benchmarks/RESULTS.md` R5.
