# Paper and report source

Everything needed to produce the major project report and the conference paper.

## Layout

| Path | What it is |
|---|---|
| `system-report.md` | The content document. Chaptered to match `report/main.tex` one to one, with file references for every non-obvious claim. Start here. |
| `report/main.tex` | Major project report, `report` class. Compiles to a 51 page PDF. |
| `report/be_title.tex`, `report/be_certificate2.tex` | Title page and approval certificate. |
| `paper/paper.tex` | Conference paper, IEEEtran two-column. Compiles to 4 pages. |
| `diagrams/*.png` | 19 figures at 300 dpi. |
| `diagrams/src/` | The Graphviz and matplotlib sources that generate them. |
| `benchmarks/RESULTS.md` | Measured results, with the test bed and its limits stated. |
| `benchmarks/bench.py` | The harness. Runs against whatever cluster is up. |
| `benchmarks/raw/` | Raw output behind every number quoted. |

## Rebuilding

```sh
# Figures
python3 diagrams/src/plots_design.py
python3 diagrams/src/plots_results.py
for f in diagrams/src/*.dot; do
  dot -Tpng -Gdpi=300 "$f" -o "diagrams/$(basename "${f%.dot}").png"
done

# Documents (three passes, for the table of contents and cross-references)
cd report && pdflatex main.tex && pdflatex main.tex && pdflatex main.tex
cd ../paper && pdflatex paper.tex && pdflatex paper.tex
```

`report/main.tex` needs `enumitem`, `multirow` and `tocloft`; `paper/paper.tex`
needs the Times fonts that IEEEtran expects (`psnfss` plus the URW base35 Type 1
outlines). Neither is unusual in a full TeX Live installation.

## Re-running the measurements

```sh
bash scripts/lab-up.sh              # 3 worker containers
./bin/pipedpeer nodes add 127.0.0.1 38081   # and 38082, 38083
python3 docs/benchmarks/bench.py --repeat 5
python3 docs/diagrams/src/plots_results.py  # redraw the results figures
bash scripts/lab-down.sh
```

The harness is unchanged when pointed at real machines instead of containers,
which is what the results chapter says is needed to obtain speedup figures.

## Two things to know before reading

**The previous report describes a system that no longer exists.** It documents
bubblewrap sandboxing, dispatch over a message bus, and a task queue package
that has been deleted, and it lists the interception layer as an unimplemented
limitation. Appendix B of `system-report.md` tabulates the differences. Its
results chapter was entirely unfilled placeholders.

**No speedup figure is claimed anywhere.** The measurements were taken with all
three workers as containers on one host, which cannot demonstrate multi-machine
speedup. What that bed does measure honestly, it measures: the environment cache
(12.00 s cold against 0.66 s warm), interception overhead, and correct
completion after a worker is killed mid-run. What is missing is listed rather
than estimated.
