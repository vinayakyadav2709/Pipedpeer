#!/usr/bin/env python3
"""The IEEE paper's evaluation figures, in one consistent style.

  paper_figures.py [--results results/lab] [--sample] [--draft] [--out DIR]
                   [--only NAME,...] [--list] [--docs]

Inputs are sample_figures.py's: runs.jsonl (plus, with --sample, the
rehearsal and more-bench lines in results/lab/sample/data/{rehearsal,more}),
OTel metrics and logs, logs/<run_id>/{record.json,receipt.json.gz,chunks.json},
port-lines.csv, timeline.jsonl (E14), energy/ (E16), machines.csv and
node-ids.csv. Defaults read the lab day: results/lab.

Outputs: <name>.pdf (vector, Type 42 fonts) and <name>.png (600 dpi) at the
exact IEEE width (3.5 in single column, 7.16 in double), in --out:
docs/paper/figures by default, docs/paper/figures-draft with --draft.
--draft stamps every figure "emulated" (and draws E16 from a synthetic
example when there are no joules); the final export never does either.
A figure whose data is absent is printed as skipped; skipped.txt lists them.
--docs also regenerates docs/paper/FIGURES.md and the preview's figure list
(docs/paper/figures-preview-body.tex) from the registry and
docs/diagrams/tikz/diagrams.json.
"""
import argparse
import os
import sys
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "lib"))

from paperfig import data, docs, style  # noqa: E402
from paperfig import figs_scaling, figs_faults, figs_cluster, figs_headline, figs_more  # noqa: E402,F401
from paperfig.registry import FIGS, Ctx, Skip  # noqa: E402

ROOT = data.ROOT


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--results", help="results directory (default results/lab)")
    p.add_argument("--sample", action="store_true", help="the container sample in results/lab/sample/data")
    p.add_argument("--draft", action="store_true", help="stamp figures 'emulated' (preview only)")
    p.add_argument("--out")
    p.add_argument("--only", default="", help="comma-separated figure names")
    p.add_argument("--list", action="store_true", help="list the figures and exit")
    p.add_argument("--docs", action="store_true", help="also write FIGURES.md and the preview body")
    a = p.parse_args(argv)
    if a.list:
        for f in FIGS:
            print("%-28s %-6s %.2f in  %s" % (f.name, f.width, f.height, f.section))
        return 0
    inp = data.sample_inputs() if a.sample else data.lab_inputs(a.results)
    if not os.path.exists(inp.runs):
        print("no %s: nothing to draw (use --sample for the container sample)" % inp.runs, file=sys.stderr)
        return 2
    out = a.out or os.path.join(ROOT, "docs", "paper", "figures-draft" if a.draft else "figures")
    lab = data.load(inp)
    ctx = Ctx(draft=a.draft, sample=inp.sample)
    only = {x for x in a.only.split(",") if x}
    made, skipped, failed, sizes = [], [], [], {}
    for f in FIGS:
        if only and f.name not in only:
            continue
        try:
            res = f.draw(lab, ctx)
            mark = None
            if isinstance(res, tuple):
                res, mark = res
            fsize = res.get_size_inches()
            assert abs(fsize[0] - style.WIDTH[f.width]) < 1e-6, "%s: width %.3f" % (f.name, fsize[0])
            style.save(res, out, f.name, draft=a.draft or mark is not None,
                       mark=style.synthetic_mark if mark == "synthetic" else None)
            made.append(f.name)
            sizes[f.name] = (round(fsize[0], 2), round(fsize[1], 2))
            print("made     %-28s %s %.2f x %.2f in" % (f.name, f.width, fsize[0], fsize[1]))
        except Skip as e:
            skipped.append((f.name, str(e)))
            print("skipped  %-28s %s" % (f.name, e))
        except Exception:  # one broken figure must not stop the rest
            failed.append(f.name)
            print("FAILED   %s\n%s" % (f.name, traceback.format_exc()), file=sys.stderr)
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "skipped.txt"), "w") as fh:
        for name, why in skipped:
            fh.write("%s\t%s\n" % (name, why))
        for name in failed:
            fh.write("%s\tFAILED (traceback on stderr)\n" % name)
    if a.docs:
        docs.write(FIGS, sizes, dict(skipped))
    print("%d made, %d skipped, %d failed -> %s" % (len(made), len(skipped), len(failed), os.path.relpath(out, ROOT)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
