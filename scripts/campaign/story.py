#!/usr/bin/env python3
"""The lab campaign as one scrolling page: results/lab/story/index.html.
One self-contained file (data, d3, styles and scripts inlined) that opens
offline from file://. With --export it also renders, in headless Chromium,
a screenshot per section (screens/), each chart as SVG, PNG and PDF in the
paper's light look and the slides' dark look (export/), and the hero
replay as a 1080p MP4 and a GIF.

  story.py                      the lab day: results/lab/runs.jsonl, otel/
  story.py --export             ... and screens/ + export/ (needs chromium, ffmpeg, node)
  story.py --sample --export    the container sample and its rehearsal lines

Inputs are sample_figures.py's (--runs, --otel, --clocks, --port-lines),
plus --extra-runs for more runs files (headline lines) and --timeline for
E14's timeline.jsonl. --emulated (the default with --sample) stamps every
number "emulated, container lab, sample sizes; not a result".
"""
import argparse
import datetime
import json
import os
import re
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import story_model  # noqa: E402

ROOT = os.path.normpath(os.path.join(HERE, "..", ".."))
ASSETS = os.path.join(HERE, "story")
# Concatenated in this order into one <script>; each file is one module of the page.
JS = ["core.js", "charts.js", "hero.js", "scaling.js", "headline.js", "detail.js", "main.js"]


def args(argv):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--results", default=os.path.join(ROOT, "results", "lab"))
    p.add_argument("--runs")
    p.add_argument("--extra-runs", action="append", default=[])
    p.add_argument("--otel", action="append", default=[])
    p.add_argument("--clocks")
    p.add_argument("--port-lines")
    p.add_argument("--timeline", action="append", default=[])
    p.add_argument("--out", default=os.path.join(ROOT, "results", "lab", "story"))
    p.add_argument("--label", default="")
    p.add_argument("--sample", action="store_true", help="the container sample in results/lab/sample/data")
    p.add_argument("--emulated", action="store_true", help="stamp every number as emulated")
    p.add_argument("--export", action="store_true", help="also write screens/ and export/ (headless chromium)")
    p.add_argument("--no-video", action="store_true", help="with --export, skip the MP4 and GIF")
    a = p.parse_args(argv)
    if a.sample:
        d = os.path.join(ROOT, "results", "lab", "sample", "data")
        a.results = d
        a.runs = a.runs or os.path.join(d, "runs.jsonl")
        a.extra_runs = a.extra_runs or [os.path.join(d, "rehearsal", "runs.jsonl")]
        a.otel = a.otel or [os.path.join(d, "otel"), os.path.join(d, "otel-cgroup")]
        a.port_lines = a.port_lines or os.path.join(ROOT, "results", "lab", "port-lines.csv")
        a.label = a.label or "container sample + rehearsal"
        a.emulated = True
    a.runs = a.runs or os.path.join(a.results, "runs.jsonl")
    a.otel = a.otel or [os.path.join(os.path.dirname(os.path.abspath(a.runs)), "otel")]
    return a


def read(name):
    with open(os.path.join(ASSETS, name)) as f:
        return f.read()


def page(model):
    data = json.dumps(model, separators=(",", ":"), allow_nan=False).replace("</", "<\\/")
    html = read("template.html")
    parts = {"STYLE": read("style.css"), "D3": read("vendor/d3.v7.9.0.min.js"),
             "DATA": data, "APP": "\n".join("// --- %s\n%s" % (n, read(n)) for n in JS)}
    # One pass, so an inlined part can never be taken for a later placeholder.
    return re.sub(r"/\*(STYLE|D3|DATA|APP)\*/", lambda m: parts[m.group(1)], html)


def main(argv=None):
    a = args(argv)
    if not os.path.exists(a.runs):
        sys.exit("story.py: no runs file at %s" % a.runs)
    model = story_model.build(a)
    model["meta"]["generated"] = datetime.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    model["meta"]["command"] = "scripts/campaign/story.py" + (" --sample" if a.sample else "") + " --export"
    os.makedirs(a.out, exist_ok=True)
    out = os.path.join(a.out, "index.html")
    with open(out, "w") as f:
        f.write(page(model))
    print("wrote %s (%d kB)" % (out, os.path.getsize(out) // 1024))
    if a.export:
        node = shutil.which("node")
        if not node:
            sys.exit("story.py: --export needs node")
        cmd = [node, os.path.join(ASSETS, "export.mjs"), out] + (["--no-video"] if a.no_video else [])
        sys.exit(subprocess.call(cmd))


if __name__ == "__main__":
    main()
