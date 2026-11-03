"""Build the ESE presentation, docs/presentation/PipedPeer-ESE.pptx.

    uv run --with python-pptx --with pillow python docs/presentation/build_deck.py

Every number on a slide comes from docs/benchmarks (or the commit named in
docs/paper/journal/manuscript.tex beside the same number). Figures are the
report's own, from docs/diagrams. Speaker notes carry what to say.
"""
import pathlib

from PIL import Image
from pptx import Presentation
from pptx.chart.data import CategoryChartData
from pptx.dml.color import RGBColor
from pptx.enum.chart import XL_CHART_TYPE, XL_LEGEND_POSITION
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.oxml.ns import qn
from pptx.util import Emu, Inches, Pt

HERE = pathlib.Path(__file__).resolve().parent
DIAG = HERE.parent / "diagrams"
OUT = HERE / "PipedPeer-ESE.pptx"
LOGO = HERE / "spitlogo.png"

BLUE = RGBColor(0x1E, 0x50, 0xA0)
DARK = RGBColor(0x22, 0x22, 0x22)
GREY = RGBColor(0x6B, 0x6B, 0x6B)
LIGHT = RGBColor(0xEE, 0xF3, 0xFB)
GREEN = RGBColor(0x2F, 0x7D, 0x4F)
RED = RGBColor(0xA3, 0x33, 0x33)
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
FOOTER = "Department of Computer Engineering, Sardar Patel Institute of Technology"
FONT = "Calibri"

W, H = Inches(13.333), Inches(7.5)
prs = Presentation()
prs.slide_width, prs.slide_height = W, H
BLANK = prs.slide_layouts[6]


# ---------------------------------------------------------------- helpers

def _font(run, size, bold=False, color=DARK, italic=False, name=FONT):
    f = run.font
    f.size, f.bold, f.italic, f.name = Pt(size), bold, italic, name
    f.color.rgb = color


def _bullet(p, char, level):
    """Give paragraph p a hanging bullet; level 0 or 1."""
    pPr = p._p.get_or_add_pPr()
    mar = Inches(0.28 + 0.32 * level)
    pPr.set("marL", str(int(mar)))
    pPr.set("indent", str(-int(Inches(0.24))))
    for tag in ("a:buNone", "a:buChar", "a:buFont"):
        for el in pPr.findall(qn(tag)):
            pPr.remove(el)
    bf = pPr.makeelement(qn("a:buFont"), {"typeface": "Arial"})
    bc = pPr.makeelement(qn("a:buChar"), {"char": char})
    pPr.append(bf)
    pPr.append(bc)


def text(slide, x, y, w, h, items, size=18, color=DARK, bold=False,
         align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP, spacing=6):
    """items: a string, or a list of strings / (level, string) for bullets.
    A plain string in a list is a level-0 bullet; '!' prefix makes it a
    non-bulleted line."""
    tb = slide.shapes.add_textbox(x, y, w, h)
    tf = tb.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    tf.margin_left = tf.margin_right = Inches(0.05)
    if isinstance(items, str):
        items = [items if items.startswith("!") else "!" + items]
    first = True
    for it in items:
        level, s = (it if isinstance(it, tuple) else (0, it))
        p = tf.paragraphs[0] if first else tf.add_paragraph()
        first = False
        p.alignment = align
        p.space_after = Pt(spacing)
        plain = s.startswith("!")
        if plain:
            s = s[1:]
        else:
            _bullet(p, "•" if level == 0 else "–", level)
        # **bold** segments
        parts = s.split("**")
        for i, part in enumerate(parts):
            if not part:
                continue
            r = p.add_run()
            r.text = part
            _font(r, size - 2 * level, bold or i % 2 == 1, color)
    return tb


def picture(slide, path, x, y, w, h):
    """Place an image inside the box (x, y, w, h), centred, aspect kept."""
    iw, ih = Image.open(path).size
    scale = min(w / iw, h / ih)
    pw, ph = int(iw * scale), int(ih * scale)
    slide.shapes.add_picture(str(path), x + (w - pw) // 2, y + (h - ph) // 2, pw, ph)


def table(slide, x, y, w, rows, col_w=None, size=13, row_h=0.36, header=True):
    nr, nc = len(rows), len(rows[0])
    shp = slide.shapes.add_table(nr, nc, x, y, w, Inches(row_h * nr))
    t = shp.table
    if col_w:
        total = sum(col_w)
        for i, cw in enumerate(col_w):
            t.columns[i].width = int(w * cw / total)
    for r, row in enumerate(rows):
        t.rows[r].height = Inches(row_h)
        for c, val in enumerate(row):
            cell = t.cell(r, c)
            cell.margin_left = cell.margin_right = Inches(0.06)
            cell.margin_top = cell.margin_bottom = Inches(0.03)
            cell.vertical_anchor = MSO_ANCHOR.MIDDLE
            tf = cell.text_frame
            tf.word_wrap = True
            tf.paragraphs[0].text = ""
            run = tf.paragraphs[0].add_run()
            run.text = str(val)
            is_head = header and r == 0
            _font(run, size, bold=is_head, color=WHITE if is_head else DARK)
            cell.fill.solid()
            cell.fill.fore_color.rgb = BLUE if is_head else (LIGHT if r % 2 == 0 else WHITE)
    return t


def bar_chart(slide, x, y, w, h, cats, series, title=None, fmt='0.0', legend=True,
              colors=(BLUE, GREEN, RED, GREY), size=12):
    cd = CategoryChartData()
    cd.categories = cats
    for name, vals in series:
        cd.add_series(name, vals)
    gf = slide.shapes.add_chart(XL_CHART_TYPE.COLUMN_CLUSTERED, x, y, w, h, cd)
    ch = gf.chart
    ch.font.size, ch.font.name = Pt(size), FONT
    ch.has_title = bool(title)
    if title:
        ch.chart_title.text_frame.text = title
        _font(ch.chart_title.text_frame.paragraphs[0].runs[0], size + 2, True)
    ch.has_legend = legend and len(series) > 1
    if ch.has_legend:
        ch.legend.position = XL_LEGEND_POSITION.BOTTOM
        ch.legend.include_in_layout = False
    plot = ch.plots[0]
    plot.gap_width = 60
    plot.has_data_labels = True
    dl = plot.data_labels
    dl.number_format, dl.number_format_is_linked = fmt, False
    dl.font.size = Pt(size)
    for i, s in enumerate(plot.series):
        s.format.fill.solid()
        s.format.fill.fore_color.rgb = colors[i % len(colors)]
    ch.value_axis.has_major_gridlines = False
    ch.value_axis.tick_labels.font.size = Pt(size - 1)
    ch.category_axis.tick_labels.font.size = Pt(size)
    return ch


page = [0]


def new_slide(title, notes=""):
    s = prs.slides.add_slide(BLANK)
    page[0] += 1
    if title is not None:
        text(s, Inches(0.5), Inches(0.3), Inches(11.4), Inches(0.8), "!" + title,
             size=30, color=BLUE, bold=True, anchor=MSO_ANCHOR.MIDDLE)
        s.shapes.add_picture(str(LOGO), Inches(12.3), Inches(0.22), height=Inches(0.75))
        line = s.shapes.add_connector(1, Inches(0.5), Inches(1.12), Inches(12.83), Inches(1.12))
        line.line.color.rgb = BLUE
        line.line.width = Pt(1.25)
        text(s, Inches(0.5), Inches(7.02), Inches(9), Inches(0.35), "!" + FOOTER,
             size=10, color=GREY)
        text(s, Inches(12.1), Inches(7.02), Inches(0.75), Inches(0.35), "!" + str(page[0]),
             size=10, color=GREY, align=PP_ALIGN.RIGHT)
    if notes:
        s.notes_slide.notes_text_frame.text = notes
    return s


L, T = Inches(0.55), Inches(1.35)          # content origin
CW, CH = Inches(12.25), Inches(5.55)       # content size
HALF = Inches(6.0)


# ---------------------------------------------------------------- slides

# 1 Title
s = new_slide(None, notes=(
    "Good morning. We are Yateen, Vinayak and Sahil. Our guide is Dr. Rupali Sawant. "
    "Our project is PipedPeer: it runs an ordinary Python script on the other "
    "machines around you, without setting up a cluster, without changing the script, and across "
    "different home or college networks."))
s.shapes.add_picture(str(LOGO), int((W - Inches(1.4)) / 2), Inches(0.45), height=Inches(1.4))
text(s, Inches(1), Inches(2.0), Inches(11.33), Inches(1.3),
     "!PipedPeer: Zero-Configuration Distributed Computing via Recursive, Decentralised Coordination",
     size=32, color=BLUE, bold=True, align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
text(s, Inches(1), Inches(3.5), Inches(11.33), Inches(1.4), [
    "!Yateen Vaviya (UCID 2023300251)",
    "!Vinayak Yadav (UCID 2023300264)",
    "!Sahil Gupta (UCID 2023300072)"], size=20, align=PP_ALIGN.CENTER, spacing=2)
text(s, Inches(1), Inches(5.0), Inches(11.33), Inches(0.9), [
    "!Guide: Dr. Rupali Sawant"], size=18,
    align=PP_ALIGN.CENTER)
text(s, Inches(1), Inches(5.8), Inches(11.33), Inches(1.0), [
    "!" + FOOTER + ", Mumbai",
    "!Main Project Stage-II, End Semester Examination, November 2026"], size=15,
    color=GREY, align=PP_ALIGN.CENTER, spacing=2)

# 2 Introduction
s = new_slide("Introduction", notes=(
    "Every lab and every home has machines sitting idle. A student with a slow script cannot easily use "
    "them. Spark, Ray and Dask need a cluster that someone set up, a program rewritten against their API, "
    "and machines that can reach each other. ssh avoids setup but gives no environment, no placement and "
    "no failure handling. So we asked: can python script.py simply use the machines around it?"))
text(s, L, T, CW, CH, [
    "Idle computers are everywhere: college labs, friends' laptops, phones.",
    "Using them for a Python script normally needs three things first:",
    (1, "a configured cluster with a head node and matching software on every machine,"),
    (1, "a program rewritten against a framework's API (Spark, Ray, Dask),"),
    (1, "machines that can reach one another, which home and campus routers prevent."),
    "Remote execution over ssh avoids setup but gives no environment management, placement, "
    "isolation or fault tolerance.",
    "PipedPeer: **pipedpeer run script.py** runs the unchanged script on other machines, "
    "and spreads the parallel work inside it when that is faster.",
], size=20)

# 3 Problem statement
s = new_slide("Problem Statement", notes=(
    "Formally: given an unmodified Python script and a few machines owned by one group, possibly behind "
    "different routers, run the script and its parallel work on those machines with no cluster setup and "
    "no code change, without ever changing the result, and without being slower than staying local."))
text(s, L, T, CW, CH, [
    "Given an **unmodified Python script** and a small group's machines, possibly on "
    "**different networks behind NAT routers**, run the script and its parallel work on those machines:",
    (1, "with **no cluster to configure** and no head node or scheduler process,"),
    (1, "with **no change to the script**,"),
    (1, "reproducing the script's environment exactly on every machine,"),
    (1, "joining machines across networks **without relaying** job data through a third machine,"),
    (1, "never changing the result, and declining to distribute when it would be slower."),
], size=21)

# 4 Objectives
s = new_slide("Objectives", notes=(
    "These are the objectives we set. Each one maps to a part of the system and a measurement in the report."))
text(s, L, T, CW, CH, [
    "One binary per machine; joining takes one setup command and, across networks, one invite code.",
    "Build the script's environment as a pinned Nix closure; send a peer only the store paths it lacks.",
    "Substitute the parallel primitives the script already uses (Pool, joblib, pandas, NumPy, PyTorch "
    "training) at import time.",
    "Divide work by measured machine speed; keep work local when a cost model predicts no gain.",
    "Connect machines behind different routers directly: introducer, port mapping, hole punching; no relay.",
    "Train PyTorch models data-parallel across machines, with accuracy held within 1% of one device.",
    "Survive a worker, link or rank failing without changing the result; sandbox every job.",
    "Use an Android phone's CPU as a worker.",
], size=19)

# 5 Literature survey
s = new_slide("Literature Survey", notes=(
    "We compared the existing ways to use more than one machine from Python. Each solves part of the "
    "problem. The closest is Arjona et al. 2023, which runs unmodified multiprocessing code, but on "
    "serverless cloud functions that someone must provision and pay for. The full survey is chapter 2 of "
    "the report."))
table(s, L, T, CW, [
    ["System / paper", "What it does", "Limitation for our problem"],
    ["Spark (Zaharia et al., 2010)", "Cluster computing on resilient datasets", "Configured cluster, Spark API, JVM"],
    ["Ray (Moritz et al., OSDI 2018)", "Tasks and actors; drop-in Pool and joblib", "Head node, changed import, reachable machines"],
    ["Dask (Rocklin, 2015)", "Parallel NumPy/pandas, task scheduler", "Scheduler process, changed code"],
    ["Arjona et al., FGCS 2023 (Lithops)", "Unmodified multiprocessing on serverless functions", "Needs a cloud back end; no peers, no NAT"],
    ["HTCondor / BOINC", "Harvest idle machines for jobs", "Central manager; project-specific"],
    ["Nix (Dolstra et al., 2004)", "Content-addressed, reproducible builds", "Single machine; no peer-to-peer push"],
    ["Ford et al., 2005; STUN/ICE", "UDP hole punching across NATs", "Building blocks, not an executor"],
    ["DiLoCo (2023); local SGD (2020)", "Train with infrequent synchronisation", "Datacentre scale; need a framework"],
], col_w=[3.2, 4.3, 4.7], size=13, row_h=0.52)

# 6 Research gap
s = new_slide("Research Gap", notes=(
    "No system we surveyed covers the three obstacles together for a small group. The frameworks assume "
    "a cluster; the drop-in ones assume a cloud back end; the networking tools are pieces, not an executor."))
text(s, L, T, CW, CH, [
    "Cluster frameworks remove the code change only partly and always need a configured cluster.",
    "Drop-in multiprocessing systems (Fiber, Lithops, Arjona et al.) need a provisioned back end: "
    "a cluster manager or a cloud account.",
    "None ships the script's exact environment to ordinary peer machines.",
    "None connects machines behind different home routers without a VPN or a relay.",
    "None decides per call whether distributing is worth it, and stays local when it is not.",
    "**Gap:** run unmodified Python across a small group's own machines, on any network, "
    "with no setup beyond one command, and never slower or different than running it locally "
    "by design.",
], size=20)

# 7 Proposed system
s = new_slide("Proposed System: PipedPeer", notes=(
    "This is what the user sees. Every machine runs the same daemon. The user types pipedpeer run. The "
    "client builds the environment, picks a machine, ships what is missing, and runs the job in a sandbox. "
    "Inside the job, a Python shim spreads the parallel calls across machines when the model says it pays."))
text(s, L, T, Inches(6.3), CH, [
    "Each machine runs **one daemon** from a single Go binary; any machine submits and works.",
    "**pipedpeer setup** installs everything; **pipedpeer invite / connect** joins another machine.",
    "**pipedpeer run script.py**:",
    (1, "scans imports and builds a pinned **Nix closure**,"),
    (1, "places the job by live capacity under a lease,"),
    (1, "sends only the missing store paths and the workspace,"),
    (1, "runs it in a **crun** sandbox with a memory ceiling,"),
    (1, "streams output back; changed files return."),
    "Inside the job, a **shim** spreads Pool, joblib, pandas, NumPy and torch work over the cluster.",
], size=18)
table(s, Inches(7.1), T + Inches(0.1), Inches(5.7), [
    ["Command", "What it does"],
    ["setup", "Install, open ports, start daemon"],
    ["invite / connect", "Join a machine with one code"],
    ["run script.py", "Run, spreading the work"],
    ["status / dashboard", "Machines and their work"],
    ["doctor", "Diagnose, with the fix"],
    ["jobs / attach", "History; reattach to a run"],
    ["ui", "Cluster web page"],
    ["uninstall", "Remove everything it added"],
], col_w=[2.2, 3.5], size=14, row_h=0.47)

# 8 Architecture
s = new_slide("System Architecture", notes=(
    "The submitting node and the worker are the same binary. The submitter builds the closure and places "
    "the job: accept, commit, then the missing paths and files, then run and stream. The worker admits the "
    "job, fills its closure cache, and runs the script with the shim in a crun sandbox. Parallel work and "
    "training ranks go to peer nodes, chunks also to phones. The introducer lives in every daemon and only "
    "exchanges addresses; it never carries job data."))
picture(s, DIAG / "fig_3.1_system_design.png", L, T, Inches(8.6), CH)
text(s, Inches(9.35), T + Inches(0.3), Inches(3.5), CH, [
    "No head node: placement runs in the submitting client.",
    "Worker: admit, cache by store path, sandbox.",
    "Peers take parallel work and training ranks.",
    "Phones take chunks only.",
    "Introducer in every daemon: addresses only.",
], size=16)

# 9 Life of a job
s = new_slide("How a Job Runs", notes=(
    "This is the sequence of one run. The lease has three phases so a dead submitter's slot is reclaimed "
    "and a dead worker's job is rescheduled. Only missing paths are sent. Output streams back live, and a "
    "dropped connection never kills the job: you can reattach."))
picture(s, DIAG / "fig_3.5_sequence.png", L, T, Inches(5.6), CH)
text(s, Inches(6.5), T + Inches(0.2), Inches(6.3), CH, [
    "!1. Scan imports; build or reuse the closure.",
    "!2. Filter and score candidates; three-phase lease (accept, commit, complete), renewed every 30 s.",
    "!3. Ask what the worker lacks; send only those store paths, then the workspace.",
    "!4. Run in a sandbox with the shim on the interpreter's path.",
    "!5. Parallel calls may go to peers that can run the same closure.",
    "!6. Stream output; return changed files; print where the time went.",
    "Ctrl+C stops the job on every machine; a dropped connection never kills it (reattach).",
], size=17)

# 10 Environment reproduction
s = new_slide("Environment Reproduction: Nix Closures", notes=(
    "The environment is a Nix closure pinned to one exact package-set revision, so the same script has the "
    "same store path on every machine. A worker that holds it receives nothing; one that holds part of it "
    "receives only the missing paths. Measured: first submission 23.4 seconds, then 0.83 seconds median. "
    "Adding one package sent 28.6 MB instead of 206 MB."))
picture(s, DIAG / "fig_4.6_closure_cache.png", L, T, Inches(6.6), Inches(4.3))
text(s, L, T + Inches(4.4), Inches(6.6), Inches(1.2), [
    "Pinned package-set revision: same script, same store path, every machine and every day.",
    "Signed closures pushed peer to peer; only peers that can run it get distributed work.",
], size=15)
bar_chart(s, Inches(7.4), T, Inches(5.4), Inches(3.4),
          ["1st submit", "2nd (new worker)", "cached (median)"],
          [("seconds", (23.4, 8.1, 0.83))], title="Same job, five submissions (plain lab)",
          fmt='0.0#', legend=False, size=12)
table(s, Inches(7.4), T + Inches(3.6), Inches(5.4), [
    ["Closure diff (one package added)", "Sent"],
    ["Whole closure", "206.0 MB"],
    ["Only missing paths (7 of 43)", "28.6 MB (-86%)"],
], col_w=[3.3, 2.1], size=13, row_h=0.4)

# 11 Interception
s = new_slide("Transparent Interception", notes=(
    "A Python package embedded in the binary is imported before the first line of the script. Libraries are "
    "patched only when the script imports them, which keeps startup at 1.24 times plain Python; patching "
    "eagerly had cost 20 times. Every substituted call falls back to the original on any error."))
picture(s, DIAG / "fig_4.8_shim_injection.png", L, T, Inches(7.0), Inches(4.2))
text(s, Inches(7.8), T + Inches(0.1), Inches(5.0), CH, [
    "Imported before the script's first line; no line of the script changes.",
    "Patched on first import:",
    (1, "multiprocessing / concurrent.futures maps, joblib"),
    (1, "pandas groupby, merge, chunked read_csv"),
    (1, "NumPy products and decompositions"),
    (1, "PyTorch data-parallel training"),
    "Any error falls back to the original call.",
    "Startup **1.24×** plain Python (eager patching: 20×).",
    "A declined call costs about **0.12 s** per job.",
], size=17)

# 12 Dividing a parallel map
s = new_slide("Dividing Work by Measured Speed", notes=(
    "Each machine is modelled by a setup time and a measured rate. We find the finish time T at which the "
    "machines together absorb all the items, and drop any machine that cannot even start before T. So a "
    "machine three times faster gets three times the work, and a very slow one is left out. Late parts get "
    "a backup copy; the first to finish wins. Honest result: on an 8, 2 and 1 CPU lab, splitting by speed "
    "beat splitting by headcount, but was still 1.3 times slower than staying local at that small size."))
text(s, L, T, Inches(6.4), CH, [
    "Each device i: setup time **oᵢ**, effective rate **rᵢ** (measured, link cost folded in).",
    "Makespan T solves  **Σ max(0, T − oᵢ)·rᵢ = W**  (W items).",
    "A device with oᵢ ≥ T cannot start in time and is dropped; repeat until the set settles.",
    "Rates are refitted from every part a device returns.",
    "Late parts are copied to a free device; first result wins (backup tasks).",
    "Idle local cores re-run unfinished parts, so a dead cluster degrades to a local pool.",
], size=17)
table(s, Inches(7.2), T + Inches(0.1), Inches(5.6), [
    ["Lab of 8, 2 and 1 CPUs", "Time"],
    ["Local only (derived)", "≈ 2.3 s"],
    ["Split by headcount", "3.6 s (1.6× slower)"],
    ["Split by measured speed", "2.9 s (1-CPU node declined)"],
], col_w=[3.0, 2.6], size=14, row_h=0.45)
text(s, Inches(7.2), T + Inches(2.1), Inches(5.6), Inches(2.5), [
    "Measured split beat headcount but was still ~1.3× slower than local at this size.",
    "A peer at 1/10 CPU: slowdown fell from 1.6× to 1.08× over four runs as rates converged.",
], size=15, color=GREY)

# 13 Placement and cost model
s = new_slide("Placement and the Cost Model", notes=(
    "Placement is computed in the client: hard filters first, then a bounded score where current load "
    "dominates hardware. For numerical work the cost model compares time per byte locally against "
    "transfer plus remote compute; on our machines local BLAS is faster than any CPU cluster's transport, "
    "so dense matrix products stay local."))
picture(s, DIAG / "fig_4.3_placement_flow.png", L, T, Inches(6.4), Inches(3.9))
text(s, L, T + Inches(4.0), Inches(6.4), Inches(1.6), [
    "Filters: architecture, GPU, free memory, cores, VRAM.",
    "Score: load terms subtract up to 1.0; hardware terms move it ±0.18.",
    "× health factor: halves when a node fails, regains 0.1 per healthy poll.",
], size=15)
picture(s, DIAG / "fig_4.11_cost_model.png", Inches(7.2), T, Inches(5.6), Inches(3.9))
text(s, Inches(7.2), T + Inches(4.0), Inches(5.6), Inches(1.6), [
    "Move work only if  φ/R > ρ/B + 1.3·φ/(R′K).",
    "Bandwidth measured only when offloading could win.",
], size=15)

# 14 Joining across networks
s = new_slide("Joining Across Networks (NAT Traversal)", notes=(
    "Machines behind different routers accept no inbound connections. Every daemon is an introducer for "
    "its own cluster: it keeps an address book and forwards one punch request, and never carries job data. "
    "Both sides race every candidate pair over one UDP socket. When one router changes port per "
    "destination, we use a birthday collision: 256 sockets against 600 probes, about 91 percent per round "
    "by calculation; we did not measure that rate. Links are QUIC, signed by each node's key and bound to "
    "the cluster secret."))
picture(s, DIAG / "fig_4.1_nat_traversal.png", L, T, Inches(5.2), CH)
text(s, Inches(6.1), T + Inches(0.1), Inches(6.7), CH, [
    "Introducer in **every daemon**: address book + punch request; **no job data**.",
    "Candidates: LAN addresses, router port mapping (PCP, NAT-PMP, UPnP), reflexive address.",
    "Probes, introducer traffic and QUIC share **one UDP socket**; every pair probed every 250 ms for 15 s.",
    "Port-randomising NAT: 256 sockets vs 600 probes — **≈91% per round, computed**, not measured.",
    "QUIC link, hello signed with each node's **Ed25519** key + HMAC of the cluster secret.",
    "A node is its key, not its address: a machine that changes network rejoins on its own.",
    "No path? Reported with the reason and routed around — **never relayed**.",
], size=16)

# 15 Trust and sandbox
s = new_slide("Trust and Sandboxing", notes=(
    "Closures are signed by the sending node and checked against its published identity before import. "
    "Each job runs in an unprivileged crun container with a memory ceiling; GPU driver files are bound in "
    "one by one. We are honest about the gaps: TLS is trust on first use, signing keys are not yet bound to "
    "an authenticated identity, and one shared secret is the only authorisation, so this is for machines "
    "whose owners trust each other."))
picture(s, DIAG / "fig_4.2_trust.png", L, T, Inches(5.6), Inches(3.4))
picture(s, DIAG / "fig_4.7_sandbox.png", Inches(6.4), T, Inches(6.4), Inches(4.2))
text(s, L, T + Inches(3.6), Inches(5.6), Inches(2.0), [
    "Closures signed by the sender; key checked before import.",
    "crun container per job: memory ceiling, GPU bound in.",
    "**Gaps:** TLS trust-on-first-use; one shared secret; signing keys not yet bound to identity.",
], size=14)

# 16 Training
s = new_slide("Data-Parallel Training Through the Daemons", notes=(
    "Ranks send gradients to daemons, not to each other, so they do not need to reach one another directly. "
    "Each rank gets one sample-weighted average, sent as 8-bit values with error feedback. Batches are "
    "split by measured speed. Fast mode averages weights every H steps; at H = 32 on two ranks validation "
    "loss stayed within 1 percent of one device on all three test models, with 7.6 to 9.2 times fewer "
    "bytes. If one machine is faster alone, the ring is released."))
picture(s, DIAG / "fig_4.10_ddp.png", L, T, Inches(5.8), Inches(4.2))
text(s, Inches(6.6), T + Inches(0.1), Inches(6.2), CH, [
    "Ranks post gradients to daemons — no rank-to-rank mesh needed.",
    "One **sample-weighted average**, sent as 8-bit with error feedback.",
    "Global batch split by **measured speed**; learning rate preserved.",
    "**Exact** sync every step, or **fast** sync (average weights every H steps).",
    "H chosen per ring size from accuracy runs: 32 (2 ranks), 16 (3), 4 (4); none for 5+.",
    "After warm-up, the ring is **released** if one machine alone is faster.",
    "A rank that dies: others take its share at the next epoch; a returning rank rejoins.",
], size=16)

# 17 Phones
s = new_slide("Phones as Workers", notes=(
    "An Android phone joins from Termux without root. It runs a Debian under proot with Nix, builds the "
    "job's environment for ARM from the public cache, and takes parallel-map and torch chunks. It does not "
    "take whole jobs yet: that needs a nested proot sandbox that the Play Store Termux cannot start. On our "
    "S20 FE it took 8 of 192 items with the same checksum. It contributes a small share; we did not measure "
    "whether it made a run faster."))
text(s, L, T, Inches(6.4), CH, [
    "Android phone joins from **Termux**, no root: **pipedpeer connect <code>**.",
    "Debian under proot-distro with single-user Nix; environment built for aarch64 from the public cache.",
    "Takes **Pool / joblib and torch chunks only** (chunks-only role).",
    "Whole jobs need a nested proot sandbox that Play-Store Termux cannot start.",
    "Sandbox is weaker than crun: proot view, memory limit, low priority.",
    "Keep it charging and run termux-wake-lock.",
], size=17)
table(s, Inches(7.2), T + Inches(0.1), Inches(5.6), [
    ["Samsung Galaxy S20 FE (Android 13)", "Result"],
    ["CPU script on 2 laptops + phone", "phone took 8 of 192 items; same checksum"],
    ["Environment built on the phone", "27 s, from the cache"],
    ["160 small torch models", "phone trained 6; all converged"],
    ["32 small torch models", "phone took none"],
    ["CPU script on the phone alone", "62.4–66.0 s (Nix Python)"],
], col_w=[2.9, 2.7], size=13, row_h=0.55)

# 18 Fault tolerance
s = new_slide("Fault Tolerance and Recovery", notes=(
    "Using the cluster must never change the result. These are the failure drills we ran. When a node's "
    "link was cut during a 240-item map, its 53 items were re-run on the submitter and the checksum was "
    "unchanged. Two machines are still single points of failure: the submitter of a parallel script and the "
    "machine of a training ring's rank 0."))
table(s, L, T, CW, [
    ["Drill", "Result"],
    ["Node's link cut during a 240-item map (two-site NAT testbed)", "its 53 items re-ran on the submitter; checksum unchanged"],
    ["A worker killed with map chunks in flight (80 items)", "33 items had run on peers; no chunk lost; result correct"],
    ["A worker killed during a remote job", "node reported gone 16 s after the kill"],
    ["A training rank killed", "remaining rank finished all 80 epochs (90.5 s)"],
    ["A rank's machine back during training", "rejoined at the next epoch, 4 s after its daemon started"],
    ["Ctrl+C during a parallel map", "peers stopped within 2 s"],
    ["Submitter's network removed for 200 s", "job ran once; result back ~15 s after reconnecting"],
    ["Detach, then attach", "followed live; replayed in 1.0 s once finished"],
], col_w=[5.8, 6.4], size=14, row_h=0.5)
text(s, L, T + Inches(4.85), CW, Inches(0.6),
     "!Single points of failure remain: the submitter of a parallel script and the machine of a training "
     "ring's rank 0.", size=15, color=GREY)

# 19 Implementation
s = new_slide("Implementation", notes=(
    "PipedPeer is one Go binary with 46 packages; the Python shim is embedded in it. Links use quic-go, "
    "environments use Nix flakes, jobs run in crun. In a fresh full test run with torch installed, 1,012 of "
    "1,017 tests passed, 4 were skipped and one timing-sensitive test failed. A mutation audit disables 95 "
    "features one at a time, and the tests caught every one."))
text(s, L, T, Inches(6.4), CH, [
    "**Go 1.25**, one binary (client + daemon), **46 packages**.",
    "Embedded **Python shim**; links on **quic-go**; environments with **Nix flakes**; jobs in **crun**.",
    "Web page (pipedpeer ui) and terminal dashboard.",
    "Tests: **1,012 of 1,017 pass**, 4 skipped, 1 timing-sensitive failure (fresh run, 2 Nov 2026).",
    "**Mutation audit:** 95 features disabled one at a time — the tests caught **95 / 95**.",
    "CI: formatting, vet, race detector, skip check, never-3×-slower benchmarks.",
], size=17)
table(s, Inches(7.2), T + Inches(0.1), Inches(5.6), [
    ["Area", "Technology"],
    ["Language", "Go 1.25 + Python shim"],
    ["Environments", "Nix flakes (pinned nixpkgs)"],
    ["Sandbox", "crun (OCI), cgroup v2"],
    ["Transport", "QUIC (quic-go), TLS 1.3"],
    ["Identity", "Ed25519 node keys, HMAC"],
    ["Training", "PyTorch, 8-bit gradients"],
    ["Phone", "Termux + proot-distro"],
], col_w=[2.2, 3.4], size=14, row_h=0.47)

# 20 Experiment setup
s = new_slide("Experiment Setup", notes=(
    "We keep four kinds of measurement apart and label every result. The plain lab shares one processor, so "
    "it shows cost and correctness, never speedup. The quota lab is emulated. The two-site testbed has real "
    "NAT routers in containers. The physical runs are two laptops in different homes plus a phone, and a "
    "second pair of machines."))
table(s, L, T, CW, [
    ["Setting", "What it is", "What it can show"],
    ["Plain lab", "3 worker containers sharing one laptop", "Correctness, overhead, caching — not speedup"],
    ["Quota lab (emulated)", "Fixed CPU/memory per container; LAN / Wi-Fi / 5G shaped with netem", "How cost grows; emulated speedup"],
    ["Two-site testbed", "Two private networks behind NAT router containers + introducer", "Joining across NATs; failover"],
    ["Physical set A", "Ryzen 7 + RTX 3060 and Core i5 + RTX 3050 in two homes; Galaxy S20 FE", "Real distribution (over Tailscale)"],
    ["Physical set B", "20-core Core i9 + Ryzen laptop; cloud host as introducer", "Real pool map; introducer traffic"],
], col_w=[2.4, 5.6, 4.2], size=14, row_h=0.62)
text(s, L, T + Inches(4.0), CW, Inches(1.4), [
    "Lab times: medians of 3–5 runs. Training: one seed (accuracy: mean of 3–6 seeds).",
    "Physical runs: single runs or ranges over 2–3 runs — evidence, not a controlled scaling study.",
], size=15, color=GREY)

# 21 Results: real machines
s = new_slide("Results: Two Laptops in Different Homes", notes=(
    "Between two laptops in different homes, the mixed CPU script took 13.3 to 14.6 seconds against 17.4 to "
    "18.1 on one laptop, about half the items on each. The small training model did not gain: one GPU took "
    "4.2 seconds, faster than every split. Fast sync was 5 times faster than exact at the same loss. We show "
    "both because the system's job is to choose correctly."))
bar_chart(s, L, T, Inches(6.0), Inches(4.4), ["one laptop", "two laptops"],
          [("best", (17.4, 13.3)), ("worst", (18.1, 14.6))],
          title="Mixed CPU script (pool, joblib, NumPy, pandas), s", fmt='0.0', size=12)
bar_chart(s, Inches(6.8), T, Inches(6.0), Inches(4.4),
          ["1 GPU", "exact", "fast", "auto (s1)", "fast (s2)", "auto (s2)"],
          [("seconds", (4.2, 37.1, 7.4, 32.9, 8.5, 14.6))],
          title="Small training model, s (validation loss 0.0794–0.0797)",
          fmt='0.0', legend=False, colors=(GREEN,), size=12)
text(s, L, T + Inches(4.55), CW, Inches(1.0), [
    "CPU work gained on two machines; the small model was fastest on one GPU — splitting it did not pay.",
    "Fast sync 5.0× faster than exact at about the same loss; s1/s2 = two sessions on different builds.",
], size=15)

# 22 Results: overheads and other machines
s = new_slide("Results: Overheads and Other Measurements", notes=(
    "Interception costs 1.24 times the startup of a trivial job and 0.12 seconds when it declines. On the "
    "second pair of machines a parallel map took 4.9 to 5.4 seconds against 5.8 to 6.4 alone. Exact training "
    "over a 54 megabit home link was not faster than one machine. The introducer carried 0.2 MB against "
    "30 MB between the peers."))
picture(s, DIAG / "fig_5.2_shim_overhead.png", L, T, Inches(6.4), Inches(3.0))
table(s, Inches(7.2), T, Inches(5.6), [
    ["Measurement", "Result"],
    ["Startup with interception", "1.24× plain Python"],
    ["Declined map (16 items)", "+0.12 s per job"],
    ["crun vs bubblewrap start", "+15–21 ms"],
    ["Pool map, Core i9 alone vs + peer", "5.8–6.4 vs 4.9–5.4 s"],
    ["Training over 54 Mbit/s: 1 machine / 2 ranks fp16 / int8", "54.9 / 69.4 / 56.1 s"],
    ["Introducer traffic vs peer traffic", "0.2 MB vs ~30 MB"],
], col_w=[3.3, 2.3], size=13, row_h=0.55)
table(s, L, T + Inches(3.2), Inches(6.4), [
    ["Emulated nodes (2 CPU each)", "1", "2", "3", "4"],
    ["Wall time, s (loaded host)", "2.37", "1.15", "0.95", "0.81"],
], col_w=[3.2, 0.8, 0.8, 0.8, 0.8], size=13, row_h=0.45)
text(s, L, T + Inches(4.25), Inches(6.4), Inches(1.2),
     "!Emulated quotas on one host: shows growth, not a physical-machine speedup. No CPU step grew "
     "faster than its expected order.", size=13, color=GREY)

# 23 Results: training accuracy
s = new_slide("Results: Training Accuracy and Bytes", notes=(
    "Fast sync every 32 steps kept validation loss within 1 percent of one device on all three models: "
    "plus 0.06, plus 0.64 and plus 0.73 percent. Each rank sent 7.6 to 9.2 times fewer bytes. DiLoCo's outer "
    "momentum step failed our bar, so we use plain averaging. At eight ranks no interval kept the "
    "transformer within 1 percent, so automatic mode never picks fast sync beyond four ranks."))
picture(s, DIAG / "fig_5.4_sync_modes.png", L, T, CW, Inches(3.0))
table(s, L, T + Inches(3.15), CW, [
    ["Model (H = 32, 2 ranks)", "Loss gap vs one device", "MiB per rank, exact / fast", "Lab time LAN: exact / fast / one device"],
    ["MLP-G (559k params)", "+0.06%", "81.1 / 10.7", "20.0 / 15.2 / 27.9 s"],
    ["MLP-C (559k params)", "+0.64%", "98.9 / 10.7", "14.6 / 10.4 / 16.9 s"],
    ["Transformer (70k params)", "+0.73%", "21.7 / 2.4", "22.7 / 19.1 / 26.7 s"],
], col_w=[3.0, 2.6, 3.0, 3.6], size=13, row_h=0.42)
text(s, L, T + Inches(4.95), CW, Inches(0.6),
     "!Loss gap: mean of seeds 0–2, in-process. Lab times: quota lab, one seed, equal 2-CPU containers.",
     size=13, color=GREY)

# 24 Comparison
s = new_slide("Comparison with Existing Systems", notes=(
    "The upper rows are what PipedPeer was designed to do differently; the lower rows are where it is "
    "partial or weaker. This comparison is qualitative, from published designs; we did not benchmark "
    "against Ray or Dask."))
table(s, L, T, CW, [
    ["Property", "PipedPeer", "Spark", "Ray", "Dask", "Lithops", "ssh"],
    ["Runs unmodified Python", "yes", "no", "partial", "partial", "partial", "yes"],
    ["No cluster or back end to configure", "yes", "no", "no", "no", "no", "partial"],
    ["No head or master node", "yes", "no", "no", "no", "n/a", "yes"],
    ["Reproduces the environment", "yes", "partial", "partial", "partial", "partial", "no"],
    ["Joins NATed machines, no relay", "yes", "no", "no", "no", "n/a", "no"],
    ["Survives losing a node", "partial", "yes", "yes", "yes", "yes", "no"],
    ["Authenticated, encrypted", "partial", "yes", "partial", "partial", "yes", "yes"],
    ["Runtimes beyond Python", "no", "yes", "partial", "no", "no", "yes"],
    ["Proven in production", "no", "yes", "yes", "yes", "yes", "yes"],
], col_w=[4.0, 1.5, 1.2, 1.2, 1.2, 1.4, 1.0], size=14, row_h=0.47)
text(s, L, T + Inches(4.85), CW, Inches(0.6),
     "!Qualitative, from published designs — no wall-time benchmark against Ray, Dask or Lithops was run.",
     size=13, color=GREY)

# 25 Limitations
s = new_slide("Limitations", notes=(
    "These are the limits, and they are in the report and the paper too."))
text(s, L, T, CW, CH, [
    "Physical evaluation covers two or three machines and one phone, mostly single runs — "
    "not a controlled scaling study.",
    "No wall-time comparison with Ray, Dask or torchrun.",
    "Two homes without a reachable introducer need an overlay (e.g. Tailscale); there is no relay by design.",
    "Single points of failure: the submitter of a parallel script, and rank 0's machine.",
    "Python on Linux only; NVIDIA GPUs in practice. Phones take chunks only.",
    "Security: TLS trust-on-first-use, one shared secret, signing keys not bound to identity.",
    "Fast sync validated for 2–4 ranks; the 1% bar holds for the mean over seeds, not every seed.",
    "Cost-model and placement constants are hand-set, not calibrated across hardware.",
], size=19)

# 26 Live demo
s = new_slide("Live Demo", notes=(
    "Now the demo. One machine joins with a single code. Then the CPU script runs plain and through "
    "PipedPeer: same result lines, lower total, and the line naming the other machines. Then the GPU "
    "script, where the automatic choice keeps one GPU. Then a remote run, the phone taking CPU work, the web "
    "page, and finally we switch off a laptop's Wi-Fi mid-run and the checksum stays the same."))
code = s.shapes.add_shape(1, L, T, Inches(7.4), Inches(5.3))
code.fill.solid()
code.fill.fore_color.rgb = RGBColor(0x1E, 0x1E, 0x1E)
code.line.fill.background()
text(s, L + Inches(0.15), T + Inches(0.1), Inches(7.15), Inches(5.1), [
    "!$ pipedpeer connect <code>        # join with one line",
    "!$ pipedpeer status",
    "!$ python3 cpu.py                  # baseline TOTAL",
    "!$ pipedpeer run cpu.py            # same results, spread",
    "!$ pipedpeer run gpu.py            # one GPU or a split?",
    "!$ pipedpeer run --ddp 2 --ddp-sync fast gpu.py",
    "!$ pipedpeer run both.py           # CPU phase, then GPU",
    "!$ pipedpeer run --remote gpu.py   # whole job elsewhere",
    "!$ pipedpeer ui                    # cluster web page",
    "!$ pipedpeer run cpu.py --items 600   # cut Wi-Fi mid-run",
], size=14, color=RGBColor(0xE6, 0xE6, 0xE6), spacing=5)
for p in s.shapes[-1].text_frame.paragraphs:
    for r in p.runs:
        r.font.name = "Consolas"
text(s, Inches(8.1), T + Inches(0.1), Inches(4.7), CH, [
    "Join: **✓ connected: N other machine(s)**.",
    "cpu.py: same checksum lines, lower TOTAL, and **ran on <machine> 60%, <machine> 40%**.",
    "gpu.py: keeps one GPU when the split would be slower — and says why.",
    "Phone appears in the **ran on** line for CPU work.",
    "Wi-Fi off mid-run: same checksum; work re-run elsewhere; machine returns by itself.",
], size=16)

# 27 Conclusion
s = new_slide("Conclusion and Future Scope", notes=(
    "To conclude: PipedPeer runs unmodified Python on a small group's machines with no cluster to set up, "
    "joining networks over direct links. It gains on CPU-bound work and declines when one machine is "
    "faster. Next: close the security gaps, survive the loss of the submitter and rank 0, whole jobs on "
    "phones, and a controlled evaluation on more machines."))
text(s, L, T, Inches(6.2), CH, [
    "!**Conclusion**",
    "Unmodified Python runs across a small group's machines with one command, no cluster and no relay.",
    "Environments are shipped as closures diffed by store path (0.83 s warm vs 23.4 s cold).",
    "CPU-bound work gained on two machines; small GPU models stayed on one GPU — "
    "the system measures and declines.",
    "Fast sync kept accuracy within 1% at 7.6–9.2× fewer bytes.",
], size=17)
text(s, Inches(7.0), T, Inches(5.8), CH, [
    "!**Future scope**",
    "Bind signing keys to identities; require TLS; per-user authorisation.",
    "Survive the loss of the submitter and of rank 0.",
    "Whole jobs on phones; AMD GPUs.",
    "Locality- and topology-aware placement.",
    "Controlled, repeated evaluation on more machines against Ray, Dask and Lithops.",
], size=17)

# 28 Execution plan, budget, distribution of work
s = new_slide("Execution Plan, Budget and Distribution of Work", notes=(
    "FILL BEFORE THE EXAM: each member's responsibilities in the table. The project ran from April to "
    "November 2026 in phases, each merged after review. The budget was nil: all software is open source and "
    "we used our own laptops and a phone."))
table(s, L, T, Inches(7.4), [
    ["Member", "Responsibilities"],
    ["Yateen Vaviya", "[fill in before the exam]"],
    ["Vinayak Yadav", "[fill in before the exam]"],
    ["Sahil Gupta", "[fill in before the exam]"],
], col_w=[2.2, 5.2], size=15, row_h=0.7)
text(s, Inches(8.3), T, Inches(4.5), CH, [
    "**Timeline:** 12 April – November 2026 (first to latest commit); each phase on its own branch, "
    "merged into dev after review.",
    "**Budget:** no hardware bought — open-source software, our own laptops and phone. "
    "[confirm: any cost of the cloud host used as introducer]",
], size=16)

# 29 Publication
s = new_slide("Research Publication", notes=(
    "Our journal paper was submitted to the Journal of Parallel and Distributed Computing, an "
    "Elsevier journal, on 3 November 2026, and is under review. A shorter IEEE conference-format "
    "version accompanies the report, with the plagiarism report attached as the evaluation "
    "requires. Every number in both comes from a recorded measurement."))
text(s, L, T, CW, CH, [
    "**\"PipedPeer: Running Unmodified Python Across a Small Group's Machines Without a Configured Cluster\"**",
    (1, "Y. Vaviya, V. Yadav, S. Gupta, R. Sawant."),
    (1, "Submitted to the **Journal of Parallel and Distributed Computing** (Elsevier), 3 November 2026; under review."),
    "Shorter IEEE conference-format version (6 pages) with the report; plagiarism report attached.",
    "Every number in the paper and report traces to a recorded measurement in the repository.",
], size=19)

# 30 References
s = new_slide("References", notes="These are the main references; the report has the full list.")
text(s, L, T, CW, CH, [
    "!M. Zaharia et al., Spark: Cluster computing with working sets, HotCloud, 2010.",
    "!P. Moritz et al., Ray: A distributed framework for emerging AI applications, OSDI, 2018.",
    "!M. Rocklin, Dask: Parallel computation with blocked algorithms and task scheduling, SciPy, 2015.",
    "!A. Arjona, G. Finol, P. García-López, Transparent serverless execution of Python multiprocessing "
    "applications, Future Gener. Comput. Syst. 140 (2023) 436–449.",
    "!E. Dolstra, M. de Jonge, E. Visser, Nix: A safe and policy-free system for software deployment, LISA, 2004.",
    "!B. Ford, P. Srisuresh, D. Kegel, Peer-to-peer communication across network address translators, USENIX ATC, 2005.",
    "!J. Iyengar, M. Thomson, QUIC: A UDP-based multiplexed and secure transport, RFC 9000, 2021.",
    "!S. Li et al., PyTorch distributed: Experiences on accelerating data parallel training, PVLDB 13(12), 2020.",
    "!A. Douillard et al., DiLoCo: Distributed low-communication training of language models, arXiv:2311.08105, 2023.",
    "!T. Lin et al., Don't use large mini-batches, use local SGD, ICLR, 2020.",
], size=15, spacing=5)

# 31 Thank you
s = new_slide(None, notes="Thank you. We are happy to take questions.")
s.shapes.add_picture(str(LOGO), int((W - Inches(1.5)) / 2), Inches(1.0), height=Inches(1.5))
text(s, Inches(1), Inches(2.9), Inches(11.33), Inches(1.0), "!Thank You", size=40, color=BLUE,
     bold=True, align=PP_ALIGN.CENTER)
text(s, Inches(1), Inches(4.0), Inches(11.33), Inches(2.0), [
    "!Questions?",
    "!Yateen Vaviya, Vinayak Yadav, Sahil Gupta",
    "!Guide: Dr. Rupali Sawant",
], size=20, align=PP_ALIGN.CENTER)

prs.save(OUT)
print(f"wrote {OUT} ({len(prs.slides)} slides)")
