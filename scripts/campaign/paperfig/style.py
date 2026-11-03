"""The paper's figure style: one system mapping, sizes, and the few drawing
helpers every figure shares. paper.mplstyle holds the rcParams.

Every system keeps one colour (Okabe-Ito, colourblind safe), one marker,
one line style and one hatch in every figure, so a figure still reads in
greyscale print. PipedPeer is the only solid fill; the baselines are
hatched or hollow.
"""
import os
from dataclasses import dataclass

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
STYLE = os.path.join(HERE, "paper.mplstyle")


def apply():
    """The paper style, again: other campaign scripts set rcParams of their
    own (analyse.py's figures), and a figure takes them at creation."""
    plt.style.use("default")
    plt.style.use(STYLE)


apply()

# IEEEtran's \columnwidth and \textwidth (252 pt and 516 pt, TeX points):
# the nominal 3.5 in and 7.16 in overflow the class's column by 1-1.5 pt.
SINGLE, DOUBLE = 252 / 72.27, 516 / 72.27
WIDTH = {"single": SINGLE, "double": DOUBLE}

INK, INK2, MUTED, RULE, GRID, PAPER = "#1A1A1A", "#555555", "#8C8C8C", "#BDBDBD", "#E3E3E3", "#FFFFFF"
ORANGE = "#E69F00"


@dataclass(frozen=True)
class System:
    label: str
    color: str
    marker: str
    ls: object
    hatch: str
    solid: bool  # filled bars and markers; baselines are hatched / hollow


SYS = {
    "pipedpeer": System("PipedPeer", "#0072B2", "o", "-", "", True),
    "ray": System("Ray", "#D55E00", "s", (0, (4, 1.6)), "////", False),
    "dask": System("Dask", "#009E73", "^", (0, (5, 1.4, 1, 1.4)), "\\\\\\\\", False),
    "plain": System("plain Python", "#595959", "D", (0, (1, 1.3)), "....", False),
    "torchrun": System("torchrun", "#CC79A7", "v", (0, (3, 1, 1, 1, 1, 1)), "xx", False),
}
DIST = ["pipedpeer", "ray", "dask"]
ORDER = ["pipedpeer", "ray", "dask", "plain", "torchrun"]

# Variants keep their family's colour and differ in label and fill.
VARIANT = {
    "pipedpeer-headcount": ("pipedpeer", "PipedPeer, even split"),
    "pipedpeer-exact": ("pipedpeer", "PipedPeer exact"),
    "pipedpeer-fast": ("pipedpeer", "PipedPeer fast"),
    "pipedpeer-shim": ("pipedpeer", "shim on"),
    "pipedpeer-noshim": ("plain", "shim off"),
    "pipedpeer-ctrlc": ("pipedpeer", "PipedPeer Ctrl-C"),
}


def family(system):
    return system if system in SYS else VARIANT.get(system, ("plain", system))[0]


def sys_of(system):
    return SYS[family(system)]


def label(system):
    return SYS[system].label if system in SYS else VARIANT.get(system, (None, system))[1]


def order_key(system):
    f = family(system)
    return (ORDER.index(f) if f in ORDER else 9, system != f, system)


# --- figure and axes ------------------------------------------------------------

def figure(width, height, nrows=1, ncols=1, **kw):
    """A figure at exactly width x height inches (width 'single' | 'double' or
    inches). constrained_layout keeps every label inside without changing
    the size."""
    apply()
    w = WIDTH.get(width, width)
    kw.setdefault("squeeze", False)
    fig, axes = plt.subplots(nrows, ncols, figsize=(w, height), **kw)
    return fig, axes


def int_y(ax):
    from matplotlib.ticker import MaxNLocator
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5, integer=True))


def ygrid(ax, minor=False):
    ax.grid(axis="y", which="major", color=GRID, linewidth=0.4)
    if minor:
        ax.grid(axis="y", which="minor", color=GRID, linewidth=0.25)


def xgrid(ax):
    ax.grid(axis="x", which="major", color=GRID, linewidth=0.4)


def log2_x(ax, ns):
    ax.set_xscale("log", base=2)
    ax.set_xticks(ns)
    ax.set_xticklabels([str(n) for n in ns])
    ax.minorticks_off()
    lo, hi = min(ns), max(ns)
    ax.set_xlim(lo / 1.25, hi * 1.25)


def plain_log(ax, axis="both"):
    """Log axis labelled 20, 50, 100 instead of 2x10^1."""
    from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter
    fmt = FuncFormatter(lambda v, _p: ("%g" % v) if v >= 1 else ("%.2g" % v))
    for a in ([ax.xaxis, ax.yaxis] if axis == "both" else [getattr(ax, axis + "axis")]):
        a.set_major_locator(LogLocator(base=10, subs=(1, 2, 5)))
        a.set_major_formatter(fmt)
        a.set_minor_formatter(NullFormatter())


def panel_tag(ax, text, loc="left"):
    """(a), (b) ... above a panel, in the paper's caption style."""
    x = 0.0 if loc == "left" else 1.0
    ax.text(x, 1.0, text, transform=ax.transAxes, ha=loc, va="bottom", fontsize=8, color=INK)


def line_kw(system, ms=3.6, lw=1.0):
    s = sys_of(system)
    return dict(color=s.color, marker=s.marker, linestyle=s.ls, linewidth=lw, markersize=ms,
                markerfacecolor=s.color if s.solid else PAPER, markeredgecolor=s.color, markeredgewidth=0.8)


def point_kw(system, ms=4.0):
    k = line_kw(system, ms)
    k["linestyle"] = "none"
    return k


def bar_kw(system, hollow=False):
    """Bar style: PipedPeer solid, baselines a light tint with a hatch in
    the system colour; hollow=True draws the outline-only variant."""
    s = sys_of(system)
    if hollow:
        return dict(facecolor=PAPER, edgecolor=s.color, linewidth=0.7, hatch=s.hatch or "")
    if s.solid:
        return dict(facecolor=s.color, edgecolor=s.color, linewidth=0.5)
    return dict(facecolor=tint(s.color, 0.22), edgecolor=s.color, linewidth=0.6, hatch=s.hatch)


def tint(hex_color, a):
    """hex_color mixed with white: a = 1 is the colour, 0 white."""
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
    return "#%02X%02X%02X" % tuple(round(255 - (255 - c) * a) for c in (r, g, b))


def handles(systems, kind="line"):
    out = []
    for s in systems:
        if kind == "bar":
            out.append(Patch(label=label(s), **bar_kw(s)))
        else:
            out.append(Line2D([], [], label=label(s), **line_kw(s)))
    return out


def legend_top(fig, systems, kind="line", ncol=None, top=0.9, **kw):
    """One row of legend entries across the top of the figure; the axes get
    the space below `top` (a fraction of the height)."""
    h = handles(systems, kind) if systems and isinstance(systems[0], str) else systems
    fig.get_layout_engine().set(rect=(0, 0, 1, top))
    return fig.legend(handles=h, loc="upper center", bbox_to_anchor=(0.5, 1.0), ncol=ncol or len(h),
                      frameon=False, borderaxespad=0.1, **kw)


def end_labels(ax, ends, min_gap_pt=7.5, dx_pt=3, fontsize=7):
    """Direct labels at the right end of lines: ends = [(x, y, text, color)].
    Labels keep their order, sit at least min_gap_pt apart, and stay inside
    the axes' height."""
    if not ends:
        return
    fig = ax.figure
    fig.canvas.draw()
    px_per_pt = fig.dpi / 72.0
    gap = min_gap_pt * px_per_pt
    box = ax.get_window_extent()
    items = sorted(((ax.transData.transform((x, y))[1], x, y, t, c) for x, y, t, c in ends), key=lambda e: e[0])
    pos = []
    for py, *_ in items:
        pos.append(max(py, pos[-1] + gap) if pos else max(py, box.y0 + gap / 2))
    over = pos[-1] - (box.y1 - gap / 2)
    if over > 0:
        pos = [p - over for p in pos]
        for i in range(len(pos) - 2, -1, -1):
            pos[i] = min(pos[i], pos[i + 1] - gap)
    for p, (py, x, y, t, c) in zip(pos, items):
        ax.annotate(t, xy=(x, y), xytext=(dx_pt, (p - py) / px_per_pt), textcoords="offset points",
                    ha="left", va="center", fontsize=fontsize, color=c, annotation_clip=False)


def place_labels(ax, pts, fontsize=5.8, color=None, pad_pt=1.0):
    """Label points without overlap: pts = [(x, y, text)]. Each label tries
    above, below, right and left of its point and takes the first spot that
    clears the labels already placed and the axes edges."""
    fig = ax.figure
    fig.canvas.draw()
    r = fig.canvas.get_renderer()
    taken = []
    ax_box = ax.get_window_extent(r)
    spots = [((0, 3.2), "center", "bottom"), ((0, -3.6), "center", "top"), ((3.4, 0), "left", "center"),
             ((-3.4, 0), "right", "center"), ((0, 7.5), "center", "bottom"), ((0, -8), "center", "top")]
    for x, y, t in pts:
        best = None
        for off, ha, va in spots:
            a = ax.annotate(t, (x, y), xytext=off, textcoords="offset points", ha=ha, va=va, fontsize=fontsize,
                            color=color or INK2, annotation_clip=False)
            bb = a.get_window_extent(r).expanded(1.0 + pad_pt / 20, 1.0 + pad_pt / 10)
            inside = ax_box.x0 <= bb.x0 and bb.x1 <= ax_box.x1 and ax_box.y0 <= bb.y0 and bb.y1 <= ax_box.y1
            if inside and not any(bb.overlaps(o) for o in taken):
                best = (a, bb)
                break
            a.remove()
        if best is None:
            off, ha, va = spots[0]
            a = ax.annotate(t, (x, y), xytext=off, textcoords="offset points", ha=ha, va=va, fontsize=fontsize,
                            color=color or INK2, annotation_clip=False)
            best = (a, a.get_window_extent(r))
        taken.append(best[1])


def note(ax, x, y, text, transform=None, **kw):
    kw.setdefault("fontsize", 7)
    kw.setdefault("color", INK2)
    return ax.text(x, y, text, transform=transform or ax.transAxes, **kw)


def draft_mark(fig, text="emulated: container sample, not a result"):
    """Along the right edge, outside every axes constrained_layout placed."""
    fig.text(0.999, 0.5, text, ha="right", va="center", rotation=90, fontsize=5, color="#C0392B",
             alpha=0.9, fontstyle="italic", zorder=100)


def synthetic_mark(fig):
    fig.text(0.5, 0.5, "SYNTHETIC EXAMPLE", ha="center", va="center", fontsize=20, color="#C0392B",
             alpha=0.16, rotation=18, fontweight="bold", zorder=100)
    draft_mark(fig, "synthetic example: layout only, no measurement")


def save(fig, out, name, draft=False, mark=None):
    """PDF (vector, Type 42 fonts) and 600 dpi PNG at the figure's exact size."""
    if draft:
        eng = fig.get_layout_engine()
        if eng is not None and hasattr(eng, "set"):
            x0, y0, w, h = eng.get().get("rect") or (0, 0, 1, 1)
            eng.set(rect=(x0, y0, min(w, 0.975 - x0), h))
        (mark or draft_mark)(fig)
    os.makedirs(out, exist_ok=True)
    pdf = os.path.join(out, name + ".pdf")
    with plt.rc_context({"savefig.bbox": "standard", "savefig.pad_inches": 0, "pdf.fonttype": 42}):
        fig.savefig(pdf, metadata={"Creator": "scripts/campaign/paper_figures.py", "CreationDate": None})
        fig.savefig(os.path.join(out, name + ".png"), dpi=600)
    plt.close(fig)
    return pdf
