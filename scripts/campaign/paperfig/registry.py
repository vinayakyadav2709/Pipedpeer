"""The figure registry: one entry per evaluation figure, with its size,
paper section and draft caption. paper_figures.py draws them; the preview
document and docs/paper/FIGURES.md are generated from the same entries, so
a caption is written once.
"""
from dataclasses import dataclass
from typing import Callable


class Skip(Exception):
    """The figure's data is absent; the reason is printed, nothing drawn."""


@dataclass
class Ctx:
    draft: bool
    sample: bool


@dataclass
class Fig:
    name: str
    width: str  # "single" | "double"
    height: float  # inches
    section: str
    shows: str
    caption: str
    draw: Callable
    order: int = 0


FIGS = []


def fig(name, width, height, section, shows, caption):
    def wrap(fn):
        FIGS.append(Fig(name, width, height, section, shows, caption, fn, len(FIGS)))
        return fn
    return wrap


def need(cond, why):
    if not cond:
        raise Skip(why)
