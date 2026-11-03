"""paper_figures.py: every figure draws from the container sample at its
IEEE width with embedded TrueType fonts, absent data skips instead of
crashing, and the synthetic E16 example never reaches the final export."""
import os
import shutil
import subprocess
import sys

import pytest

pytest.importorskip("matplotlib")

HERE = os.path.dirname(os.path.abspath(__file__))
CAMPAIGN = os.path.normpath(os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(CAMPAIGN, "lib"))
sys.path.insert(0, CAMPAIGN)

import paper_figures  # noqa: E402
from paperfig import docs, style  # noqa: E402
from paperfig.registry import FIGS  # noqa: E402


def skipped(out):
    return dict(line.rstrip("\n").split("\t", 1) for line in open(os.path.join(out, "skipped.txt")) if line.strip())


def test_sample_draft_draws_every_figure_at_ieee_width(tmp_path):
    out = str(tmp_path / "draft")
    assert paper_figures.main(["--sample", "--draft", "--out", out]) == 0
    assert skipped(out) == {}
    for f in FIGS:
        pdf = os.path.join(out, f.name + ".pdf")
        assert os.path.exists(os.path.join(out, f.name + ".png")), f.name
        if shutil.which("pdfinfo"):
            w, _h = docs.pdf_size(pdf)
            assert abs(w - style.WIDTH[f.width]) < 0.01, (f.name, w)
        if shutil.which("pdffonts"):
            fonts = subprocess.run(["pdffonts", pdf], capture_output=True, text=True, check=True).stdout
            assert "Type 3" not in fonts and "CID TrueType" in fonts, (f.name, fonts)


def test_final_export_never_draws_the_synthetic_energy_example(tmp_path):
    out = str(tmp_path / "final")
    assert paper_figures.main(["--sample", "--out", out, "--only", "eval-e16-energy"]) == 0
    assert "no joules" in skipped(out)["eval-e16-energy"]
    assert not os.path.exists(os.path.join(out, "eval-e16-energy.pdf"))


def test_absent_data_skips_every_figure(tmp_path):
    results = tmp_path / "results"
    results.mkdir()
    (results / "runs.jsonl").write_text("")
    out = str(tmp_path / "out")
    assert paper_figures.main(["--results", str(results), "--out", out]) == 0
    assert set(skipped(out)) == {f.name for f in FIGS}


def test_lab_scale_layout_draws_without_failures(tmp_path):
    from paperfig import synth
    synth.main(str(tmp_path / "lab"))
    out = str(tmp_path / "out")
    assert paper_figures.main(["--results", str(tmp_path / "lab"), "--out", out]) == 0
    # no OTel and no joules in the synthetic lab
    assert set(skipped(out)) == {"eval-cpu-heatmap", "eval-net-bytes", "eval-e16-energy"}
