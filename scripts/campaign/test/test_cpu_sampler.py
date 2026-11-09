"""Tests for docs/benchmarks/workloads/cpu_sampler.py."""

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent.parent.parent
CPU_SAMPLER = REPO / "docs" / "benchmarks" / "workloads" / "cpu_sampler.py"

# Add workloads dir to sys.path so we can import cpu_sampler directly
sys.path.insert(0, str(REPO / "docs" / "benchmarks" / "workloads"))

try:
    import cpu_sampler
except ImportError:
    cpu_sampler = None


@pytest.fixture(autouse=True)
def check_sampler_exists():
    if not CPU_SAMPLER.exists() or cpu_sampler is None:
        pytest.fail(f"cpu_sampler module not yet implemented at {CPU_SAMPLER}")


def test_parse_stat_line():
    # Aggregate cpu line: user nice system idle iowait irq softirq steal
    line = "cpu  100 20 50 800 10 5 15 0 0 0"
    name, busy, total = cpu_sampler.parse_stat_line(line)
    assert name == "cpu"
    # total = 100+20+50+800+10+5+15+0 = 1000
    # idle = 800+10 = 810
    # busy = 1000 - 810 = 190
    assert total == 1000
    assert busy == 190

    # Non-cpu line
    assert cpu_sampler.parse_stat_line("intr 12345 0 0") is None
    assert cpu_sampler.parse_stat_line("") is None


def test_read_proc_stat_with_cores(tmp_path):
    stat_file = tmp_path / "stat"
    stat_file.write_text(
        "cpu  200 0 100 1700 0 0 0 0\n"
        "cpu0 100 0 50 850 0 0 0 0\n"
        "cpu1 100 0 50 850 0 0 0 0\n"
    )
    agg, cores = cpu_sampler.read_proc_stat(str(stat_file))
    assert agg == (300, 2000)
    assert cores == 2


def test_compute_busy_cores():
    # 2 cores, interval delta: total 1000, busy 250 (12.5% of total capacity = 0.25 busy cores)
    # wait: 250 / 1000 = 0.25 fraction across all cores. For 2 cores: 0.25 * 2 = 0.5 busy cores.
    prev = ((100, 1000), 2)
    curr = ((350, 2000), 2)
    busy_cores = cpu_sampler.compute_busy_cores(prev, curr)
    assert pytest.approx(busy_cores, 0.001) == 0.5


def test_streak_case1_first_streak():
    # Case: samples 2,2,6,6,6,6,6 on 8 cores, join at sample 0 -> first_work_s = index of the first 6
    # 8 cores -> 50% threshold is 4.0 cores.
    join_ns = 1_000_000_000_000  # sample 0 timestamp
    samples = [2, 2, 6, 6, 6, 6, 6]
    lines = [
        f"{join_ns + i * 1_000_000_000} {val:.1f}"
        for i, val in enumerate(samples)
    ]
    # First 6 is at index 2 (time = join_ns + 2s).
    res = cpu_sampler.find_first_work_s(lines, join_ns, cores=8)
    assert res == 2.0


def test_streak_case2_near_miss_ignored():
    # Case: spike of 3 samples then idle then a 5-sample streak -> the streak, the near-miss
    join_ns = 1_000_000_000_000
    samples = [2, 2, 6, 6, 6, 2, 2, 6, 6, 6, 6, 6]
    lines = [
        f"{join_ns + i * 1_000_000_000} {val:.1f}"
        for i, val in enumerate(samples)
    ]
    # Near miss is at index 2..4 (length 3).
    # Actual 5-sample streak starts at index 7.
    res = cpu_sampler.find_first_work_s(lines, join_ns, cores=8)
    assert res == 7.0


def test_streak_case3_no_streak():
    # Case: no streak -> empty / None, logged
    join_ns = 1_000_000_000_000
    samples = [2, 3, 2, 6, 6, 2, 6, 6, 6, 6, 3, 2]  # longest streak is 4 (< 5)
    lines = [
        f"{join_ns + i * 1_000_000_000} {val:.1f}"
        for i, val in enumerate(samples)
    ]
    res = cpu_sampler.find_first_work_s(lines, join_ns, cores=8)
    assert res is None


def test_cpu_sampler_cli_two_reads(tmp_path):
    # Case: cpu_sampler.py, fake stat file, two reads -> busy_cores computed from deltas
    stat_file = tmp_path / "proc_stat"
    out_file = tmp_path / "samples.txt"

    # Initial state: 8 cores, 1000 total, 100 busy
    stat_file.write_text("cpu  100 0 0 900 0 0 0 0\n" + "".join(f"cpu{i} 12 0 0 113 0 0 0 0\n" for i in range(8)))

    env = os.environ.copy()
    env["LAB_PROC_STAT"] = str(stat_file)
    env["LAB_SAMPLE_INTERVAL_S"] = "0.05"
    env["LAB_CORES"] = "8"

    # Start cpu_sampler.py for 0.2 s in background
    proc = subprocess.Popen(
        [sys.executable, str(CPU_SAMPLER), str(out_file), "0.25"],
        env=env,
    )

    # Let it read the first state
    time.sleep(0.02)
    # Second state: delta busy = 400, delta total = 800 (50% busy -> 4.0 cores)
    stat_file.write_text("cpu  500 0 0 1300 0 0 0 0\n" + "".join(f"cpu{i} 62 0 0 163 0 0 0 0\n" for i in range(8)))

    proc.wait(timeout=5)
    assert out_file.exists()
    content = out_file.read_text().strip().splitlines()
    assert len(content) >= 1
    # Check that busy cores delta was computed
    first_sample = content[0].split()
    assert len(first_sample) == 2
    ns, busy = int(first_sample[0]), float(first_sample[1])
    assert ns > 0
    assert pytest.approx(busy, 0.1) == 4.0
