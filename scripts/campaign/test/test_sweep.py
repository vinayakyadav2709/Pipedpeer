"""Case table for the SW1 workloads (sweep_map, noop_map, lat_map, fileio).

Every test maps to a row of step 5's SW1 case table; the Ray/Dask/ssh port
checksums are rows in test/ports_wsweep.sh, because they need a cluster (or a
second ssh target) that this file does not start.
"""

import importlib.util
import math
import os
import re
import subprocess
import sys
import types
import zlib
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
WORKLOADS = REPO / "docs" / "benchmarks" / "workloads"
BASELINES = WORKLOADS / "baselines"
SWEEP = WORKLOADS / "sweep_map.py"
NOOP = WORKLOADS / "noop_map.py"
LAT = WORKLOADS / "lat_map.py"
FILEIO = WORKLOADS / "fileio.py"
SSH_PART = BASELINES / "sweep_ssh_part.py"

SWEEP_OK = re.compile(
    r"^SWEEP-OK items=(\d+) seconds=([0-9.]+) checksum=([0-9.]+)$", re.M
)
NOOP_OK = re.compile(
    r"^NOOP-OK items=(\d+) seconds=([0-9.]+) tasks_per_s=([0-9.]+)$", re.M
)
LAT_OK = re.compile(
    r"^LAT-OK n=(\d+) p50_ms=([0-9.]+) p90_ms=([0-9.]+) p99_ms=([0-9.]+)$", re.M
)
FILEIO_OK = re.compile(
    r"^FILEIO-OK files=(\d+) bytes=(\d+) seconds=([0-9.]+)$", re.M
)


def _run(script, env, cwd=None, timeout=120):
    full = {k: v for k, v in os.environ.items() if not k.startswith("BENCH_")}
    full.update(env)
    return subprocess.run(
        [sys.executable, str(script)],
        cwd=cwd,
        env=full,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _module(name, path, monkeypatch=None):
    """Load a workload/baseline module without importing its cluster library."""
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _expected(items, iters):
    total = 0.0
    for i, payload in items:
        acc = 0.0
        for k in range(1, 1 + iters):
            acc += (i + k) ** 0.5
        if payload:
            acc += zlib.crc32(payload)
        total += acc
    return total


# --- sweep_map --------------------------------------------------------------


def test_sweep_four_items_checksum_is_hand_arithmetic():
    r = _run(
        SWEEP,
        {
            "BENCH_ITEMS": "4",
            "BENCH_TASK_MS": "1",
            "BENCH_ITERS_PER_MS": "1",
            "BENCH_PROCS": "1",
            "BENCH_PAYLOAD_BYTES": "0",
        },
    )
    assert r.returncode == 0, r.stderr
    m = SWEEP_OK.search(r.stdout)
    assert m, r.stdout
    assert int(m.group(1)) == 4
    # one arithmetic term per item: sqrt(i + 1) for i = 0..3
    hand = math.sqrt(1) + math.sqrt(2) + math.sqrt(3) + math.sqrt(4)
    assert float(m.group(3)) == pytest.approx(hand, abs=1e-6)


def test_sweep_payloads_are_distinct_objects():
    sweep = _module("sweep_map_sw1", SWEEP)
    items = sweep.build_items(2, 2)
    assert items[0][1] != items[1][1]
    assert items[0][1] is not items[1][1]


def test_sweep_payload_checksum_adds_crc():
    env = {
        "BENCH_ITEMS": "4",
        "BENCH_TASK_MS": "1",
        "BENCH_ITERS_PER_MS": "1",
        "BENCH_PROCS": "1",
        "BENCH_PAYLOAD_BYTES": "0",
    }
    plain = _run(SWEEP, env)
    assert plain.returncode == 0, plain.stderr
    m0 = SWEEP_OK.search(plain.stdout)
    assert m0

    env["BENCH_PAYLOAD_BYTES"] = "1000"
    with_payload = _run(SWEEP, env)
    assert with_payload.returncode == 0, with_payload.stderr
    m1 = SWEEP_OK.search(with_payload.stdout)
    assert m1
    assert float(m1.group(3)) != pytest.approx(float(m0.group(3)), abs=1e-9)

    crc = sum(zlib.crc32(bytes([i % 251]) * 1000) for i in range(4))
    assert float(m1.group(3)) == pytest.approx(float(m0.group(3)) + crc, abs=1e-6)


def test_sweep_missing_task_ms_exits_naming_it():
    env = {
        "BENCH_ITEMS": "1",
        "BENCH_ITERS_PER_MS": "1",
        "BENCH_PROCS": "1",
        "BENCH_PAYLOAD_BYTES": "0",
    }
    r = _run(SWEEP, env)
    assert r.returncode != 0
    assert "BENCH_TASK_MS" in r.stderr


# --- batching (the Ray rule, checked without a Ray cluster) -----------------


def _load_sweep_ray(monkeypatch):
    fake = types.ModuleType("ray")

    def _remote(fn):
        return fn

    fake.remote = _remote
    fake.init = lambda **kwargs: None
    monkeypatch.setitem(sys.modules, "ray", fake)
    try:
        return _module("sweep_ray_sw1", BASELINES / "sweep_ray.py")
    finally:
        sys.modules.pop("ray", None)


class _FakeRemote:
    def __init__(self):
        self.calls = []

    def remote(self, chunk):
        self.calls.append(chunk)
        return chunk


def test_ray_batch_is_ceil_100_over_task_ms(monkeypatch):
    ray = _load_sweep_ray(monkeypatch)
    items = [(i, b"") for i in range(250)]
    assert len(ray.split_batches(items, max(1, math.ceil(100 / 1)))) == 3
    assert len(ray.split_batches(items, max(1, math.ceil(100 / 1000)))) == 250
    fake = _FakeRemote()
    chunks = ray.split_batches(items, 100)
    assert ray.submit_batches(fake, chunks) == chunks
    assert len(fake.calls) == 3


def test_ray_one_item_per_task_when_task_is_long(monkeypatch):
    ray = _load_sweep_ray(monkeypatch)
    items = [(i, b"") for i in range(12)]
    batch = max(1, math.ceil(100 / 1000))
    chunks = ray.split_batches(items, batch)
    assert batch == 1
    assert len(chunks) == 12
    assert all(len(c) == 1 for c in chunks)


# --- sweep_ssh_part ---------------------------------------------------------


def _run_part(part, parts, items, payload_bytes, payload_file, tmp_path):
    env = {
        "BENCH_ITEMS": str(items),
        "BENCH_TASK_MS": "1",
        "BENCH_ITERS_PER_MS": "1",
        "BENCH_PROCS": "1",
        "BENCH_PAYLOAD_BYTES": str(payload_bytes),
    }
    cmd = [
        sys.executable,
        str(SSH_PART),
        "--part",
        str(part),
        "--parts",
        str(parts),
    ]
    if payload_file is not None:
        cmd += ["--payload-file", str(payload_file)]
    full = {k: v for k, v in os.environ.items() if not k.startswith("BENCH_")}
    full.update(env)
    return subprocess.run(
        cmd, cwd=tmp_path, env=full, capture_output=True, text=True, timeout=60
    )


def test_ssh_part_reads_the_file_it_was_sent(tmp_path):
    sweep = _module("sweep_map_part_sw1", SWEEP)
    items = sweep.build_items(6, 2)
    part_items = [items[i] for i in range(0, 6, 2)]
    pfile = tmp_path / "part0.bin"
    pfile.write_bytes(b"".join(payload for _, payload in part_items))

    r = _run_part(0, 2, 6, 2, pfile, tmp_path)
    assert r.returncode == 0, r.stderr
    import json

    row = json.loads(r.stdout.strip())
    assert row["part"] == 0
    assert row["items"] == 3
    assert row["out"] == pytest.approx([_expected([it], 1) for it in part_items])

    # Removing the shipped file must fail the part, not regenerate the payload.
    pfile.unlink()
    gone = _run_part(0, 2, 6, 2, pfile, tmp_path)
    assert gone.returncode != 0
    assert str(pfile) in gone.stderr


# --- noop_map ---------------------------------------------------------------


def test_noop_100_items_and_rate():
    r = _run(
        NOOP,
        {"BENCH_ITEMS": "100", "BENCH_PROCS": "2"},
    )
    assert r.returncode == 0, r.stderr
    m = NOOP_OK.search(r.stdout)
    assert m, r.stdout
    assert int(m.group(1)) == 100
    seconds = float(m.group(2))
    assert float(m.group(3)) == pytest.approx(100 / seconds, rel=1e-3)


# --- lat_map ----------------------------------------------------------------


def test_lat_20_calls_and_percentile_order():
    r = _run(
        LAT,
        {"BENCH_CALLS": "20", "BENCH_PROCS": "2"},
    )
    assert r.returncode == 0, r.stderr
    m = LAT_OK.search(r.stdout)
    assert m, r.stdout
    assert int(m.group(1)) == 20
    p50, p90, p99 = (float(m.group(i)) for i in (2, 3, 4))
    assert p50 <= p90 <= p99


def test_lat_percentile_of_one_is_that_value():
    lat = _module("lat_map_sw1", LAT)
    assert lat.percentile([7.5], 0.50) == 7.5
    assert lat.percentile([7.5], 0.90) == 7.5
    assert lat.percentile([7.5], 0.99) == 7.5


# --- fileio -----------------------------------------------------------------


def test_fileio_four_files_and_no_leftovers(tmp_path):
    r = _run(
        FILEIO,
        {"BENCH_FILES": "4", "BENCH_FILE_KB": "1"},
        cwd=tmp_path,
    )
    assert r.returncode == 0, r.stderr
    m = FILEIO_OK.search(r.stdout)
    assert m, r.stdout
    assert int(m.group(1)) == 4
    assert int(m.group(2)) == 4096
    assert list(tmp_path.iterdir()) == []


def test_fileio_read_only_dir_names_the_path(tmp_path):
    ro = tmp_path / "ro"
    ro.mkdir()
    ro.chmod(0o500)
    try:
        r = _run(FILEIO, {"BENCH_FILES": "2", "BENCH_FILE_KB": "1"}, cwd=ro)
    finally:
        ro.chmod(0o700)
    assert r.returncode != 0
    assert "fileio-" in r.stderr
