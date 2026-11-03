"""The shim's TTL caches on a machine booted moments ago.

time.monotonic() counts from boot, so on a fresh VM it starts near zero. A
cache stamped 0.0 at import then looked fresh, and the first call returned the
placeholder instead of asking: no bandwidth probe, so all work stayed local.
"""
import importlib.util
import io
import json
import time
import urllib.request

import pytest

from pipedpeer_shim import core


def _fresh(name):
    """A new copy of a shim module, with its caches as they are at import
    (other tests have already filled the shared copy's)."""
    spec = importlib.util.find_spec("pipedpeer_shim." + name)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def young_clock(monkeypatch):
    """A clock 1 s after boot."""
    monkeypatch.setattr(time, "monotonic", lambda: 1.0)
    monkeypatch.setattr(core, "_URL", "http://127.0.0.1:1")
    monkeypatch.setattr(core, "_ENABLED", True)


def _reply(obj):
    return io.BytesIO(json.dumps(obj).encode())


def test_first_bandwidth_call_measures(young_clock, monkeypatch):
    transfer = _fresh("transfer")
    monkeypatch.setattr(transfer, "_daemon_link_speed", lambda: 7e8)
    assert transfer._measure_bandwidth() == 7e8


def test_first_gpu_question_asks_the_daemon(young_clock, monkeypatch):
    numpy_ops = _fresh("numpy_ops")
    peers = [{"state": "healthy", "source": "lan", "load": {"gpus": 1}}]
    monkeypatch.setattr(urllib.request, "urlopen", lambda req, timeout: _reply(peers))
    assert numpy_ops._cluster_has_gpu() is True


def test_first_ooc_batch_asks_the_daemon(young_clock, monkeypatch):
    pandas_ooc = _fresh("pandas_ooc")
    mem = 100 * 5 * pandas_ooc._OOC_CHUNK
    monkeypatch.setattr(pandas_ooc, "_daemon_open", lambda req, timeout: _reply({"available_mem": mem}))
    assert pandas_ooc._ooc_batch() == min(40, pandas_ooc._OOC_BATCH_MAX)
