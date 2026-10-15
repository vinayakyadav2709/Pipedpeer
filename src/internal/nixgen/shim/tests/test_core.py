from pipedpeer_shim import core


def _cgroup(tmp_path, own, quotas):
    """A fake cgroup v2 tree under tmp_path; quotas maps a path below the
    root ("" = the root) to its cpu.max line."""
    root = tmp_path / "cgroup"
    (root / own.strip("/")).mkdir(parents=True)
    for rel, line in quotas.items():
        (root / rel).joinpath("cpu.max").write_text(line + "\n")
    return str(root)


def test_cpu_quota_takes_the_tightest_ancestor(tmp_path):
    root = _cgroup(tmp_path, "/a/b", {"": "max 100000", "a": "250000 100000", "a/b": "max 100000"})
    assert core._cpu_quota(root, "/a/b") == 3     # 2.5 cores, rounded up


def test_cpu_quota_none_without_a_limit(tmp_path):
    root = _cgroup(tmp_path, "/a", {"a": "max 100000"})
    assert core._cpu_quota(root, "/a") is None


def test_usable_cores_honours_the_quota(monkeypatch, tmp_path):
    root = _cgroup(tmp_path, "/", {"": "200000 100000"})   # a --cpus=2 container
    monkeypatch.setattr(core.os, "sched_getaffinity", lambda pid: set(range(16)))
    monkeypatch.delenv("PIPEDPEER_CPU_LIMIT", raising=False)
    real = core._cpu_quota
    monkeypatch.setattr(core, "_cpu_quota", lambda: real(root, "/"))
    assert core._usable_cores() == 2


def test_usable_cores_honours_the_daemons_limit(monkeypatch):
    monkeypatch.setattr(core.os, "sched_getaffinity", lambda pid: set(range(16)))
    monkeypatch.setattr(core, "_cpu_quota", lambda: None)   # sandbox: no /sys
    monkeypatch.setenv("PIPEDPEER_CPU_LIMIT", "1.5")
    assert core._usable_cores() == 2
    monkeypatch.setenv("PIPEDPEER_CPU_LIMIT", "bogus")
    assert core._usable_cores() == 16


def test_log_only_under_verbose_warn_always(monkeypatch, capsys):
    monkeypatch.setattr(core, "_ENABLED", True)
    monkeypatch.setattr(core, "_VERBOSE", False)
    core._log("detail")
    core._warn("act now")
    assert capsys.readouterr().err == "pipedpeer: act now\n"
    monkeypatch.setattr(core, "_VERBOSE", True)
    core._log("detail")
    assert capsys.readouterr().err == "[pipedpeer] detail\n"
