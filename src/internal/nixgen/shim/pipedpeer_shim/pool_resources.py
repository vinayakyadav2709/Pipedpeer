"""Pool width and the memory/cgroup probes behind it."""
import os

from pipedpeer_shim import core
from pipedpeer_shim.core import (
    _log,
)


def _cgroup_stat(path, key):
    """One counter from a cgroup's memory.stat, or 0 if it cannot be read."""
    try:
        with open(path + "/memory.stat") as f:
            for line in f:
                name, _, value = line.partition(" ")
                if name == key:
                    return int(value)
    except (OSError, ValueError):
        pass
    return 0


def _cgroup_headroom():
    """What this process's cgroup still allows, or None if it is uncapped.

    The daemon runs under a memory ceiling so that a runaway pool cannot take
    the whole machine down with it. Without reading that ceiling here, the
    pool sizes itself against the machine's free memory, overshoots the cap,
    and the kernel kills a worker - a job failure caused by the very
    protection meant to prevent one. Reading it turns "killed" into "used
    fewer workers".
    """
    try:
        with open("/proc/self/cgroup") as f:
            for line in f:
                if line.startswith("0::"):
                    own = line[3:].strip()
                    break
            else:
                return None
    except OSError:
        return None

    # The limit can be set on any ancestor, and the binding one is the
    # smallest remaining headroom along the whole chain.
    best = None
    path = "/sys/fs/cgroup" + own
    while True:
        try:
            with open(path + "/memory.max") as f:
                raw = f.read().strip()
            if raw != "max":
                with open(path + "/memory.current") as f:
                    used = int(f.read().strip())
                # memory.current counts page cache, which the kernel reclaims
                # before it kills anything. Left in, every file a job reads
                # shrinks the next job's pool: measured 2.0GB of a worker's
                # 2.15GB "used" was inactive cache, against 33MB of real
                # memory, and the pool fell from 5 workers to 1 over five warm
                # runs. inactive_file is the part reclaimed first; current
                # minus it is the "working set" container runtimes report.
                used -= _cgroup_stat(path, "inactive_file")
                room = int(raw) - max(0, used)
                if best is None or room < best:
                    best = room
        except (OSError, ValueError):
            pass
        if path == "/sys/fs/cgroup" or "/" not in path[len("/sys/fs/cgroup"):]:
            break
        path = path.rsplit("/", 1)[0]
    return best


def _avail_bytes():
    """Memory this machine could actually give a new process.

    Not SC_AVPHYS_PAGES: that counts only wholly free pages and ignores the
    page cache, which the kernel hands back on demand. On any box with a warm
    cache it reads several times too low - measured 2GB free against 21GB
    available here - and every limit derived from it collapses. MemAvailable
    is the kernel's own answer to this question.

    Bounded by the cgroup's remaining headroom where there is one, because the
    machine having memory free does not mean this process is allowed to use
    it.
    """
    avail = 0
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    avail = int(line.split()[1]) * 1024
                    break
    except (OSError, ValueError, IndexError):
        pass
    if not avail:
        try:
            avail = os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
        except (ValueError, OSError):
            return 0
    room = _cgroup_headroom()
    if room is not None and room < avail:
        avail = max(0, room)
    room = _limit_headroom()
    if room is not None and room < avail:
        avail = max(0, room)
    return avail


def _private_bytes(pid):
    """Resident memory a process does not share (statm resident - shared)."""
    try:
        with open("/proc/%s/statm" % pid) as f:
            fields = f.read().split()
        return max(0, int(fields[1]) - int(fields[2])) * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return 0


def _limit_headroom():
    """What the job's memory cap still allows, or None without one.

    A sandboxed job has no /sys, so _cgroup_headroom finds nothing there and
    the daemon passes the cap as PIPEDPEER_MEM_LIMIT instead. Used is this
    process plus its children, measured as _workers_memory measures workers,
    so a resize that adds those back (reclaim) nets to this process alone.
    """
    raw = os.environ.get("PIPEDPEER_MEM_LIMIT", "").strip()
    if not raw:
        return None
    try:
        limit = int(raw)
    except ValueError:
        _log("PIPEDPEER_MEM_LIMIT=%r is not a byte count; ignored" % raw)
        return None
    import multiprocessing
    used = _private_bytes("self")
    for child in multiprocessing.active_children():
        used += _private_bytes(child.pid)
    return limit - used


def _imap_batch(procs):
    """Items to buffer before running a batch of a lazy imap."""
    override = os.environ.get("PIPEDPEER_IMAP_BATCH", "").strip()
    if override:
        try:
            return max(1, int(override))
        except ValueError:
            pass
    return max(64, procs * 32)


def _pool_width(requested, reclaim=0, workers=0):
    """How many local workers to run.

    A hand-picked process count is a guess about the machine the author had
    in front of them, so by default it is ignored in favour of what this
    machine can actually run: every core, bounded by free memory. Picking the
    number is the work this is supposed to remove.

    Some counts encode correctness rather than speed, though - a rate limit,
    a resource that is not safe to touch twice, a model that only fits a few
    times over - and those break rather than slow down when overridden.
    PIPEDPEER_RESPECT_POOL_SIZE=1 hands the decision back.

    reclaim is what the current pool's workers hold and workers how many
    there are: their average is the real per-worker cost (a torch import is
    ~300MB, not the 256MB floor) and sizes the next pool.
    """
    cores = core._usable_cores()
    if os.environ.get("PIPEDPEER_RESPECT_POOL_SIZE") == "1" and requested:
        return max(1, int(requested))
    n = cores
    avail = _avail_bytes()
    if avail > 0:
        # reclaim is what an existing pool's workers hold: rebuilding the
        # pool frees it, so it is memory a new width can count on.
        avail += reclaim
        # Nothing here knows the per-worker working set before the job runs,
        # so assume a conservative 256MB rather than none: a box with many
        # cores and little free memory must not fan out to all of them.
        per = 256 << 20
        if workers > 0:
            per = max(per, reclaim // workers)
        n = min(n, max(1, int(avail * 0.4) // per))
    if requested and int(requested) != n:
        _log("using %d workers, not the %d requested (cores=%d); "
             "set PIPEDPEER_RESPECT_POOL_SIZE=1 to keep your own count"
             % (n, int(requested), cores))
    return max(1, n)
