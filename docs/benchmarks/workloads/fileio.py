#!/usr/bin/env python3
"""The E20 file-I/O workload: BENCH_FILES files of BENCH_FILE_KB KB in the
working directory, then read back in order, then delete all.

Distributed: nothing; one process, local filesystem. E20 runs this with and
without the sandbox on the same machine.
Local: everything. The files are removed before exit so no workspace ships
them back. /tmp is not used: under crun it may be another filesystem, so it
would not compare like with like.
Result line, byte-for-byte as systems.sh expects:
  FILEIO-OK files=%d bytes=%d seconds=%.4f
"""
import os
import sys
import time


def _env_int(name, required=True, default=None):
    raw = os.environ.get(name, "")
    if raw == "":
        if required:
            sys.exit(f"fileio.py: {name} is not set")
        return default
    try:
        return int(raw)
    except ValueError:
        sys.exit(f"fileio.py: {name} is not an integer: {raw!r}")


def main():
    files = _env_int("BENCH_FILES")
    kb = _env_int("BENCH_FILE_KB")
    if files < 0 or kb < 0:
        sys.exit("fileio.py: BENCH_FILES and BENCH_FILE_KB must be >= 0")
    chunk = b"x" * (kb * 1024)
    total = files * len(chunk)
    written = 0
    paths = []
    t0 = time.perf_counter()
    try:
        for i in range(files):
            path = f"fileio-{i}.bin"
            paths.append(path)
            with open(path, "wb") as f:
                f.write(chunk)
                f.flush()
                os.fsync(f.fileno())
            written += len(chunk)
        for path in paths:
            with open(path, "rb") as f:
                back = f.read()
            if back != chunk:
                sys.exit(f"fileio.py: {path} read back {len(back)} bytes, expected {len(chunk)}")
        dt = time.perf_counter() - t0
    except OSError as e:
        name = e.filename if e.filename is not None else (paths[-1] if paths else ".")
        sys.stderr.write(f"fileio.py: cannot use {name}: {e}\n")
        raise SystemExit(1)
    finally:
        for path in paths:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
            except OSError as e:
                sys.stderr.write(f"fileio.py: warning: cannot remove {path}: {e}\n")
    print(f"FILEIO-OK files={files} bytes={written} seconds={dt:.4f}")


if __name__ == "__main__":
    main()
