#!/usr/bin/env python3
"""Copy stdin to stdout line by line, stamping the first line's arrival.

The lab measures when a run produced its first result, not when it was
started: systemd-run, the shell and the interpreter all sit between the two,
and on a cold Nix closure that gap is the interesting part. `first_out_ns`
is written on the PC by this process, from the PC's own clock, as the run
prints its first line; the campaign's wall time is a separate pair of
`date +%s%N` reads around the command.

Usage: stamp.py <file for time.time_ns()>

Output is flushed per line so a reader tailing the file sees results as the
run produces them, and a killed run keeps what it printed.
"""

import sys
import time


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: stamp.py <first-line-ns-file>", file=sys.stderr)
        return 2
    path = sys.argv[1]
    stamped = False
    readline = sys.stdin.readline
    write = sys.stdout.write
    while True:
        line = readline()
        if not line:
            break
        if not stamped:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(f"{time.time_ns()}\n")
            stamped = True
        write(line)
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
