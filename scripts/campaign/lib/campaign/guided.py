"""The guided timer for the hand-timed tasks (E3, E10): it prints the
steps, takes the start and stop from Enter presses, asks for the counts the
operator observed, and appends one JSON line per attempt to runs.jsonl.
Reads stdin only, so a test (or the rehearsal) can feed it a script."""
import os
import subprocess
import sys
import time

from . import record

REPO = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", ".."))


def ask(prompt, out=sys.stdout, inp=sys.stdin):
    out.write(prompt)
    out.flush()
    line = inp.readline()
    if not line:
        raise EOFError(prompt.strip())
    return line.strip()


def ask_int(prompt, **kw):
    while True:
        s = ask(prompt, **kw)
        if s.isdigit():
            return int(s)
        kw.get("out", sys.stdout).write("  a whole number, please\n")


def local_version():
    try:
        return subprocess.run(["pipedpeer", "--version"], capture_output=True, text=True,
                              timeout=10).stdout.strip() or "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def recorder(out_dir, experiment, rehearsal):
    return record.Recorder(out_dir, experiment, REPO, local_version(), rehearsal=rehearsal)


def attempt(rec, system, steps, participant, order, extra=None, out=sys.stdout, inp=sys.stdin):
    """One timed attempt; returns the line written."""
    out.write("\n=== %s: %s (participant %s) ===\n" % (rec.experiment.upper(), system, participant))
    for i, s in enumerate(steps, 1):
        out.write("  %d. %s\n" % (i, s))
    ask("Press Enter to START the clock... ", out=out, inp=inp)
    start = time.time_ns()
    done = ask("Press Enter when W1 runs on both PCs (type 'fail' then Enter to give up)... ",
               out=out, inp=inp)
    end = time.time_ns()
    ok = done.lower() not in ("fail", "f", "n", "no")
    line = {"run_id": rec.new_run_id(), "workload": "W1", "system": system, "n": 2, "repeat": 1,
            "participant": participant, "order": order, "start_ns": start, "end_ns": end,
            "minutes": round((end - start) / 6e10, 2), "success": ok, "result_ok": ok,
            "commands_typed": ask_int("Commands typed (count): ", out=out, inp=inp),
            "script_lines_changed": ask_int("Script lines changed: ", out=out, inp=inp),
            "notes": ask("Where stuck / notes (no names): ", out=out, inp=inp)}
    if not ok:
        line["error"] = "gave up"
    line.update(extra or {})
    rec.write(line)
    out.write("recorded: %.1f min, %s\n" % (line["minutes"], "success" if ok else "FAILED"))
    return line
