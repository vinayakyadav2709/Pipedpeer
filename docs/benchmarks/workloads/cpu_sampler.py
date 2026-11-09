#!/usr/bin/env python3
"""cpu_sampler.py: sample busy cores from /proc/stat every interval.

Usage:
    cpu_sampler.py <out> <duration_s>
    cpu_sampler.py --streak <samples_file> <join_ns> <cores>
"""

import os
import signal
import sys
import time


def parse_stat_line(line):
    """Parse one line of /proc/stat.

    Returns (cpu_name, busy_jiffies, total_jiffies) or None.
    """
    parts = line.strip().split()
    if not parts or not parts[0].startswith("cpu"):
        return None
    try:
        fields = [int(x) for x in parts[1:9]]
    except ValueError:
        return None
    if len(fields) < 4:
        return None
    # Fields: user, nice, system, idle, iowait, irq, softirq, steal
    total = sum(fields)
    idle = fields[3] + (fields[4] if len(fields) > 4 else 0)
    busy = total - idle
    return parts[0], busy, total


def read_proc_stat(stat_path):
    """Read stat file and return (aggregate_(busy, total), num_cores)."""
    if not os.path.exists(stat_path):
        return None
    with open(stat_path, "r") as f:
        content = f.read()

    agg = None
    cores = 0
    for line in content.splitlines():
        parsed = parse_stat_line(line)
        if not parsed:
            continue
        name, busy, total = parsed
        if name == "cpu":
            agg = (busy, total)
        elif name.startswith("cpu") and name[3:].isdigit():
            cores += 1

    env_cores = os.environ.get("LAB_CORES") or os.environ.get("CORES")
    if env_cores:
        try:
            cores = int(env_cores)
        except ValueError:
            pass
    if cores == 0:
        cores = os.cpu_count() or 1

    if agg is None:
        return None
    return agg, cores


def compute_busy_cores(prev_stat, curr_stat):
    """Compute number of busy cores from previous and current stat readings."""
    if not prev_stat or not curr_stat:
        return 0.0
    (p_busy, p_total), _ = prev_stat
    (c_busy, c_total), cores = curr_stat
    d_total = c_total - p_total
    d_busy = c_busy - p_busy
    if d_total <= 0:
        return 0.0
    ratio = max(0.0, min(1.0, d_busy / d_total))
    return ratio * cores


def find_first_work_s(sampler_lines, join_ns, cores, min_streak=5, threshold_ratio=0.5):
    """Find seconds from join_ns to the first sample of a streak of >= min_streak
    samples where busy_cores >= threshold_ratio * cores."""
    threshold = cores * threshold_ratio
    streak = []
    for line in sampler_lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        try:
            t_ns = int(parts[0])
            busy = float(parts[1])
        except ValueError:
            continue
        if t_ns < join_ns:
            # before the join: not this joiner's work; also breaks a streak
            streak = []
            continue
        if busy >= threshold:
            streak.append((t_ns, busy))
            if len(streak) == min_streak:
                first_t_ns = streak[0][0]
                diff_s = (first_t_ns - join_ns) / 1e9
                return round(diff_s, 3)
        else:
            streak = []
    return None


def main():
    if len(sys.argv) >= 2 and sys.argv[1] == "--streak":
        if len(sys.argv) < 5:
            sys.stderr.write("usage: cpu_sampler.py --streak <samples_file> <join_ns> <cores>\n")
            sys.exit(2)
        file_path = sys.argv[2]
        join_ns = int(sys.argv[3])
        cores = float(sys.argv[4])
        lines = []
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                lines = f.readlines()
        res = find_first_work_s(lines, join_ns, cores)
        if res is not None:
            print(res)
        sys.exit(0)

    if len(sys.argv) < 3:
        sys.stderr.write("usage: cpu_sampler.py <out> <duration_s>\n")
        sys.exit(2)

    out_path = sys.argv[1]
    duration_s = float(sys.argv[2])
    stat_path = os.environ.get("LAB_PROC_STAT", "/proc/stat")
    interval_s = float(os.environ.get("LAB_SAMPLE_INTERVAL_S", "1.0"))

    running = True

    def handle_signal(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    out_dir = os.path.dirname(os.path.abspath(out_path))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    prev = read_proc_stat(stat_path)
    start_time = time.time()

    while running:
        time.sleep(interval_s)
        curr = read_proc_stat(stat_path)
        if prev is not None and curr is not None:
            busy = compute_busy_cores(prev, curr)
            ns = time.time_ns()
            with open(out_path, "a") as f:
                f.write(f"{ns} {busy:.4f}\n")
        prev = curr
        if time.time() - start_time >= duration_s:
            break


if __name__ == "__main__":
    main()
