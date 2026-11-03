"""runs.jsonl: one JSON line per run, failed runs included, appended and
flushed as each run ends so a crash loses at most the run in flight."""
import json
import os
import subprocess
import time
import uuid

FIELDS = [
    "run_id", "experiment", "workload", "system", "n", "repeat", "warmup",
    "start_ns", "end_ns", "wall_s", "result", "result_ok", "hosts",
    "git_commit", "pipedpeer_version",
]


def git_commit(repo: str) -> str:
    try:
        return subprocess.run(["git", "-C", repo, "rev-parse", "HEAD"], capture_output=True,
                              text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


class ResultCache:
    """The plain run's result per (workload, size), kept across invocations
    in results/lab/result-cache.json."""

    def __init__(self, path: str):
        self.path = path
        self.data = {}
        if os.path.exists(path):
            with open(path) as f:
                self.data = json.load(f)

    @staticmethod
    def key(workload: str, size: dict) -> str:
        return workload + "|" + ",".join("%s=%s" % kv for kv in sorted(size.items()))

    def get(self, workload, size):
        return self.data.get(self.key(workload, size))

    def put(self, workload, size, result, source):
        self.data[self.key(workload, size)] = {"result": result, "source": source}
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.data, f, indent=1, sort_keys=True)
        os.replace(tmp, self.path)


class Recorder:
    def __init__(self, out_dir: str, experiment: str, repo: str, pipedpeer_version: str,
                 rehearsal: bool = False):
        self.out_dir = out_dir
        self.experiment = experiment
        self.rehearsal = rehearsal
        os.makedirs(os.path.join(out_dir, "logs"), exist_ok=True)
        self.path = os.path.join(out_dir, "runs.jsonl")
        self.cache = ResultCache(os.path.join(out_dir, "result-cache.json"))
        self.git_commit = git_commit(repo)
        self.pipedpeer_version = pipedpeer_version
        self.count = 0
        self.failed = 0

    def new_run_id(self) -> str:
        return "%s-%s-%s" % (self.experiment, time.strftime("%Y%m%dT%H%M%S"), uuid.uuid4().hex[:6])

    def log_dir(self, run_id: str) -> str:
        d = os.path.join(self.out_dir, "logs", run_id)
        os.makedirs(d, exist_ok=True)
        return d

    def write(self, rec: dict) -> dict:
        line: dict = {k: None for k in FIELDS}
        line.update({"experiment": self.experiment, "git_commit": self.git_commit,
                     "pipedpeer_version": self.pipedpeer_version, "warmup": False})
        if self.rehearsal:
            line["rehearsal"] = True
        line.update(rec)
        if line.get("start_ns") and line.get("end_ns") and line.get("wall_s") is None:
            line["wall_s"] = round((line["end_ns"] - line["start_ns"]) / 1e9, 4)
        with open(self.path, "a") as f:
            f.write(json.dumps(line, sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())
        self.count += 1
        if line.get("error"):
            self.failed += 1
        return line
