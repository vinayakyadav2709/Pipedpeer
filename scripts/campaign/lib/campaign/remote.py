"""Every command on a lab PC goes through lib/ansible.sh (Ansible's raw
module). Wall times are taken on the PC itself, so Ansible's own start-up is
never inside a measurement."""
import os
import re
import shlex
import subprocess
import time
from dataclasses import dataclass

HERE = os.path.dirname(os.path.abspath(__file__))
ANSIBLE_SH = os.path.normpath(os.path.join(HERE, "..", "ansible.sh"))
TIMES = re.compile(r"__PL_TIMES (\d+) (\d+) (-?\d+)\s*$", re.M)


@dataclass
class Timed:
    rc: int
    out: str
    start_ns: int
    end_ns: int

    @property
    def wall_s(self) -> float:
        return (self.end_ns - self.start_ns) / 1e9


class Remote:
    def __init__(self, inventory_path: str, log=print):
        self.inventory_path = inventory_path
        self.log = log

    def _call(self, args, timeout=None):
        env = dict(os.environ, PL_INVENTORY=self.inventory_path)
        try:
            p = subprocess.run(["bash", ANSIBLE_SH, *args], env=env, capture_output=True,
                               text=True, timeout=timeout)
        except subprocess.TimeoutExpired as e:
            out = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
            return 124, out + "\n[runner] ansible call timed out after %ss" % timeout
        out = p.stdout
        if p.returncode != 0 and p.stderr.strip():
            out += "\n" + p.stderr
        return p.returncode, out

    def sh(self, pattern: str, cmd: str, timeout: float = 600):
        """Run cmd on every host matching pattern. Returns (rc, output)."""
        # timeout on the PC as well: a hung job must not hang the campaign.
        remote = "timeout -k 10 %d sh -c %s" % (int(timeout), shlex.quote(cmd))
        return self._call(["sh", pattern, remote], timeout=timeout + 60)

    def timed(self, host: str, cmd: str, timeout: float = 1800) -> Timed:
        """Run cmd on one host, timing it there with its own clock."""
        wrapped = ("s=$(date +%%s%%N); timeout -k 10 %d sh -c %s; rc=$?; "
                   "e=$(date +%%s%%N); echo; echo \"__PL_TIMES $s $e $rc\"") % (
                       int(timeout), shlex.quote(cmd))
        t0 = time.time_ns()
        rc, out = self._call(["sh", host, wrapped], timeout=timeout + 120)
        m = None
        for m in TIMES.finditer(out):
            pass
        if m is None:
            t1 = time.time_ns()
            return Timed(rc if rc else 1, out, t0, t1)
        body = out[: m.start()].rstrip("\n")
        return Timed(int(m.group(3)), body, int(m.group(1)), int(m.group(2)))

    def bg(self, host: str, log: str, cmd: str):
        return self._call(["bg", host, log, cmd], timeout=120)

    def put(self, pattern: str, src: str, dest: str):
        return self._call(["put", pattern, src, dest], timeout=600)

    def timed_all(self, hosts, cmd: str, timeout: float = 3600) -> dict:
        """Run cmd on every host at once (one Ansible fan-out), each timed on
        its own clock. Returns {host id: Timed}; a host with no timing line
        failed before the command ran."""
        wrapped = ("s=$(date +%%s%%N); timeout -k 10 %d sh -c %s; rc=$?; "
                   "e=$(date +%%s%%N); echo; echo \"__PL_TIMES $s $e $rc\"") % (
                       int(timeout), shlex.quote(cmd))
        ids = [h.id for h in hosts]
        t0 = time.time_ns()
        rc, out = self._call(["sh", ":".join(ids), wrapped], timeout=timeout + 120)
        t1 = time.time_ns()
        per = {i: [] for i in ids}
        if len(ids) == 1:
            per[ids[0]] = out.splitlines()
        else:
            for line in out.splitlines():
                h, sep, rest = line.partition(": ")
                if sep and h in per:
                    per[h].append(rest)
        res = {}
        for i in ids:
            text = "\n".join(per[i])
            m = None
            for m in TIMES.finditer(text):
                pass
            if m is None:
                res[i] = Timed(rc or 1, text, t0, t1)
            else:
                res[i] = Timed(int(m.group(3)), text[: m.start()].rstrip("\n"), int(m.group(1)), int(m.group(2)))
        return res

    def start_job(self, host: str, log: str, cmd: str):
        """Start cmd detached on host; its output and a timing line go to log."""
        wrapped = ("s=$(date +%%s%%N); sh -c %s; rc=$?; e=$(date +%%s%%N); "
                   "echo; echo \"__PL_TIMES $s $e $rc\"") % shlex.quote(cmd)
        return self.bg(host, log, wrapped)

    def wait_job(self, host: str, log: str, timeout: float = 3600, poll: float = 2) -> Timed:
        deadline = time.time() + timeout
        while time.time() < deadline:
            rc, out = self.sh(host, "cat %s 2>/dev/null" % log, timeout=60)
            m = None
            for m in TIMES.finditer(out):
                pass
            if m:
                return Timed(int(m.group(3)), out[: m.start()].rstrip("\n"), int(m.group(1)), int(m.group(2)))
            time.sleep(poll)
        rc, out = self.sh(host, "cat %s 2>/dev/null" % log, timeout=60)
        return Timed(124, out + "\n[runner] job did not finish in %ss" % timeout, 0, 0)
