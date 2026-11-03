#!/usr/bin/env python3
"""Per-PC state poller for the lab campaign telemetry (node.sh starts it).

    pp-poll.py run   --logs DIR --lab-home DIR   poll until killed
    pp-poll.py facts --logs DIR                  one "facts" record, then exit

Writes JSON lines, one object per line, each with ts_ns (wall clock, ns),
kind, and run_id when $LAB_HOME/current-run names one:

  logs/pp-poll.jsonl     kind=api: one line per local daemon API route polled
                         (route, status, ms, body or error)
  logs/net.jsonl         kind=sockets: TCP/UDP sockets on the campaign ports
                         (ss -i fields: bytes, segments, rtt, cwnd, retrans);
                         kind=snmp: kernel Ip/Tcp/Udp/TcpExt counters
  logs/node-state.jsonl  kind=procs (campaign processes, full command lines,
                         on change), kind=clock (chronyc tracking, 30 s),
                         kind=facts (once), kind=dask-sd / ray-sd (scrape targets, on change)
  logs/sd/dask.json      Prometheus file_sd targets for the Dask scheduler
                         and the Dask workers on this PC
  logs/sd/ray.json       the Ray metrics agent on this PC, while it listens

The node collector (node.yaml.tmpl) tails all three. Standard library only;
any python3 >= 3.8 runs it. It never stops on an error: a stopped daemon is
one small "api" line a second saying so (the runner stops daemons to set N).
"""

import argparse
import fcntl
import http.client
import json
import os
import re
import shutil
import socket
import struct
import subprocess
import sys
import time
import urllib.request

ROTATE_BYTES = 256 << 20  # one old copy kept (.1); the collector reads both

# Local routes only. /v1/stats is left out on purpose: it asks every node in
# the cluster, so 60 pollers would add 60x60 requests a second to the very
# traffic E11 measures.
ROUTES = [  # (path, period in s)
    ("/health", 1),
    ("/v1/pool/stats", 1),
    ("/v1/jobs", 1),
    ("/v1/peers/unreachable", 1),
    ("/v1/nodes", 5),
    ("/v1/introducers", 5),
    ("/v1/reserved", 5),
    ("/v1/addresses", 30),
]

# Ports whose sockets net.jsonl records: pipedpeer (README, Runner integration
# has the Ray/Dask flags that pin theirs). Ranges are inclusive.
DEFAULT_PORTS = (
    "4242-4244,"  # pipedpeer API, discovery, direct, introducer
    "6379-6385,"  # ray: GCS, node manager, object manager, ...
    "8090,8265,"  # ray metrics export, dashboard
    "8786-8787,"  # dask scheduler, scheduler dashboard
    "9000-9199,"  # dask worker and nanny ports
    "10002-10999,"  # ray worker ports
    "29500"  # torchrun rendezvous
)

PROC_RE = re.compile(
    r"^(\.?(pipedpeer|python|ray|dask|torchrun|nix|otelcol).*|raylet|gcs_server|crun|tcpdump)$"
)


def now_ns():
    return time.time_ns()


class Sink:
    """Append-only JSON lines file with size rotation."""

    def __init__(self, path):
        self.path = path
        self.f = open(path, "a", buffering=1)

    def write(self, rec):
        line = json.dumps(rec, separators=(",", ":"), default=str)
        self.f.write(line + "\n")
        try:
            if self.f.tell() > ROTATE_BYTES:
                self.f.close()
                os.replace(self.path, self.path + ".1")
                self.f = open(self.path, "a", buffering=1)
        except OSError:
            pass


def read_text(path, default=""):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return default


class RunId:
    """$LAB_HOME/current-run, reread when its mtime changes."""

    def __init__(self, path):
        self.path, self.mtime, self.value = path, None, None

    def get(self):
        try:
            m = os.stat(self.path).st_mtime_ns
        except OSError:
            self.mtime, self.value = None, None
            return None
        if m != self.mtime:
            self.mtime = m
            self.value = read_text(self.path).strip() or None
        return self.value


# ---------------------------------------------------------------- daemon API


def daemon_state_dir():
    d = os.environ.get("PIPEDPEER_DAEMON_STATE_DIR")
    if d:
        return d
    base = os.environ.get("XDG_STATE_HOME") or os.path.expanduser("~/.local/state")
    return os.path.join(base, "pipedpeer")


def daemon_port():
    try:
        p = int(read_text(os.path.join(daemon_state_dir(), "daemon.port")).strip())
        if p > 0:
            return p
    except ValueError:
        pass
    return 4242


def daemon_token():
    # As scripts/lib/cli.sh pp_read_token: PIPEDPEER_TOKEN, else cluster.json.
    t = os.environ.get("PIPEDPEER_TOKEN")
    if t:
        return t
    base = os.environ.get("XDG_DATA_HOME") or os.path.expanduser("~/.local/share")
    try:
        with open(os.path.join(base, "pipedpeer", "cluster.json")) as f:
            return json.load(f).get("token") or ""
    except (OSError, ValueError):
        return ""


class Daemon:
    def __init__(self):
        self.conn = None
        self.port = None
        self.token = ""
        self.refreshed = 0.0

    def refresh(self):
        # The port and token change only when the daemon is set up again;
        # rereading every 10 s follows that without a stat per request.
        if time.monotonic() - self.refreshed < 10:
            return
        self.refreshed = time.monotonic()
        port, token = daemon_port(), daemon_token()
        if port != self.port:
            self.close()
        self.port, self.token = port, token

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def get(self, path):
        """(status, body bytes) or raises OSError/HTTPException."""
        if self.conn is None:
            self.conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=0.8)
        headers = {"X-Pipedpeer-Token": self.token} if self.token else {}
        try:
            self.conn.request("GET", path, headers=headers)
            r = self.conn.getresponse()
            return r.status, r.read()
        except Exception:
            self.close()
            raise


def poll_api(daemon, tick, sink, base):
    daemon.refresh()
    up = True
    for path, period in ROUTES:
        if tick % period:
            continue
        if not up:
            break  # /health failed: the rest would fail the same way
        t0 = time.monotonic()
        rec: dict = dict(base, kind="api", route=path, port=daemon.port)
        try:
            status, body = daemon.get(path)
            rec["status"] = status
            try:
                rec["body"] = json.loads(body)
            except ValueError:
                rec["body_text"] = body[:2000].decode("utf-8", "replace")
        except (OSError, http.client.HTTPException) as e:
            rec["error"] = f"{type(e).__name__}: {e}"
            up = False
        rec["ms"] = round((time.monotonic() - t0) * 1000, 2)
        sink.write(rec)


# ---------------------------------------------------------------- network


def parse_ports(spec):
    out = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        lo, _, hi = part.partition("-")
        out.append((int(lo), int(hi or lo)))
    return out


def ss_filter(ranges):
    terms = []
    for lo, hi in ranges:
        for side in ("sport", "dport"):
            if lo == hi:
                terms.append(f"{side} = :{lo}")
            else:
                terms.append(f"( {side} >= :{lo} and {side} <= :{hi} )")
    return " or ".join(terms)


SS_KV = re.compile(r"\b(bytes_sent|bytes_acked|bytes_received|bytes_retrans|segs_out|segs_in|data_segs_out|data_segs_in|"
                   r"rtt|rttvar|cwnd|ssthresh|mss|delivery_rate|send|pacing_rate|retrans|lost|unacked|"
                   r"notsent|minrtt|busy|rwnd_limited|sndbuf_limited|rcv_space|skmem):?([^\s]+)")


def ss_sockets(ranges):
    """Sockets on the campaign ports from `ss -i`, or None when ss is missing."""
    ss = shutil.which("ss")
    if not ss:
        return None
    try:
        out = subprocess.run(
            [ss, "-H", "-O", "-t", "-u", "-a", "-n", "-i", "-m", "-p", ss_filter(ranges)],
            capture_output=True, text=True, timeout=2,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    socks = []
    for line in out.splitlines():
        f = line.split()
        if len(f) < 6:
            continue
        s: dict = {"p": f[0], "st": f[1], "rq": int(f[2]), "sq": int(f[3]), "l": f[4], "r": f[5]}
        rest = " ".join(f[6:])
        m = re.search(r'users:\(\("([^"]*)",pid=(\d+)', rest)
        if m:
            s["comm"], s["pid"] = m.group(1), int(m.group(2))
        for k, v in SS_KV.findall(rest):
            s[k] = v
        socks.append(s)
    return socks


TCP_STATES = {"01": "ESTAB", "02": "SYN-SENT", "03": "SYN-RECV", "04": "FIN-WAIT-1", "05": "FIN-WAIT-2",
              "06": "TIME-WAIT", "07": "CLOSE", "08": "CLOSE-WAIT", "09": "LAST-ACK", "0A": "LISTEN", "0B": "CLOSING"}


def proc_sockets(ranges):
    """Fallback without ss: /proc/net/{tcp,udp}{,6}; queues and state only."""

    def addr(hexaddr):
        ip, port = hexaddr.split(":")
        port = int(port, 16)
        raw = bytes.fromhex(ip)
        if len(raw) == 4:
            return f"{socket.inet_ntop(socket.AF_INET, raw[::-1])}:{port}", port
        words = b"".join(raw[i:i + 4][::-1] for i in range(0, 16, 4))
        return f"[{socket.inet_ntop(socket.AF_INET6, words)}]:{port}", port

    def wanted(p):
        return any(lo <= p <= hi for lo, hi in ranges)

    socks = []
    for proto in ("tcp", "tcp6", "udp", "udp6"):
        for line in read_text(f"/proc/net/{proto}").splitlines()[1:]:
            f = line.split()
            if len(f) < 10:
                continue
            (la, lp), (ra, rp) = addr(f[1]), addr(f[2])
            if not (wanted(lp) or wanted(rp)):
                continue
            tx, rx = f[4].split(":")
            st = TCP_STATES.get(f[3], f[3]) if proto.startswith("tcp") else ("UNCONN" if f[3] == "07" else "ESTAB")
            socks.append({"p": proto[:3], "st": st, "rq": int(rx, 16), "sq": int(tx, 16), "l": la, "r": ra,
                          "inode": int(f[9])})
    return socks


SNMP_KEEP = {
    "Ip": {"InReceives", "InDiscards", "InDelivers", "OutRequests", "OutDiscards", "ReasmFails", "FragFails"},
    "Tcp": {"ActiveOpens", "PassiveOpens", "AttemptFails", "EstabResets", "CurrEstab", "InSegs", "OutSegs",
            "RetransSegs", "InErrs", "OutRsts", "InCsumErrors"},
    "Udp": None,  # all
    "UdpLite": set(),
    "TcpExt": {"ListenOverflows", "ListenDrops", "TCPLostRetransmit", "TCPTimeouts", "TCPSynRetrans",
               "TCPBacklogDrop", "TCPRcvQDrop", "TCPOFOQueue", "TCPRetransFail", "TCPFastRetrans",
               "TCPSlowStartRetrans", "TCPAbortOnTimeout", "TCPAbortOnData", "TCPAbortOnClose"},
    "IpExt": {"InOctets", "OutOctets", "InNoRoutes"},
}


def snmp_counters():
    out = {}
    for path in ("/proc/net/snmp", "/proc/net/netstat"):
        lines = read_text(path).splitlines()
        for head, vals in zip(lines[0::2], lines[1::2]):
            name, keys = head.split(":", 1)
            keep = SNMP_KEEP.get(name, set())
            sect = {}
            for k, v in zip(keys.split(), vals.split(":", 1)[1].split()):
                if keep is None or k in keep:
                    sect[k] = int(v)
            if sect:
                out[name] = sect
    return out


# ---------------------------------------------------------------- processes


def campaign_procs():
    procs = {}
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        comm = read_text(f"/proc/{pid}/comm").strip()
        if not PROC_RE.match(comm):
            continue
        cmd = read_text(f"/proc/{pid}/cmdline").replace("\0", " ").strip()
        stat = read_text(f"/proc/{pid}/stat")
        try:
            after = stat.rsplit(")", 1)[1].split()
            ppid, start_ticks = int(after[1]), int(after[19])
        except (IndexError, ValueError):
            ppid, start_ticks = 0, 0
        procs[int(pid)] = {"comm": comm, "ppid": ppid, "start_ticks": start_ticks, "cmd": cmd[:4096]}
    return procs


# ---------------------------------------------------------------- dask targets


def local_ips():
    ips = {"127.0.0.1", "localhost"}
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        for _, name in socket.if_nameindex():
            try:
                req = struct.pack("256s", name.encode()[:15])
                ips.add(socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, req)[20:24]))  # SIOCGIFADDR
            except OSError:
                pass
    finally:
        s.close()
    try:
        ips.add(socket.gethostbyname(socket.gethostname()))
    except OSError:
        pass
    return ips


def dask_targets(procs):
    """file_sd target groups for the local Dask scheduler and local workers.

    The scheduler's dashboard port comes from its command line (default 8787).
    Workers listen on a random dashboard port each, so ask the scheduler
    (/json/identity.json, on its dashboard port) and keep the workers whose
    host is this PC.
    """
    groups, sched_urls = [], set()
    for p in procs.values():
        cmd = p["cmd"]
        if "dask" not in cmd and "distributed" not in cmd:
            continue
        m = re.search(r"--dashboard-address[= ]\S*:(\d+)", cmd)
        if re.search(r"\b(dask(-wrapped)?[- ]scheduler|dask_scheduler)\b", cmd):
            port = m.group(1) if m else "8787"
            groups.append({"targets": [f"127.0.0.1:{port}"], "labels": {"dask_role": "scheduler"}})
            sched_urls.add(f"http://127.0.0.1:{port}")
        elif re.search(r"\b(dask(-wrapped)?[- ]worker|dask_worker)\b", cmd):
            a = re.search(r"(?:tcp|tls)://([^\s:/]+):\d+", cmd)
            if a:
                sched_urls.add(f"http://{a.group(1)}:8787")
    if not sched_urls:
        return groups
    mine, workers = local_ips(), []
    for url in sorted(sched_urls):
        try:
            with urllib.request.urlopen(url + "/json/identity.json", timeout=1) as r:
                ident = json.load(r)
        except (OSError, ValueError):
            continue
        for addr, w in (ident.get("workers") or {}).items():
            host = w.get("host") or addr.split("://")[-1].rsplit(":", 1)[0]
            port = (w.get("services") or {}).get("dashboard")
            if host in mine and port:
                workers.append(f"{host}:{port}")
        break
    if workers:
        groups.append({"targets": sorted(set(workers)), "labels": {"dask_role": "worker"}})
    return groups


def listen_addr(port):
    """The local address a TCP port listens on (0.0.0.0 -> 127.0.0.1), or None."""
    for proto in ("tcp", "tcp6"):
        for line in read_text(f"/proc/net/{proto}").splitlines()[1:]:
            f = line.split()
            if len(f) < 4 or f[3] != "0A":  # LISTEN
                continue
            ip, p = f[1].split(":")
            if int(p, 16) != port:
                continue
            raw = bytes.fromhex(ip)
            if len(raw) == 4:
                a = socket.inet_ntop(socket.AF_INET, raw[::-1])
                return f"{'127.0.0.1' if a == '0.0.0.0' else a}:{port}"
            return f"127.0.0.1:{port}" if not raw.strip(b"\0") else f"[::1]:{port}"
    return None


def ray_targets(port):
    """Ray's metrics agent on this PC, only while it listens: a target that is
    not running would make the collector log a failed scrape every interval."""
    a = listen_addr(port)
    return [{"targets": [a], "labels": {"ray_role": "node"}}] if a else []


def write_sd(path, groups):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(groups, f)
    os.replace(tmp, path)


# ---------------------------------------------------------------- clock, facts


def run(cmd, timeout=3):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip() if r.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def clock_state():
    rec = {}
    if shutil.which("chronyc"):
        # -c: CSV; fields per chronyc(1) "tracking": ref id, name, stratum,
        # ref time, system time offset (s), last offset, rms offset, freq ppm,
        # residual freq, skew, root delay, root dispersion, update interval, leap.
        out = run(["chronyc", "-c", "tracking"])
        if out:
            f = out.split(",")
            names = ["ref_id", "ref_name", "stratum", "ref_time", "system_time_s", "last_offset_s", "rms_offset_s",
                     "freq_ppm", "resid_freq_ppm", "skew_ppm", "root_delay_s", "root_dispersion_s",
                     "update_interval_s", "leap"]
            rec["chrony"] = dict(zip(names, f))
    if shutil.which("timedatectl"):
        out = run(["timedatectl", "show"])
        if out:
            rec["timedatectl"] = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
    return rec


def facts():
    rec: dict = {"uname": run(["uname", "-a"]), "nproc": os.cpu_count(), "hostname": socket.gethostname()}
    lscpu = run(["lscpu", "-J"])
    if lscpu:
        try:
            rec["lscpu"] = {e["field"].rstrip(":"): e["data"] for e in json.loads(lscpu)["lscpu"]}
        except (ValueError, KeyError, TypeError):
            rec["lscpu_text"] = run(["lscpu"])
    models = sorted({line.split(":", 1)[1].strip() for line in read_text("/proc/cpuinfo").splitlines()
                     if line.startswith("model name")})
    rec["cpu_models"] = models
    mem = read_text("/proc/meminfo").splitlines()
    rec["meminfo_kb"] = {line.split(":")[0]: int(line.split()[1]) for line in mem[:5] if len(line.split()) >= 2}
    nics = {}
    for nic in sorted(os.listdir("/sys/class/net")) if os.path.isdir("/sys/class/net") else []:
        d = f"/sys/class/net/{nic}"
        nics[nic] = {k: read_text(f"{d}/{k}").strip() for k in ("speed", "duplex", "mtu", "operstate", "address")}
        if shutil.which("ethtool"):
            nics[nic]["ethtool"] = run(["ethtool", nic])
    rec["nics"] = nics
    rec["os_release"] = dict(
        (k, v.strip('"')) for k, _, v in (ln.partition("=") for ln in read_text("/etc/os-release").splitlines()) if v
    )
    rec["kernel_cmdline"] = read_text("/proc/cmdline").strip()
    rec["clock"] = clock_state()
    pp = shutil.which("pipedpeer")
    rec["pipedpeer_version"] = run([pp, "--version"]) if pp else None
    rec["python"] = sys.version
    return rec


# ---------------------------------------------------------------- main loop


def main_run(args):
    logs = args.logs
    os.makedirs(os.path.join(logs, "sd"), exist_ok=True)
    api = Sink(os.path.join(logs, "pp-poll.jsonl"))
    net = Sink(os.path.join(logs, "net.jsonl"))
    state = Sink(os.path.join(logs, "node-state.jsonl"))
    sd_path = os.path.join(logs, "sd", "dask.json")
    ray_sd_path = os.path.join(logs, "sd", "ray.json")
    ray_port = int(os.environ.get("PIPEDPEER_LAB_RAY_METRICS_PORT", "8090"))
    for p in (sd_path, ray_sd_path):
        write_sd(p, [])
    last_ray = []
    run_id = RunId(os.path.join(args.lab_home, "current-run"))
    ranges = parse_ports(os.environ.get("PIPEDPEER_LAB_SOCK_PORTS", DEFAULT_PORTS))
    daemon = Daemon()
    last_procs, procs_at, last_sd, tick = None, 0, None, 0
    start = time.monotonic()
    while True:
        base: dict = {"ts_ns": now_ns()}
        rid = run_id.get()
        if rid:
            base["run_id"] = rid
        poll_api(daemon, tick, api, base)

        socks = ss_sockets(ranges)
        src = "ss"
        if socks is None:
            socks, src = proc_sockets(ranges), "proc"
        net.write(dict(base, kind="sockets", src=src, n=len(socks), sockets=socks))
        net.write(dict(base, kind="snmp", counters=snmp_counters()))

        if tick % 5 == 0:
            procs = campaign_procs()
            sig = {pid: (p["comm"], p["start_ticks"]) for pid, p in procs.items()}
            if sig != last_procs or time.monotonic() - procs_at > 60:
                state.write(dict(base, kind="procs", procs=procs))
                last_procs, procs_at = sig, time.monotonic()
            groups = dask_targets(procs)
            if groups != last_sd:
                write_sd(sd_path, groups)
                state.write(dict(base, kind="dask-sd", groups=groups))
                last_sd = groups
            groups = ray_targets(ray_port)
            if groups != last_ray:
                write_sd(ray_sd_path, groups)
                state.write(dict(base, kind="ray-sd", groups=groups))
                last_ray = groups
        if tick % 30 == 0:
            c = clock_state()
            if c:
                state.write(dict(base, kind="clock", **c))

        tick += 1
        # Ticks on whole seconds from start, so a slow poll does not drift.
        time.sleep(max(0.05, start + tick - time.monotonic()))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["run", "facts"])
    ap.add_argument("--logs", required=True)
    ap.add_argument("--lab-home", default=os.path.expanduser("~/.local/share/pipedpeer-lab"))
    args = ap.parse_args()
    if args.mode == "facts":
        os.makedirs(args.logs, exist_ok=True)
        base: dict = {"ts_ns": now_ns()}
        rid = RunId(os.path.join(args.lab_home, "current-run")).get()
        if rid:
            base["run_id"] = rid
        Sink(os.path.join(args.logs, "node-state.jsonl")).write(dict(base, kind="facts", **facts()))
        return
    try:
        main_run(args)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
