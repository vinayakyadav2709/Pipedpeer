"""E11: cluster size. For each size in 8, 16, 32, 60 (capped at the
inventory): every daemon but pc00's is stopped, then all size-1 are started
at once (one Ansible fan-out), and join_s is the time until pc00's API lists
all of them healthy (driver clock, so it includes Ansible's start, about a
second). Then the cluster idles PL_E11_IDLE_S seconds (600; 30 in the
rehearsal) and every PC's membership and gossip traffic is read:

  nft   with passwordless sudo and nft (lab PCs after lab-pc.sh setup): a
        table pplab counting tcp/udp packets to or from ports 4242-4244,
        each packet once, removed afterwards;
  nic   otherwise (the containers): /proc/net/dev, every interface but lo,
        which includes anything else the PC sent (telemetry among it).

bytes_per_node_per_min is the median over the PCs of (rx+tx)/idle*60;
introducer_bytes is pc00's (every daemon introduces its own cluster). Then
one W1 run at the largest size.
"""
import json
import os
import statistics
import time

from campaign import probes

NFT_SETUP = (
    "sudo -n nft delete table inet pplab 2>/dev/null; "
    "sudo -n nft add table inet pplab && "
    "sudo -n nft add chain inet pplab in '{ type filter hook input priority 0; }' && "
    "sudo -n nft add chain inet pplab out '{ type filter hook output priority 0; }' && "
    "for c in in out; do "
    "sudo -n nft add rule inet pplab $c meta l4proto '{ tcp, udp }' th dport 4242-4244 counter accept && "
    "sudo -n nft add rule inet pplab $c meta l4proto '{ tcp, udp }' th sport 4242-4244 counter accept; done "
    "&& echo PLNFT ok")
NFT_READ = ("sudo -n nft -j list table inet pplab | tr -d '\\n' | "
            "grep -o '\"bytes\": *[0-9]*' | awk -F: '{s+=$2} END {printf \"PLNFT %d\\n\", s}'")
NFT_DROP = "sudo -n nft delete table inet pplab 2>/dev/null; true"


def per_host(out, hosts, tag):
    res = {}
    for line in out.splitlines():
        host, sep, rest = line.partition(": ")
        if len(hosts) == 1:
            host, rest = hosts[0].id, line
        parts = rest.split()
        if len(parts) == 2 and parts[0] == tag and parts[1].isdigit():
            res[host] = int(parts[1])
    return res


def main(ctx):
    pp = ctx.backend("pipedpeer")
    have = len(ctx.inv.all)
    sizes = sorted({min(s, have) for s in ctx.ns([8, 16, 32, 60])})
    idle = float(os.environ.get("PL_E11_IDLE_S", "30" if ctx.rehearsal else "600"))
    for size in sizes:
        rec = ctx.recorder
        run_id = rec.new_run_id()
        logdir = rec.log_dir(run_id)
        hosts = ctx.inv.all[:size]
        line = {"run_id": run_id, "workload": "E11-idle", "system": "pipedpeer", "n": size, "repeat": 1,
                "hosts": [h.id for h in hosts], "idle_s": idle, "log_path": os.path.relpath(logdir, ctx.out_dir)}
        ctx.log("E11 size %d: stopping every daemon but pc00's" % size)
        try:
            pp.activate(1)
            others = hosts[1:]
            t0 = time.time_ns()
            if others:
                pp.sh_ok(":".join(h.id for h in others), pp._start_cmd(None, {}), "pipedpeer start", timeout=180)
                for h in others:
                    ctx.daemons[h.id] = (None, ())
            pp.wait_nodes(size, timeout=max(ctx.wait_timeout, 600))
            t1 = time.time_ns()
        except Exception as e:
            line.update({"error": str(e), "failing_pc": getattr(e, "host", None), "result_ok": False})
            rec.write(line)
            continue
        line.update({"start_ns": t0, "end_ns": t1, "join_s": round((t1 - t0) / 1e9, 3)})
        pat = ":".join(h.id for h in hosts)
        rc, out = ctx.remote.sh(pat, NFT_SETUP, timeout=60)
        nft_ok = per_host(out.replace("PLNFT ok", "PLNFT 0"), hosts, "PLNFT")
        use_nft = len(nft_ok) == len(hosts)
        if not use_nft:
            ctx.remote.sh(pat, NFT_DROP, timeout=60)
        before = probes.netdev(ctx, hosts)
        ctx.log("E11 size %d: joined in %.1f s; idling %.0f s (bytes from %s)" % (
            size, line["join_s"], idle, "nft" if use_nft else "nic"))
        time.sleep(idle)
        if use_nft:
            rc, out = ctx.remote.sh(pat, NFT_READ, timeout=60)
            got = per_host(out, hosts, "PLNFT")
            ctx.remote.sh(pat, NFT_DROP, timeout=60)
        else:
            got = {h: rx + tx for h, (rx, tx) in probes.delta(before, probes.netdev(ctx, hosts)).items()}
        with open(os.path.join(logdir, "bytes.json"), "w") as f:
            json.dump(got, f, indent=1, sort_keys=True)
        rates = [b / idle * 60 for b in got.values()]
        line.update({"bytes_source": "nft" if use_nft else "nic", "bytes_per_pc": got,
                     "bytes_per_node_per_min": round(statistics.median(rates), 1) if rates else None,
                     "introducer_bytes": got.get(ctx.inv.head.id),
                     "result_ok": len(got) == len(hosts)})
        if len(got) != len(hosts):
            line["error"] = "no byte count from %s" % ",".join(h.id for h in hosts if h.id not in got)
        rec.write(line)
    wl = ctx.workloads[ctx.wl_ids(["W1"])[0]]
    ctx.one_run(wl, "pipedpeer", sizes[-1], 1, extra={"note": "one W1 run at the largest size"})
