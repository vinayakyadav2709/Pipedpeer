"""E15: network sensitivity. W1 at N=4 (strong size) on pipedpeer, ray and
dask with 0, 20 and 80 ms of round-trip time added between the PCs
(PL_NETEM_RTTS). Each PC that any of the three uses at N (pc00..pcN) gets
`tc qdisc replace dev <lab interface> root netem delay <RTT/2>ms` on the
interface it reaches the others through, so every pair sees the full RTT
added. One warm-up per system at 0 ms, then PL_NETEM_REPEATS (2) runs per
RTT in a random order; each line has rtt_added_ms and netem_hosts.

The qdisc is root's (sudo -n, which lab.sh grants). Each PC that got one has
$STATE/netem.dev naming the interface; the experiment removes every qdisc it
added when it ends or fails, removes any left by a cut-off attempt before it
starts, and day/cleanup-host.sh removes it again from any PC with the file.
A PC whose root qdisc is not a kernel default (something we did not put
there) is left alone, and that RTT's runs are recorded as failed.
"""
import os

from campaign import systems

MARK = systems.STATE + "/netem.dev"
SUDO = '$( [ "$(id -u)" = 0 ] || echo sudo -n )'


def _dev(peer_ip):
    # No awk or sed on every PC: split ip's words in sh.
    return ('set -- $(ip -4 route get %s); dev=; while [ $# -gt 0 ]; do '
            '[ "$1" = dev ] && { dev=$2; break; }; shift; done; [ -n "$dev" ] || { echo PLNETEM nodev; exit 3; }'
            % peer_ip)


def clear_cmd():
    return ('if [ -f %(m)s ]; then d=$(cat %(m)s); %(s)s tc qdisc del dev "$d" root 2>/dev/null; rm -f %(m)s; fi; '
            'if tc qdisc show 2>/dev/null | grep -q netem; then echo PLNETEM left; else echo PLNETEM clear; fi'
            % {"m": MARK, "s": SUDO})


def set_cmd(peer_ip, half_ms):
    return ('%(dev)s; cur=$(tc qdisc show dev "$dev" root 2>/dev/null); '
            'case "$cur" in *netem*|*noqueue*|*fq_codel*|*pfifo_fast*|"qdisc mq "*|"qdisc fq "*|"") ;; '
            '*) echo "PLNETEM refused $cur"; exit 3 ;; esac; '
            'mkdir -p %(st)s && echo "$dev" > %(m)s && '
            '%(s)s tc qdisc replace dev "$dev" root netem delay %(d)sms && echo "PLNETEM set $dev"'
            % {"dev": _dev(peer_ip), "st": systems.STATE, "m": MARK, "s": SUDO, "d": half_ms})


def clear_all(ctx, hosts):
    left = []
    for h in hosts:
        rc, out = ctx.remote.sh(h.id, clear_cmd(), timeout=60)
        if "PLNETEM clear" not in out:
            left.append(h.id)
    if left:
        ctx.log("WARNING: a netem qdisc is still on %s" % ", ".join(left))
    return left


def set_all(ctx, hosts, rtt_ms):
    """Put RTT/2 on every host; raises StepError naming the first PC that
    refused (the others are cleared again by the caller's finally)."""
    if rtt_ms == 0:
        left = clear_all(ctx, hosts)
        if left:
            raise systems.StepError("netem still on %s" % ", ".join(left), host=left[0])
        return
    for i, h in enumerate(hosts):
        peer = hosts[1] if i == 0 else hosts[0]
        rc, out = ctx.remote.sh(h.id, set_cmd(peer.ip, "%g" % (rtt_ms / 2.0)), timeout=60)
        if rc != 0 or "PLNETEM set" not in out:
            raise systems.StepError("netem on %s failed: %s" % (h.id, out.strip()[-200:]), host=h.id)


def main(ctx):
    n = ctx.ns([4])[-1]
    rtts = [int(x) for x in os.environ.get("PL_NETEM_RTTS", "0,20,80").split(",") if x]
    reps = int(os.environ.get("PL_NETEM_REPEATS", "1" if ctx.rehearsal else "2"))
    wl = ctx.workloads[ctx.wl_ids(["W1"])[0]]
    hosts = ctx.inv.all[:n + 1]
    chosen = [s for s in ["pipedpeer", "ray", "dask"] if ctx.enabled(s)]
    clear_all(ctx, hosts)  # a cut-off attempt may have left some
    try:
        for system in ctx.rng.sample(chosen, len(chosen)):
            clear_all(ctx, hosts)  # the last system may have ended at 80 ms
            ctx.one_run(wl, system, n, 0, warmup=True, extra={"rtt_added_ms": 0})
            for rtt in ctx.rng.sample(rtts, len(rtts)):
                extra = {"rtt_added_ms": rtt, "netem_hosts": [h.id for h in hosts]}
                try:
                    set_all(ctx, hosts, rtt)
                except systems.StepError as e:
                    ctx.log("netem %d ms: %s; its runs are recorded as failed" % (rtt, e))
                    clear_all(ctx, hosts)
                    for r in range(1, reps + 1):
                        ctx.recorder.write(dict(extra, workload=wl.id, system=system, n=n, repeat=r,
                                                error=str(e), failing_pc=e.host, result_ok=False))
                    continue
                for r in range(1, reps + 1):
                    ctx.one_run(wl, system, n, r, extra=extra)
    finally:
        clear_all(ctx, hosts)
