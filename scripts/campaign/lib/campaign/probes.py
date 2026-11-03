"""Small measurements taken on the PCs through the same Ansible fan-out."""

# Bytes received / sent on every interface but lo, from /proc/net/dev.
NETDEV = ("awk 'NR>2 { sub(/:/, \" \"); if ($1 != \"lo\") { r += $2; t += $10 } } "
          "END { printf \"PLNET %d %d\\n\", r, t }' /proc/net/dev")


def netdev(ctx, hosts):
    """{host id: (rx bytes, tx bytes)}; hosts that did not answer are left out."""
    rc, out = ctx.remote.sh(":".join(h.id for h in hosts), NETDEV, timeout=60)
    res = {}
    single = len(hosts) == 1
    for line in out.splitlines():
        if single:
            host, rest = hosts[0].id, line
        else:
            host, _, rest = line.partition(": ")
        parts = rest.split()
        if len(parts) == 3 and parts[0] == "PLNET":
            res[host] = (int(parts[1]), int(parts[2]))
    return res


def delta(before, after):
    """{host: (rx, tx)} differences, only for hosts in both."""
    return {h: (after[h][0] - before[h][0], after[h][1] - before[h][1]) for h in after if h in before}
