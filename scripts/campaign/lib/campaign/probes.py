"""Small measurements taken on the PCs through the same Ansible fan-out."""
import shlex

# Bytes received / sent on every interface but lo, from /proc/net/dev, in
# plain sh: the lab image has no awk.
NETDEV = ('r=0; t=0; while read -r l; do case "$l" in *:*) n=$(echo ${l%%:*}); '
          '[ "$n" = lo ] && continue; set -- ${l#*:}; r=$((r + $1)); t=$((t + $9));; esac; '
          'done < /proc/net/dev; echo "PLNET $r $t"')


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


# Which store paths a PC already holds, to prove a cold ship was cold
# (E5): a listing of /nix/store before the run, kept on the PC, then the
# run's closure counted against it there, so only counts cross Ansible.
STORE_LIST = "$HOME/.local/share/pipedpeer-lab/store-before.txt"
NIX_STORE = ('for x in "$(command -v nix-store)" /nix/var/nix/profiles/default/bin/nix-store '
             '"$HOME/.nix-profile/bin/nix-store"; do [ -x "$x" ] && exec "$x" -qR "$1"; done; exit 127')


def _per_host(hosts, out, tag):
    res = {}
    for line in out.splitlines():
        if len(hosts) == 1:
            host, rest = hosts[0].id, line
        else:
            host, _, rest = line.partition(": ")
        parts = rest.split()
        if len(parts) == 2 and parts[0] == tag and parts[1].isdigit():
            res[host] = int(parts[1])
    return res


def store_snapshot(ctx, hosts):
    """{host: store entries}: each PC lists /nix/store (no .drv) into STORE_LIST."""
    cmd = ('mkdir -p "$(dirname %s)"; ls /nix/store 2>/dev/null | grep -v "\\.drv$" >%s; '
           'echo "PLSTORE $(wc -l <%s)"') % (STORE_LIST, STORE_LIST, STORE_LIST)
    rc, out = ctx.remote.sh(":".join(h.id for h in hosts), cmd, timeout=120)
    return _per_host(hosts, out, "PLSTORE")


def closure(ctx, host, store_path):
    """The store path's closure (basenames) as host's nix-store lists it; [] if it cannot."""
    rc, out = ctx.remote.sh(host, "sh -c %s _ %s" % (shlex.quote(NIX_STORE), shlex.quote(store_path)), timeout=120)
    if rc != 0:
        return []
    return sorted({ln.strip().rsplit("/", 1)[-1] for ln in out.splitlines() if ln.strip().startswith("/nix/store/")})


def present_before(ctx, hosts, names):
    """{host: how many of names were in its STORE_LIST}."""
    if not names:
        return {}
    lst = STORE_LIST.replace("store-before.txt", "closure.txt")
    cmd = "printf '%%s\\n' %s >%s; echo \"PLPRESENT $(grep -cxFf %s %s || true)\"" % (
        " ".join(shlex.quote(n) for n in names), lst, lst, STORE_LIST)
    rc, out = ctx.remote.sh(":".join(h.id for h in hosts), cmd, timeout=120)
    return _per_host(hosts, out, "PLPRESENT")
