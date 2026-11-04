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
# run's closure matched against it there; only the matches come back.
STORE_LIST = "$HOME/.local/share/pipedpeer-lab/store-before.txt"
CLOSURE_LIST = "$HOME/.local/share/pipedpeer-lab/closure.txt"
NIX_STORE = ('n=; for x in "$(command -v nix-store)" /nix/var/nix/profiles/default/bin/nix-store '
             '"$HOME/.nix-profile/bin/nix-store"; do [ -x "$x" ] && n=$x && break; done; '
             '[ -n "$n" ] || exit 127; for p in $("$n" -qR "$1"); do echo "PLPATH $p $("$n" -q --size "$p")"; done')


def store_snapshot(ctx, hosts):
    """{host: store entries}: each PC lists /nix/store (no .drv) into STORE_LIST."""
    cmd = ('mkdir -p "$(dirname %s)"; ls /nix/store 2>/dev/null | grep -v "\\.drv$" >%s; '
           'echo "PLSTORE $(wc -l <%s)"') % (STORE_LIST, STORE_LIST, STORE_LIST)
    rc, out = ctx.remote.sh(":".join(h.id for h in hosts), cmd, timeout=120)
    res = {}
    for host, rest in _lines(hosts, out):
        parts = rest.split()
        if len(parts) == 2 and parts[0] == "PLSTORE" and parts[1].isdigit():
            res[host] = int(parts[1])
    return res


def closure(ctx, host, store_path):
    """{basename: nar bytes} of the store path's closure, as host's nix-store lists it; {} if it cannot."""
    rc, out = ctx.remote.sh(host, "sh -c %s _ %s" % (shlex.quote(NIX_STORE), shlex.quote(store_path)), timeout=300)
    res = {}
    for ln in out.splitlines():
        parts = ln.split()
        if len(parts) == 3 and parts[0] == "PLPATH" and parts[2].isdigit():
            res[parts[1].rsplit("/", 1)[-1]] = int(parts[2])
    return res


def present_before(ctx, hosts, sizes):
    """{host: {"paths", "mb", "python_packages"}}: the part of the closure
    (sizes, from closure()) that host's STORE_LIST held."""
    if not sizes:
        return {}
    cmd = "printf '%%s\\n' %s >%s; grep -xFf %s %s | sed 's/^/PLHAVE /'; echo PLDONE" % (
        " ".join(shlex.quote(n) for n in sorted(sizes)), CLOSURE_LIST, CLOSURE_LIST, STORE_LIST)
    rc, out = ctx.remote.sh(":".join(h.id for h in hosts), cmd, timeout=120)
    have, done = {}, set()
    for host, rest in _lines(hosts, out):
        parts = rest.split()
        if parts[:1] == ["PLHAVE"] and len(parts) == 2 and parts[1] in sizes:
            have.setdefault(host, []).append(parts[1])
        elif parts == ["PLDONE"]:
            done.add(host)
    res = {}
    for host in sorted(done):
        names = have.get(host, [])
        res[host] = {"paths": len(names), "mb": round(sum(sizes[n] for n in names) / 1e6, 1),
                     "python_packages": sorted(n.split("-", 1)[1] for n in names
                                               if n.split("-", 1)[-1].startswith("python3."))}
    return res


def _lines(hosts, out):
    for line in out.splitlines():
        if len(hosts) == 1:
            yield hosts[0].id, line
        else:
            host, _, rest = line.partition(": ")
            yield host, rest
