#!/usr/bin/env bash
# Find the lab PCs that ran lab.sh (worker setup) and build the Ansible inventory.
#   scripts/campaign/scan.sh <subnet> [--head <ip>] [--user <labuser>] [--no-nmap]
# Finds hosts with port 22 open (nmap -p22 --open, or a parallel TCP sweep in
# python3 when nmap is missing), logs in to each with the campaign key, reads
# machine-id, hostname, nproc and RAM, and writes:
#   results/lab/inventory.ini  [head] pc00, [workers] pc01..pcNN, [all:vars]
#   results/lab/hosts.csv      pc_id,ip,hostname,machine_id,cores,ram_gib
#   results/lab/pc-ids.csv     every machine ever given a pc id (machine id,
#                              hostname, last IP); only grows
# A machine keeps its pc id across rescans (also after missing one); new
# machines get the next free ids in IP order. --head pins that host to pc00.
# Env: LAB_KEY (default ~/.ssh/pipedpeer-lab_ed25519), LAB_RESULTS (default
# <repo>/results/lab), LAB_SSH_OPTS (extra ssh options, word-split).
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$here/../.." && pwd)
key=${LAB_KEY:-$HOME/.ssh/pipedpeer-lab_ed25519}
results=${LAB_RESULTS:-$repo_root/results/lab}

die() { echo "scan.sh: $*" >&2; exit 1; }
usage() { die "usage: scan.sh <subnet> [--head <ip>] [--user <labuser>] [--no-nmap]"; }

subnet="" head="" user="" no_nmap=0
while (($#)); do
  case "$1" in
    --head) head=${2:-}; [[ -n $head ]] || usage; shift 2 ;;
    --user) user=${2:-}; [[ -n $user ]] || usage; shift 2 ;;
    --no-nmap) no_nmap=1; shift ;;
    -*) usage ;;
    *) [[ -z $subnet ]] || usage; subnet=$1; shift ;;
  esac
done
[[ -n $subnet ]] || usage
command -v python3 >/dev/null || die "python3 not found"
python3 -c 'import ipaddress,sys; ipaddress.ip_network(sys.argv[1], strict=False)' "$subnet" 2>/dev/null || die "not a subnet: $subnet"
[[ -f $key ]] || die "no campaign key at $key; run scripts/campaign/serve.sh first"
mkdir -p "$results"
inv=$results/inventory.ini
if [[ -z $user && -f $inv ]]; then user=$(sed -n 's/^ansible_user=//p' "$inv" | head -n1); fi
user=${user:-$USER}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# 1. Hosts with 22/tcp open.
if ((no_nmap == 0)) && command -v nmap >/dev/null; then
  echo "scan: nmap -p22 --open $subnet"
  nmap -n -p22 --open -oG - "$subnet" | awk '/Ports: 22\/open/{print $2}' >"$work/open"
else
  echo "scan: nmap $( ((no_nmap)) && echo skipped || echo not found); python3 TCP-22 sweep of $subnet"
  python3 - "$subnet" >"$work/open" <<'EOF'
import asyncio, ipaddress, sys
net = ipaddress.ip_network(sys.argv[1], strict=False)
hosts = [str(h) for h in (net.hosts() if net.num_addresses > 2 else net)]
sem = asyncio.Semaphore(512)
async def probe(h):
    async with sem:
        try:
            _, w = await asyncio.wait_for(asyncio.open_connection(h, 22), 1.5)
            w.close()
            return h
        except Exception:
            return None
async def main():
    for h in await asyncio.gather(*(probe(h) for h in hosts)):
        if h:
            print(h)
asyncio.run(main())
EOF
fi
sort -t. -k1,1n -k2,2n -k3,3n -k4,4n -u "$work/open" -o "$work/open"
echo "scan: $(wc -l <"$work/open") host(s) with 22/tcp open"

# 2. Key login and facts, in parallel. One line per host: ip ok|fail fields...
# Runs in the remote login shell, so it stays one literal string.
# shellcheck disable=SC2016,SC2089,SC2090
remote='printf "%s %s %s %s\n" "$(cat /etc/machine-id)" "$(uname -n)" "$(nproc)" "$(awk "/MemTotal/{printf \"%.0f\",\$2/1048576}" /proc/meminfo)"'
probe() {
  local ip=$1 out
  # shellcheck disable=SC2086
  if out=$(ssh -n -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 \
      ${LAB_SSH_OPTS:-} -i "$key" "$user@$ip" "$remote" 2>/dev/null) && [[ $(wc -w <<<"$out") -eq 4 ]]; then
    echo "$ip ok $out"
  else
    echo "$ip fail"
  fi
}
export -f probe
# shellcheck disable=SC2090
export key user remote
# shellcheck disable=SC2016
xargs -r -P 32 -I{} bash -c 'probe "$1"' _ {} <"$work/open" >"$work/probe"

# 3. Merge with the ids already given out, write inventory and hosts.csv.
python3 - "$work/probe" "$results" "$head" "$user" "$key" "$subnet" <<'EOF'
import csv, datetime, ipaddress, os, re, sys
probe, results, head, user, key, subnet = sys.argv[1:]
ids_path = os.path.join(results, "pc-ids.csv")
inv_path = os.path.join(results, "inventory.ini")
ok, failed = [], []
for line in open(probe):
    f = line.split()
    if len(f) == 6 and f[1] == "ok":
        ok.append(dict(ip=f[0], machine_id=f[2], hostname=f[3], cores=f[4], ram_gib=f[5]))
    elif f:
        failed.append(f[0])
ok.sort(key=lambda h: ipaddress.ip_address(h["ip"]))

# Cloned disk images can share /etc/machine-id, so a machine is matched to its
# pc id by machine-id plus hostname, then plus IP, then by machine-id alone when
# that is unambiguous; each known machine is claimed at most once per scan.
count = {}
for h in ok:
    count[h["machine_id"]] = count.get(h["machine_id"], 0) + 1
for h in ok:
    if count[h["machine_id"]] > 1:
        print(f"scan: WARNING: {h['ip']} ({h['hostname']}) shares machine-id {h['machine_id']} with another host", file=sys.stderr)

known = []  # dicts: pc, machine_id, hostname, ip
if os.path.exists(ids_path):
    for r in csv.DictReader(open(ids_path)):
        known.append(dict(pc=int(r["pc_id"][2:]), machine_id=r["machine_id"], hostname=r.get("hostname", ""), ip=r.get("ip", "")))
elif os.path.exists(inv_path):
    for line in open(inv_path):
        m = re.match(r"^pc(\d+)\s+ansible_host=(\S+)\s+machine_id=(\S+)", line)
        if m:
            known.append(dict(pc=int(m.group(1)), machine_id=m.group(3), hostname="", ip=m.group(2)))

claimed = set()
def claim(h, rows):
    rows = [r for r in rows if r["pc"] not in claimed]
    if len(rows) == 1:
        h["pc"] = rows[0]["pc"]
        claimed.add(h["pc"])
rules = [
    lambda h, r: r["machine_id"] == h["machine_id"] and r["hostname"] == h["hostname"] and r["ip"] == h["ip"],
    lambda h, r: r["machine_id"] == h["machine_id"] and r["hostname"] == h["hostname"],
    lambda h, r: r["machine_id"] == h["machine_id"] and r["ip"] == h["ip"],
]
for rule in rules:
    for h in ok:
        if "pc" not in h:
            claim(h, [r for r in known if rule(h, r)])
for h in ok:
    same = [r for r in known if r["machine_id"] == h["machine_id"]]
    if "pc" not in h and count[h["machine_id"]] == 1 and len(same) == 1:
        claim(h, same)
next_pc = lambda: max([r["pc"] for r in known] + [h.get("pc", -1) for h in ok] + [-1]) + 1
for h in ok:
    if "pc" not in h:
        h["pc"] = next_pc()
        known.append(dict(pc=h["pc"], machine_id=h["machine_id"], hostname=h["hostname"], ip=h["ip"]))

if head:
    hh = [h for h in ok if h["ip"] == head]
    if not hh:
        sys.exit(f"scan.sh: --head {head}: no key login there")
    if hh[0]["pc"] != 0:
        # Whoever held pc00 before gets a new id at the end.
        moved = next_pc()
        for r in known:
            if r["pc"] == 0:
                r["pc"] = moved
        for h in ok:
            if h["pc"] == 0:
                h["pc"] = moved
        for r in known:
            if r["pc"] == hh[0]["pc"]:
                r["pc"] = 0
        hh[0]["pc"] = 0

# Remember every machine seen, with its latest hostname and IP.
for h in ok:
    for r in known:
        if r["pc"] == h["pc"]:
            r.update(machine_id=h["machine_id"], hostname=h["hostname"], ip=h["ip"])
for h in ok:
    h["pc_id"] = "pc%02d" % h["pc"]

with open(ids_path + ".new", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["pc_id", "machine_id", "hostname", "ip"])
    for r in sorted(known, key=lambda r: r["pc"]):
        w.writerow(["pc%02d" % r["pc"], r["machine_id"], r["hostname"], r["ip"]])
os.replace(ids_path + ".new", ids_path)

ok.sort(key=lambda h: h["pc"])
heads = [h for h in ok if h["pc"] == 0]
workers = [h for h in ok if h["pc"] != 0]
if not heads and ok:
    print("scan: WARNING: pc00 (the head) did not answer; [head] is empty", file=sys.stderr)
def host(h):
    return f"{h['pc_id']} ansible_host={h['ip']} machine_id={h['machine_id']}\n"
with open(inv_path + ".new", "w") as f:
    f.write(f"# scripts/campaign/scan.sh {subnet} at {datetime.datetime.now().astimezone().isoformat(timespec='seconds')}\n")
    f.write("[head]\n" + "".join(host(h) for h in heads) + "\n")
    f.write("[workers]\n" + "".join(host(h) for h in workers) + "\n")
    f.write(f"[all:vars]\nansible_user={user}\nansible_ssh_private_key_file={os.path.abspath(key)}\nansible_python_interpreter=auto_silent\n")
os.replace(inv_path + ".new", inv_path)

csv_path = os.path.join(results, "hosts.csv")
with open(csv_path + ".new", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["pc_id", "ip", "hostname", "machine_id", "cores", "ram_gib"])
    for h in ok:
        w.writerow([h["pc_id"], h["ip"], h["hostname"], h["machine_id"], h["cores"], h["ram_gib"]])
os.replace(csv_path + ".new", csv_path)

for h in ok:
    print(f"scan: {h['pc_id']} {h['ip']} {h['hostname']} cores={h['cores']} ram_gib={h['ram_gib']}")
for ip in failed:
    print(f"scan: {ip}: 22/tcp open but key login as {user} failed (setup not run there?)")
print(f"scan: {len(ok)} in {inv_path} ({len(heads)} head, {len(workers)} workers), {len(failed)} without login")
EOF
