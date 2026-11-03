"""Reads the Ansible INI inventory lab/setup writes: [head] pc00, [workers]
pc01..pcNN, host vars ansible_host and machine_id (and lab_ip, which the
rehearsal adds because its ansible_host is a container name)."""
import re
import shlex
from dataclasses import dataclass, field


@dataclass
class Host:
    id: str
    vars: dict = field(default_factory=dict)

    @property
    def ip(self) -> str:
        """The address other PCs reach this one at."""
        return self.vars.get("lab_ip") or self.vars.get("ansible_host") or self.id


@dataclass
class Inventory:
    head: Host
    workers: list

    def host(self, pc_id: str) -> Host:
        for h in [self.head, *self.workers]:
            if h.id == pc_id:
                return h
        raise KeyError(pc_id)

    @property
    def all(self) -> list:
        return [self.head, *self.workers]


def _key(pc_id: str):
    m = re.search(r"(\d+)$", pc_id)
    return (int(m.group(1)) if m else 1 << 30, pc_id)


def parse(text: str) -> Inventory:
    groups: dict = {}
    group = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("["):
            group = line.strip("[]")
            groups.setdefault(group, [])
            continue
        if group is None or ":" in group:
            continue  # [all:vars] and friends
        parts = shlex.split(line)
        vars_ = dict(p.split("=", 1) for p in parts[1:] if "=" in p)
        groups[group].append(Host(parts[0], vars_))
    heads = groups.get("head", [])
    if len(heads) != 1:
        raise ValueError("inventory needs exactly one host in [head], got %d" % len(heads))
    workers = sorted(groups.get("workers", []), key=lambda h: _key(h.id))
    return Inventory(heads[0], workers)


def load(path: str) -> Inventory:
    with open(path) as f:
        return parse(f.read())
