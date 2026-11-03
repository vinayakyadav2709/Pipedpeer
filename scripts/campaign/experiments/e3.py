"""E3: setup effort, the operator on 2 PCs under a fresh user account, each
tool from its official quick start until W1 runs across both. A guided
timer: run.sh e3 prints the steps, takes start/stop from Enter, then asks
for commands typed and script lines changed. Record the terminal with
asciinema (`asciinema rec results/lab/e3-<system>.cast`) and give its path
in the notes."""
import sys

from campaign import guided

LOCAL_ONLY = True
STEPS = {
    "pipedpeer": [
        "As the fresh user on PC A, follow pipedpeer-demo/KIT.md: install, `pipedpeer invite`.",
        "On PC B, install and `pipedpeer connect <code>`.",
        "Copy docs/benchmarks/workloads/pool_map.py to PC A and `pipedpeer run pool_map.py`.",
        "Stop when it prints POOLMAP-OK and the receipt shows items on PC B.",
    ],
    "ray": [
        "Follow https://docs.ray.io/en/latest/cluster/vms/user-guides/launching-clusters/on-premises.html"
        " (manual cluster): `pip install -U \"ray[default]\"` on both PCs.",
        "PC A: `ray start --head`; PC B: `ray start --address=<A>:6379`.",
        "Port pool_map.py to Ray yourself (ray.util.multiprocessing.Pool) and run it on PC A.",
        "Stop when it prints POOLMAP-OK and `ray status` shows both nodes used.",
    ],
    "dask": [
        "Follow https://distributed.dask.org/en/stable/quickstart.html: `pip install \"dask[distributed]\"`"
        " on both PCs.",
        "PC A: `dask scheduler`; both PCs: `dask worker tcp://<A>:8786`.",
        "Port pool_map.py to Dask yourself (Client.map/gather) and run it on PC A.",
        "Stop when it prints POOLMAP-OK with tasks on both workers (dashboard or logs).",
    ],
}


def main(args, out_dir, out=sys.stdout, inp=sys.stdin):
    rec = guided.recorder(out_dir, "e3", args.rehearsal)
    systems = list(STEPS)
    for i, s in enumerate(systems, 1):
        guided.attempt(rec, s, STEPS[s], "operator", i, out=out, inp=inp)
    return 0
