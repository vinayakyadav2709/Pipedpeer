#!/usr/bin/env bash
# DiLoCo quality: final validation loss of `--ddp 2 --ddp-sync fast` against
# one device, on the three test models, three seeds each.
#
# Models (CPU-training stand-ins, same architecture, data, optimizer and
# epochs as the demos; a held-out validation set is added where the demo had
# none):
#   gpu    pipedpeer-demo/gpu.py's MLP: 200k samples, 3x512, batch 4096, SGD 0.05
#   ddp04  pipedpeer-demo/04_torch_ddp.py's CPU sizing: 60k, 3x512, batch 2048,
#          SGD 0.1, 6 epochs (its default 3 is 90 steps, mostly DiLoCo warm-up)
#   tfm    a small transformer encoder (2 layers, d 64), AdamW 1e-3, batch 256,
#          so the Adam-moment sync is exercised too
#
# Arms, every one with the same batch, learning rate and epochs:
#   single  one container, no --ddp: the "one GPU" reference the 1% is judged on
#   exact   --ddp 2 (per-step sync): control, separates sharding from DiLoCo
#   fast    --ddp 2 --ddp-sync fast, once per network profile
#   gpu     the single run on this machine's real GPU (BL_GPU_PYTHON): shows
#           how far CPU and GPU numerics alone move the loss
#
# Every lab run passes --gpu off: the containers see the host's NVIDIA driver in
# /sys and would otherwise fetch the 6.6 GB CUDA torch wheel set.
#
# Containers are equal (2 CPUs, TRAIN_MEM) on scripts/lab-emu.sh's shaped
# bridge: every wall time and byte count is EMULATED. Loss does not depend on
# the emulation.
#
#   scripts/bench-diloco.sh run       # single, exact, fast x PROFILES
#   scripts/bench-diloco.sh gpu       # real-GPU single baseline (BL_GPU_PYTHON)
#   scripts/bench-diloco.sh table     # the per-model verdict as markdown
#
# Knobs: MODELS (gpu ddp04 tfm), SEEDS (1 2 3), PROFILES (wifi 5g), ARMS
# (single exact fast), TRAIN_MEM (2g), DILOCO_BIN + DILOCO_REV (default: built from a
# git archive of HEAD), BL_LAB_NAME (diloco), TOLERANCE (0.01), UNLOCKED (0; 1 =
# skip the shared bench lock: loss does not depend on CPU contention, but the
# wall times are then "unlocked, not for the paper").
set -euo pipefail
BL_LAB_NAME="${BL_LAB_NAME:-diloco}"
if [[ "${UNLOCKED:-0}" == 1 ]]; then
	BL_LOCK="${XDG_CACHE_HOME:-$HOME/.cache}/pipedpeer-$BL_LAB_NAME.lock"
	echo "[bench] UNLOCKED=1: not holding the shared bench lock; wall times are unlocked, not for the paper" >&2
fi
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/bench-lib.sh"

read -r -a models <<< "${MODELS:-gpu ddp04 tfm}"
read -r -a seeds <<< "${SEEDS:-1 2 3}"
read -r -a profiles <<< "${PROFILES:-wifi 5g}"
arms=" ${ARMS:-single exact fast} "
mem="${TRAIN_MEM:-2g}"
suite_dir="$BL_OUT/diloco"
extra="$suite_dir/extra.csv"

workload() { # workload <dir>
	mkdir -p "$1"
	: > "$1/.pipedpeerignore"
	cat > "$1/diloco.py" <<'PY'
import os, time
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

MODEL = os.environ.get("BENCH_MODEL", "gpu")
SEED = int(os.environ.get("BENCH_SEED", "1"))
dev = os.environ.get("BENCH_DEVICE", "cpu")
torch.set_num_threads(int(os.environ.get("BENCH_THREADS", "2")))
g = torch.Generator().manual_seed(11)  # the data is the same for every seed


def mlp(d, w):
    return nn.Sequential(nn.Linear(d, w), nn.ReLU(), nn.Linear(w, w), nn.ReLU(),
                         nn.Linear(w, w), nn.ReLU(), nn.Linear(w, 1))


class Tfm(nn.Module):
    def __init__(self, vocab=16, d=64, seq=32, classes=3):
        super().__init__()
        self.emb = nn.Embedding(vocab, d)
        self.pos = nn.Parameter(torch.randn(seq, d) * 0.02)
        layer = nn.TransformerEncoderLayer(d, 4, 128, dropout=0.0, batch_first=True)
        self.enc = nn.TransformerEncoder(layer, 2, enable_nested_tensor=False)
        self.head = nn.Linear(d, classes)

    def forward(self, x):
        return self.head(self.enc(self.emb(x) + self.pos).mean(1))


if MODEL == "gpu":  # pipedpeer-demo/gpu.py
    x = torch.randn(220_000, 64, generator=g)
    y = (x[:, 0] * 2 + x[:, 1] - x[:, 2] * x[:, 3] > 0.5).float().unsqueeze(1)
    ntr, batch, epochs = 200_000, 4096, 3
    torch.manual_seed(SEED)
    model, loss_fn = mlp(64, 512), nn.MSELoss()
    opt = torch.optim.SGD(model.parameters(), lr=0.05, momentum=0.9)
elif MODEL == "ddp04":  # pipedpeer-demo/04_torch_ddp.py, CPU sizing
    x = torch.randn(66_000, 64, generator=g)
    y = (x[:, 0] * 2 + x[:, 1] - x[:, 2] > 0.5).float().unsqueeze(1)
    ntr, batch, epochs = 60_000, 2048, 6
    torch.manual_seed(SEED)
    model, loss_fn = mlp(64, 512), nn.MSELoss()
    opt = torch.optim.SGD(model.parameters(), lr=0.1, momentum=0.9)
elif MODEL == "tfm":  # majority token among 1..3 (ties: the lowest), 10% labels
    x = torch.randint(0, 16, (44_000, 32), generator=g)  # noisy so the loss stays off 0
    y = torch.stack([(x == t).sum(1) for t in (1, 2, 3)], 1).argmax(1)
    noisy = torch.rand(len(y), generator=g) < 0.1
    y[noisy] = torch.randint(0, 3, (int(noisy.sum()),), generator=g)
    ntr, batch, epochs = 40_000, 256, 2
    torch.manual_seed(SEED)
    model, loss_fn = Tfm(), nn.CrossEntropyLoss()
    opt = torch.optim.AdamW(model.parameters(), lr=1e-3)
else:
    raise SystemExit("unknown BENCH_MODEL %r" % MODEL)
epochs = int(os.environ.get("BENCH_EPOCHS", epochs))
model = model.to(dev)
loader = DataLoader(TensorDataset(x[:ntr], y[:ntr]), batch_size=batch, shuffle=True)
t0, steps = time.monotonic(), 0
for _ in range(epochs):
    for xb, yb in loader:
        xb, yb = xb.to(dev), yb.to(dev)
        opt.zero_grad()
        loss_fn(model(xb), yb).backward()
        opt.step()
        steps += 1
if dev != "cpu":
    torch.cuda.synchronize()
el = time.monotonic() - t0
with torch.no_grad():
    val = loss_fn(model(x[ntr:].to(dev)), y[ntr:].to(dev)).item()
rank = os.environ.get("PIPEDPEER_RANK", "0")
print("BENCH_RANKLOSS %s %.6f" % (rank, val))
if rank == "0":
    print("BENCH_LOSS %.6f" % val)
print("BENCH_WORLD", os.environ.get("PIPEDPEER_WORLD_SIZE", "1"))
print("BENCH_STEPS_DONE", steps)
print("BENCH_STEP_S %.5f" % (el / max(steps, 1)))
print("BENCH_ELAPSED %.3f" % el)
PY
}

build() { # sets BL_BIN, BL_REV, BL_API_PORT from DILOCO_BIN or a git archive of HEAD
	BL_API_PORT=4242
	BL_REV="$(git -C "$BL_ROOT" rev-parse --short HEAD)"
	if [[ -n "${DILOCO_BIN:-}" ]]; then
		BL_BIN="$DILOCO_BIN"
		# HEAD may have moved since the binary was built: say which commit it is.
		BL_REV="${DILOCO_REV:?DILOCO_BIN needs DILOCO_REV, the commit it was built from}"
		return
	fi
	BL_BIN="$BL_BINDIR/pipedpeer-$BL_REV"
	[[ -x "$BL_BIN" ]] && return
	local src="$BL_BINDIR/../src-$BL_REV"
	rm -rf "$src" && mkdir -p "$src" "$BL_BINDIR"
	# git archive: other people's working-tree edits never reach the binary.
	git -C "$BL_ROOT" archive HEAD src | tar -x -C "$src"
	bl_log "building $BL_REV from a git archive of HEAD"
	(cd "$src/src" && CGO_ENABLED=0 systemd-run --user --scope -q --slice=pipedpeer-agents.slice \
		go build -o "$BL_BIN" .) >&2 || bl_die "build failed"
}

# run_one <model> <arm> <seed> [flags...] - one bl_run plus the columns its CSV
# lacks (every rank's loss, bytes synced) appended to extra.csv. Ring runs pass
# -v: the shim prints its sync totals and DiLoCo schedule only then.
run_one() {
	local m="$1" arm="$2" s="$3"
	shift 3
	bl_run "$m" "$arm" after "$s" diloco.py --gpu off "$@" -e BENCH_MODEL="$m" -e BENCH_SEED="$s"
	python3 - "$BL_OUT/diloco/logs/${m}_${arm}_after_${BL_PROFILE}_n${BL_NODES_UP}_r${s}.log" \
		"$m" "$arm" "$BL_PROFILE" "$s" "$BL_REV" >> "$extra" <<'PY'
import re, sys, time
log, m, arm, prof, seed, rev = sys.argv[1:]
t = open(log, errors="replace").read()
ranks = ";".join("%s=%s" % r for r in re.findall(r"BENCH_RANKLOSS (\d+) ([0-9.]+)", t))
sent = sum(float(v) for v in re.findall(r"([0-9.]+) MiB sent", t))
recv = sum(float(v) for v in re.findall(r"([0-9.]+) MiB received", t))
syncs = sum(int(v) for v in re.findall(r"ddp: (\d+) sync\(s\)", t))
print(",".join([m, arm, prof, seed, rev, ranks, "%.1f" % sent, "%.1f" % recv, str(syncs),
                time.strftime("%Y-%m-%dT%H:%M:%S")]))
PY
}

case "${1:-}" in
run) ;;
gpu)
	py="${BL_GPU_PYTHON:-python3}"
	"$py" -c 'import torch, sys; sys.exit(0 if torch.cuda.is_available() else 1)' ||
		bl_die "$py has no CUDA torch; set BL_GPU_PYTHON to one that does"
	mkdir -p "$suite_dir/gpu" "$(dirname "$BL_LOCK")"
	exec 9>"$BL_LOCK"
	flock -n 9 || { bl_log "another timed run holds $BL_LOCK; waiting for it"; flock 9; }
	workload "$suite_dir/gpu"
	out="$suite_dir/gpu.csv"
	[[ -f "$out" ]] || echo "model,seed,loss,step_s,when" > "$out"
	for m in "${models[@]}"; do
		for s in "${seeds[@]}"; do
			log="$suite_dir/gpu/${m}_s$s.log"
			BENCH_MODEL="$m" BENCH_SEED="$s" BENCH_DEVICE=cuda BENCH_THREADS=4 "$py" "$suite_dir/gpu/diloco.py" > "$log" 2>&1 ||
				{ bl_log "gpu baseline $m seed $s failed: $log"; continue; }
			echo "$m,$s,$(grep -oP 'BENCH_LOSS \K[0-9.]+' "$log"),$(grep -oP 'BENCH_STEP_S \K[0-9.]+' "$log"),$(date +%FT%T)" >> "$out"
			bl_log "gpu $m seed $s: $(grep -o 'BENCH_LOSS .*' "$log") (real GPU, not emulated)"
		done
	done
	exit 0
	;;
table)
	python3 - "$suite_dir/results.csv" "$extra" "$suite_dir/gpu.csv" "${TOLERANCE:-0.01}" <<'PY'
import csv, os, statistics as st, sys
res, extra, gpu, tol = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
last = {}  # (model, arm, profile, seed) -> newest ok row
for r in csv.DictReader(open(res)):
    if r["status"] == "ok" and r["loss_val"]:
        last[(r["case"], r["variant"], r["profile"], int(r["rep"]))] = r
ext = {}
if os.path.exists(extra):
    for r in csv.reader(open(extra)):
        ext[(r[0], r[1], r[2], int(r[3]))] = r
g = {}
if os.path.exists(gpu):
    for r in csv.DictReader(open(gpu)):
        if r["loss"]:
            g.setdefault(r["model"], {})[int(r["seed"])] = float(r["loss"])
def f(v, p="%.4f"): return p % v if v is not None else "-"
def med(v): return st.median(v) if v else None
print("| model | arm | profile | seeds | val loss mean | min-max | std | gap vs single (mean) | per-seed gaps | worst rank gap | wall s | MiB sent/rank | syncs | verdict |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
verdict = {}
for m in sorted({k[0] for k in last}):
    single = {k[3]: float(r["loss_val"]) for k, r in last.items() if k[0] == m and k[1] == "single"}
    ref = st.mean(single.values()) if single else None
    if m in g:
        gl = list(g[m].values())
        gap = (st.mean(gl) / ref - 1) if ref else None
        print("| %s | gpu (real, not emulated) | - | %d | %.4f | %.4f-%.4f | %.4f | %s | - | - | - | - | - | reference noise |" % (
            m, len(gl), st.mean(gl), min(gl), max(gl), st.pstdev(gl), f(gap and 100 * gap, "%+.2f%%")))
    for arm, prof in sorted({(k[1], k[2]) for k in last if k[0] == m}, key=lambda a: (a[0] != "single", a)):
        rows = {k[3]: r for k, r in last.items() if k[0] == m and k[1] == arm and k[2] == prof}
        ls = [float(r["loss_val"]) for r in rows.values()]
        gaps = [float(rows[s]["loss_val"]) / single[s] - 1 for s in sorted(rows) if s in single]
        worst = []
        for s in rows:
            e = ext.get((m, arm, prof, s))
            if e and e[5] and s in single:
                worst += [float(x.split("=")[1]) / single[s] - 1 for x in e[5].split(";")]
        sent = [float(ext[(m, arm, prof, s)][6]) / max(1, int(rows[s]["world"] or 1)) for s in rows if (m, arm, prof, s) in ext]
        syncs = [int(ext[(m, arm, prof, s)][8]) / max(1, int(rows[s]["world"] or 1)) for s in rows if (m, arm, prof, s) in ext]
        mgap = (st.mean(ls) / ref - 1) if ref and arm != "single" else None
        v = "-"
        if arm == "fast":
            v = "PASS" if mgap is not None and mgap <= tol and len(ls) >= 3 else "FAIL"
            verdict[m] = verdict.get(m, True) and v == "PASS"
        print("| %s | %s | %s | %d | %.4f | %.4f-%.4f | %.4f | %s | %s | %s | %s | %s | %s | %s |" % (
            m, arm, "-" if arm == "single" else prof, len(ls), st.mean(ls), min(ls), max(ls), st.pstdev(ls),
            f(mgap and 100 * mgap, "%+.2f%%") if arm != "single" else "-",
            " ".join("%+.2f%%" % (100 * x) for x in gaps) if arm != "single" else "-",
            f(max(worst) * 100, "%+.2f%%") if worst and arm != "single" else "-",
            f(med([float(r["wall_s"]) for r in rows.values()]), "%.0f"), f(med(sent), "%.1f"),
            f(med(syncs), "%.0f"), v))
print()
print("Tolerance: fast's mean validation loss over the seeds within %.0f%% (relative) of single's. "
      "Overall: %s" % (100 * tol, "PASS" if verdict and all(verdict.values()) else "FAIL") +
      "".join(" | %s %s" % (m, "pass" if ok else "fail") for m, ok in sorted(verdict.items())))
PY
	exit 0
	;;
*)
	sed -n '2,38p' "$0" | sed 's/^# \{0,1\}//'
	exit 2
	;;
esac

bl_init diloco
build
work="$suite_dir/work"
workload "$work"
[[ -f "$extra" ]] || : > "$extra"

if [[ "$arms" == *" single "* ]]; then
	bl_lab_up 1 "$mem" lan 0 "$BL_BIN"
	bl_put 1 "$work"
	bl_warm diloco.py --gpu off -e BENCH_EPOCHS=0
	for m in "${models[@]}"; do
		for s in "${seeds[@]}"; do run_one "$m" single "$s"; done
	done
fi
if [[ "$arms" == *" exact "* || "$arms" == *" fast "* ]]; then
	bl_lab_up 2 "$mem" "${profiles[0]}" 0 "$BL_BIN"
	bl_put 1 "$work"
	bl_warm diloco.py --gpu off --ddp 2 -e BENCH_EPOCHS=0
	if [[ "$arms" == *" exact "* ]]; then
		for m in "${models[@]}"; do
			for s in "${seeds[@]}"; do run_one "$m" exact "$s" -v --ddp 2; done
		done
	fi
	if [[ "$arms" == *" fast "* ]]; then
		for p in "${profiles[@]}"; do
			bl_lab_shape "$p" 0
			for m in "${models[@]}"; do
				for s in "${seeds[@]}"; do run_one "$m" fast "$s" -v --ddp 2 --ddp-sync fast; done
			done
		done
	fi
fi
bl_lab_down
"$0" table
