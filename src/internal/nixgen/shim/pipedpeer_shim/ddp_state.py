"""Transparent DDP's run state: one object holding what every part of a
rank's training run shares (the ring, the counters, the receipt)."""
import atexit
import os
import weakref

from pipedpeer_shim import record
from pipedpeer_shim.core import _log, _warn
from pipedpeer_shim.ddp_diloco import (
    _DILOCO_RANKS, _diloco_every, _diloco_use_every, _sync_mode,
)
from pipedpeer_shim.ddp_sampler import _parse_weights, _recent_mean_ms
from pipedpeer_shim.ddp_wire import _SyncThread


class _RingState:
    """Everything _install_ddp's parts share, built once from the
    environment. The parts are mixins over it, one module each: ddp_sync
    (the wire), ddp_fast (DiLoCo and overlap), ddp_ring (ring changes and
    joining), ddp_loader (sharding), ddp (interception)."""

    def __init__(self, th, np):
        self.th = th
        self.native_ddp = []
        self.np = np
        # Gradients ship as float16 unless asked otherwise. Averaging is still
        # done in float64; only the wire is narrowed.
        self.fp16_grads = os.environ.get("PIPEDPEER_DDP_FP32") != "1"
        self.world = int(os.environ.get("PIPEDPEER_WORLD_SIZE", "1"))
        self.rank = int(os.environ.get("PIPEDPEER_RANK", "0"))
        # Each rank's share of a step, measured by placement. Equal shares are the
        # same thing as no shares, and are treated as such.
        self.weights = _parse_weights(os.environ.get("PIPEDPEER_DDP_WEIGHTS", ""), self.world)
        # --ddp N (C2): the user sized the ring, so the lead never shrinks it.
        self.fixed = os.environ.get("PIPEDPEER_DDP_FIXED") == "1"
        # Each rank's machine name, by rank, for messages.
        self.hosts = os.environ.get("PIPEDPEER_DDP_HOSTS", "").split(",")
        # The number of samples behind the gradient being sent, read from the
        # batch the model was last given. It decides how the daemon weighs this
        # rank's contribution, and it has to be the real figure rather than the
        # configured one: a final short batch is smaller than the rest, and a
        # rank whose loader ran dry contributes nothing at all.
        self.batch_n = 0
        # Whether the measured shares were actually used. The run announces them
        # before the script starts, and a script that indexes its own tensors
        # ("X[rank::world]") never reaches the sampler that would apply them - so
        # without this the run claims a split it did not perform.
        self.weights_applied = False
        self.warned_weights_unused = False
        # A change to the ring (new shares, a rank gone, a rank joining) waiting
        # for an epoch boundary. Resharding mid-epoch would have ranks re-slice
        # data they are part-way through, so samples would be trained on twice or
        # skipped; the boundary is where the sampler already re-slices. A dict:
        # weights, world, and join/from while a rank is on its way in.
        self.pending = None
        # Bumped at every ring change; a hashed sampler built under an older one
        # is rebuilt at its loader's next epoch.
        self.ring_gen = 0
        # Every DataLoader epoch this rank has started, across all loaders. A
        # ring change happens at one of these, and a rank joining mid-run skips
        # exactly the ones the ring had already done.
        self.iters = 0
        # [the sampler of the loader being iterated, steps taken this epoch].
        self.epoch = None
        # Set when a loader is sharded by hash: only such a ring can take a rank
        # mid-run, and the daemon is told so with every sync.
        self.sharded_hash = False
        # A rank started to join a running ring (pipedpeer saw a machine come
        # back, or a new one): it skips to the ring's epoch and loads the ring's
        # model and optimizer before its first step.
        self.join = os.environ.get("PIPEDPEER_DDP_JOIN") == "1"
        self.join_at = None
        # The learning rates the ring had when this rank took its model, per
        # optimizer, put back once the skipped epochs are behind it.
        self.join_lr = None
        # True from a skipped epoch's placeholder batch until the joiner's next
        # epoch starts; with it the stdout the script had before.
        self.catchup = False
        self.catchup_out = None
        # Every optimizer the script built, so a model can be handed to a joiner.
        self.opts = []
        # Set when the daemon has told this rank the ring is better off without
        # it.
        self.dropped = False
        # Every sync is one POST to the lead rank's daemon on the same port every
        # other byte of pipedpeer traffic uses: no sockets of our own, no
        # MASTER_PORT, nothing new to firewall (ADR-0010).
        self.sync_url = os.environ.get("PIPEDPEER_DDP_SYNC", "")
        self.group = os.environ.get("PIPEDPEER_DDP_GROUP", "ddp")
        # 1 = average gradients every step (exact DDP semantics). N>1 = local
        # SGD: train locally, average weights every Nth step — the knob for
        # high-latency links where a per-step round trip dominates.
        self.sync_raw = os.environ.get("PIPEDPEER_DDP_SYNC_EVERY", "1").strip().lower()
        self.sync_auto = self.sync_raw == "auto"
        self.sync_every = 1 if self.sync_auto else max(1, int(self.sync_raw or "1"))
        # run --ddp-sync fast (C2, Decisions): DiLoCo with an adaptive interval
        # (_DiLoCo, _diloco_round). Replaces the interval above when set.
        self.fast = os.environ.get("PIPEDPEER_DDP_SYNC_MODE", "").strip().lower() == "fast"
        self.auto_sync = _sync_mode(os.environ.get("PIPEDPEER_DDP_SYNC_MODE"),
                                os.environ.get("PIPEDPEER_DDP_OVERLAP") == "1") == "auto"
        self.diloco = {}            # id(optimizer) -> _DiLoCo
        self.total = None         # the run's optimizer steps, when a OneCycleLR says
        self.drift = None         # the last reduce reply's drift, None if it had none
        self.told_fast = False
        self.pace = None          # the last reply's steps_ms (rank -> ms), for round budgets
        # DILOCO-MANY: the interval for this ring's size. An auto ring takes the
        # lead's instead when it switches (_go_fast); a joiner takes the ring's
        # (_join_ring). ponytail: fixed at the start size; a ring that grows
        # by joins keeps it (no mid-run interval change across ranks).
        self.h_for_world, self.h_validated = _diloco_every(self._live_ranks())
        _diloco_use_every(self.h_for_world)
        if self.fast and not self.h_validated and self.rank == 0:
            _warn("ddp: --ddp-sync fast is validated for up to %d ranks and this ring has %d: "
                  "syncing every %d steps, but it may land more than 1%% off one device — "
                  "--ddp-sync exact keeps the script's arithmetic"
                  % (_DILOCO_RANKS[-1][0], self._live_ranks(), self.h_for_world))
        self.round_ms = None      # a local round's ms per step, sent as its step_ms
        self.told_pace = False
        # The butterfly (U26): every rank's sync URL as this rank reaches it, in
        # rank order, when the CLI built a ring of 3+ machines that all reach
        # each other. Shard k of each gradient goes to rank k's daemon (shard 0
        # to the lead, with the ring's control), so the lead's link carries
        # 2(N-1)/N of a gradient instead of N-1. A shard whose daemon stops
        # answering is averaged on the lead from then on (_PEER_DEAD).
        self.peers = [u for u in os.environ.get("PIPEDPEER_DDP_PEERS", "").split(",") if u]
        self.peer_dead = set()
        self.shard_pool = []
        # Ranks sharing this rank's node (multi-GPU, not the lead's node): they
        # average on their own daemon first, which sends the lead one sync for
        # the machine.
        try:
            self.local = [int(r) for r in os.environ.get("PIPEDPEER_DDP_LOCAL", "").split(",") if r]
        except ValueError:
            _warn("ddp: PIPEDPEER_DDP_LOCAL=%r is not a list of ranks; syncing directly with the lead"
                  % os.environ.get("PIPEDPEER_DDP_LOCAL"))
            self.local = []
        self.local_url = os.environ.get("PIPEDPEER_DAEMON_URL", "").rstrip("/")
        # One-step-delayed overlap (opt-in: each gradient lands a step late):
        # step t's average travels while step t+1 computes.
        self.overlap = os.environ.get("PIPEDPEER_DDP_OVERLAP") == "1"
        self.sync_thread = _SyncThread()
        self.delayed = {}           # id(optimizer) -> (_DelayedGrads, its own step)
        self.told_overlap = False
        # Seconds this rank's training thread spent blocked on syncs: what a
        # step's compute time is net of (an overlapped sync costs only its wait).
        self.exposed = 0.0
        # A receipt for training, for the same reason pool work has one: a run
        # that distributed nothing and a run that distributed everything look
        # identical from the outside, and so do a fast ring and one that spent
        # 95% of its time waiting.
        # barrier_sec is the opening round's wait for every rank to start: not
        # sync, and counted as sync it advised averaging every 32 steps (V4 80).
        self.ddp_stats = {"syncs": 0, "sync_sec": 0.0, "sent_bytes": 0, "recv_bytes": 0,
                      "barrier_sec": 0.0}
        # The start barrier: the opening weight broadcast waits this long (seconds)
        # for every rank, then the ring starts without the ranks still missing.
        try:
            self.start_wait = max(1.0, float(os.environ.get("PIPEDPEER_DDP_START_WAIT", "") or 300))
        except ValueError:
            _log("ddp: PIPEDPEER_DDP_START_WAIT=%r is not a number of seconds; using 300"
                 % os.environ.get("PIPEDPEER_DDP_START_WAIT"))
            self.start_wait = 300.0

        # Averaging less often than every step trades accuracy for speed, so it
        # is off unless asked for: PIPEDPEER_DDP_SYNC_EVERY=auto.
        #
        # The temptation to make it the default is strong, and measurement is why
        # it is resisted. On two machines over a home connection an
        # 800k-parameter model costs ~0.5s a step to sync against ~50ms to
        # compute, so 91% of the run is moving gradients and the job takes 54s
        # against 2.9s on one machine. Auto-tuning to average every ~20 steps cuts
        # that to 5.4s - and moved the final loss from 0.0603 to 0.0756, because
        # an 87-step run averaged 7 times is barely distributed training at all.
        #
        # A 10x speed-up that quietly makes the model worse is exactly the kind of
        # result this project exists to stop shipping. So the default stays exact,
        # the diagnosis is printed either way, and the user decides.
        self.sync_budget = 0.25
        self.sync_every_max = 32
        self.sync_tuned = None if self.sync_auto else self.sync_every
        self.warned_sync_bound = False
        # Said once. The condition holds for every step of the run, and a warning
        # repeated 88 times is a warning nobody reads.
        self.warned_same_work = False
        # Said when it changes rather than every step: a ring that has lost a rank
        # stays lost, and one line per step is a line nobody reads.
        self.last_ranks = 0

        # Gradients as signed bytes rather than half floats: half the bytes again
        # on a link where bytes are the whole cost. What quantisation drops is
        # carried forward in _ERR and added to the next step's gradient, so the
        # error is delayed rather than discarded - which is what keeps this from
        # being a slow bias towards zero; the lead does the same for its reply.
        # On by default (R1 #1: 8-bit with error feedback is a mild compressor
        # that keeps per-step semantics); PIPEDPEER_DDP_INT8=0 sends fp16.
        self.int8_grads = os.environ.get("PIPEDPEER_DDP_INT8", "") != "0"
        self.err = {}
        # Compute time is measured as the interval between consecutive optimizer
        # steps minus whatever was spent syncing in it - not as the duration of
        # step() itself, which is a rounding error next to the forward and
        # backward passes it follows and would make N wildly too large.
        self.step_sec = [0.0, 0]  # total compute seconds, count
        # The last few steps, for the daemon's refit. Separate from the cumulative
        # figure above, which is right for tuning how often to sync - a stable
        # long-run average - and wrong for noticing that this machine slowed down
        # a moment ago. A cumulative mean over a whole run moves toward a new
        # value at (N-k)/N, so a rank that was throttled at step 200 of 400 still
        # reports half its old speed, and one that RECOVERS waits just as long to
        # be given work back. The pool's rate model has used a sliding window
        # since it was written, for the same reason.
        self.step_recent = []
        self.step_window = 20
        self.step_mark = [None, 0.0]  # monotonic at last step, sync_sec at last step

        atexit.register(self._ddp_report)
        self.seq = 0
        self.stepn = {}
        self.warned_no_sync = False
        # When the sync now in flight started (monotonic), 0 when none is: the
        # lead watch probes only while a rank is waiting on the lead.
        self.inflight = 0.0
        self.watching = False
        self.lead_probe_sec = 8

        self.stepped = weakref.WeakSet()
        # Modules holding buffers (BatchNorm running stats), in first-forward
        # order: the same order on every rank running the same script, which is
        # how a joiner matches the ring's buffers to its own.
        self.buf_seen = weakref.WeakSet()
        self.buf_mods = []
        self.join_bufs = None
        self.orig_fwd = self.th.nn.Module.forward
        self.fwd_seen = weakref.WeakSet()
        self.orig_iter = self.th.utils.data.DataLoader.__iter__
        self.epochs = weakref.WeakKeyDictionary()
        # Keyed by id() rather than the loader itself: the decision is cheap to
        # recompute but must not be repeated per epoch, and a DataLoader is not
        # reliably hashable once a user subclasses it.
        self.sharded = {}

        # The modules a step's forward pass starts from (the first module the
        # pre-hook sees each step), for naming parameters in the manifest.
        self.roots = weakref.WeakSet()

        # Patch at Optimizer.__init__ instead of Optimizer.step: the concrete
        # optimizers (SGD, Adam, ...) override step on their own classes, so a
        # class-level step patch would never fire. Wrapping the instance's own
        # step catches every optimizer.
        self.orig_opt_init = self.th.optim.Optimizer.__init__

        self.orig_backward = self.th.Tensor.backward


    def _live_ranks(self, weights=None, world=None):
        """Ranks still training. _WORLD is the rank-id space: a dead rank
        keeps its index with a zero share, so after one dies and one rejoins
        _WORLD is 3 while 2 ranks train. Messages count ranks with this."""
        w = self.weights if weights is None else weights
        n = self.world if world is None else world
        if not w:
            return n
        return sum(1 for x in w if x > 0) or n

    def _mean_sync_ms(self):
        """What one sync has cost this rank on average, in milliseconds.

        The daemon needs it to decide whether a rank pays for itself: every
        rank in the ring costs a gradient exchange per step, and a rank whose
        compute contribution is smaller than the sync it adds is making the
        run slower. Placement cannot know this - it runs before a single byte
        of the model has moved.
        """
        if self.ddp_stats["syncs"] <= 0:
            return 0.0
        return 1000.0 * self.ddp_stats["sync_sec"] / self.ddp_stats["syncs"]

    def _mean_step_ms(self):
        """What a step is costing this rank now, in milliseconds.

        Reported so the lead daemon can compare ranks: same model, same batch,
        whatever hardware this rank has - which makes it the one number that
        compares a GPU against a CPU, and the placement probe cannot, being an
        integer loop on the CPU.

        Recent rather than cumulative, because the daemon uses it to notice a
        machine that has changed. Falls back to the cumulative figure until
        the window has anything in it.
        """
        return _recent_mean_ms(self.step_recent, self.step_sec[0], self.step_sec[1])

    def _tuned_sync_every(self):
        """How often to average, from measured sync and step times.

        Chosen so sync is at most _SYNC_BUDGET of the wall clock:
            sync/N <= budget * (step + sync/N)  =>  N >= sync*(1-budget)/(step*budget)
        """
        if self.ddp_stats["syncs"] < 3 or self.step_sec[1] < 3:
            return self.sync_tuned or self.sync_every
        sync = self.ddp_stats["sync_sec"] / self.ddp_stats["syncs"]
        step = self.step_sec[0] / self.step_sec[1]
        if step <= 0 or sync <= 0:
            return self.sync_tuned or self.sync_every
        want = sync * (1.0 - self.sync_budget) / (step * self.sync_budget)
        n = max(1, min(self.sync_every_max, int(want + 0.5)))

        if not self.sync_auto:
            # Diagnose regardless. A run that spends nine tenths of itself
            # moving gradients should say so, once, rather than leave the
            # user to conclude the system is simply slow.
            if n > 1 and not self.warned_sync_bound:
                self.warned_sync_bound = True
                _log("ddp: sync-bound — %.0f ms per sync against %.0f ms of compute, "
                     "so %.0f%% of this run is moving gradients. "
                     "--ddp-sync fast would train locally and sync about every %d "
                     "steps (DiLoCo), which is faster and trains a slightly "
                     "different model." % (sync * 1000, step * 1000,
                                           100.0 * sync / (sync + step), n))
            return self.sync_every

        if self.sync_tuned is None:
            self.sync_tuned = n
            if n > 1:
                _log("ddp: averaging every %d steps — a sync costs %.0f ms against "
                     "%.0f ms of compute. This is local SGD: faster, and not the "
                     "same arithmetic as exact DDP." % (n, sync * 1000, step * 1000))
        return self.sync_tuned

    def _ddp_report(self):
        """The receipt for a training run, and its verdict.

        Distributing is not automatically worth it, and the failure is silent:
        the loss comes out right either way. Each rank computes 1/world of the
        data, so one machine would have taken world x the compute; the ring
        takes compute + sync. It pays for itself only while

            sync < compute x (world - 1)

        which is a number this run has measured rather than a rule of thumb.
        Measured on two machines over a home link it predicts both outcomes:
        fp16 sync 607ms against 557ms of compute, ring slower (71.1s vs 55.0s);
        int8 sync 319ms, ring faster (53.0s vs 55.0s). On a GPU, where the same
        step takes 25ms, no encoding gets sync under 25ms and one card wins by
        20x - which is worth being told rather than discovering from a
        stopwatch.
        """
        if self.ddp_stats["barrier_sec"] >= 1:
            _log("ddp: waited %.0f s at the start for every rank to be ready "
                 "(not counted as sync)" % self.ddp_stats["barrier_sec"])
        if not self.ddp_stats["syncs"]:
            return
        sync_total = self.ddp_stats["sync_sec"]
        _log("ddp: %d sync(s), %.1fs total (%.0f ms each), %.1f MiB sent, %.1f MiB received"
             % (self.ddp_stats["syncs"], sync_total,
                1000.0 * sync_total / self.ddp_stats["syncs"],
                self.ddp_stats["sent_bytes"] / 1048576.0,
                self.ddp_stats["recv_bytes"] / 1048576.0))
        skipped = sum(st.skipped for st in self.diloco.values())
        if skipped:
            _log("ddp: rank %d skipped %d batch(es) in local rounds that ran over their "
                 "time budget (not trained this epoch; PIPEDPEER_DILOCO_ROUND_SLACK=0 "
                 "waits for them instead)" % (self.rank, skipped))
        live = self._live_ranks()
        if live < 2 or self.step_sec[1] < 2:
            return
        compute = self.step_sec[0]
        # Machines, not ranks. Two ranks on one laptop are two identities and
        # one set of cores, and multiplying by the rank count there says the
        # machine is twice as fast as itself: measured, a two-rank ring on one
        # machine called itself worth forming on a run that took 191s against
        # 92.8s for that machine alone. A daemon too old to send the count
        # cannot tell us, so the old assumption stands in that case and is the
        # only place it still applies.
        try:
            _machines = int(os.environ.get("PIPEDPEER_DDP_MACHINES", "") or live)
        except ValueError:
            _machines = live
        _machines = max(1, min(_machines, live))
        alone = compute * _machines       # every machine's share, done by one
        together = compute + sync_total
        if together <= 0:
            return
        speedup = alone / together
        # "alone" is this rank doing every shard at its own speed, which is
        # what can be measured from here - not what the fastest machine in the
        # ring would manage. On a mixed cluster the slower rank's estimate is
        # generous, so the comparison is stated as what it is rather than as a
        # speed-up against the best single node. Measured: this said 1.24x on
        # a run that beat a single fast machine by 1.02x.
        if speedup >= 1.05:
            _log("ddp: the ring paid for itself — %.1fs of compute and %.1fs of "
                 "sync across %d ranks on %d machine(s), against about %.1fs for "
                 "THIS machine to have done the whole dataset (%.2fx). A faster "
                 "machine on its own may still beat that."
                 % (compute, sync_total, live, _machines, alone, speedup))
        elif _machines < live:
            _log("ddp: the ring did NOT pay for itself — %d ranks on %d "
                 "machine(s), so the extra ranks brought no extra hardware: "
                 "%.1fs of compute and %.1fs of sync against about %.1fs for "
                 "this machine alone. Co-located ranks divide one machine and "
                 "pay the sync on top; they are for a second accelerator, not "
                 "a second machine."
                 % (live, _machines, compute, sync_total, alone))
        else:
            _log("ddp: the ring did NOT pay for itself — %.1fs of compute and "
                 "%.1fs of sync across %d ranks, against about %.1fs for THIS "
                 "machine alone. Syncing costs more than the extra machines save."
                 % (compute, sync_total, live, alone))

    def _count_sync(self, sec, sent, recv):
        """Book one sync round trip. The first is the opening round, which is
        also the start barrier: its time is mostly waiting for ranks to start,
        so it is kept apart from sync time."""
        if self.seq == 1:
            self.ddp_stats["barrier_sec"] += sec
        else:
            self.ddp_stats["syncs"] += 1
            self.ddp_stats["sync_sec"] += sec
            record.step("ddp_sync", sec, sent + recv)
        self.ddp_stats["sent_bytes"] += sent
        self.ddp_stats["recv_bytes"] += recv
        import threading
        if not threading.current_thread().name.startswith(_SyncThread.NAME):
            self.exposed += sec

    def _live_opts(self):
        return [o for o in (r() for r in self.opts) if o is not None]
