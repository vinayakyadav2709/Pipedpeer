"""Transparent PyTorch DDP: the interception that turns a single-process
script into one rank of a ring. The run state and its parts live in
ddp_state (shared state), ddp_sync (the wire), ddp_fast (DiLoCo, overlap),
ddp_ring (ring changes, joining) and ddp_loader (sharding)."""
import os
import time
import weakref

from pipedpeer_shim import ddp_diloco
from pipedpeer_shim.core import _log, _warn
from pipedpeer_shim.ddp_diloco import (  # noqa: F401 (_DILOCO_RANKS: read as ddp._DILOCO_RANKS by ddpmany_shim_test.go)
    _DILOCO_H_MAX, _DILOCO_RANKS, _diloco_note, _diloco_warmup,
)
from pipedpeer_shim.ddp_fast import _Fast
from pipedpeer_shim.ddp_loader import _Loader
from pipedpeer_shim.ddp_ring import _Membership
from pipedpeer_shim.ddp_state import _RingState
from pipedpeer_shim.ddp_sync import _Sync
from pipedpeer_shim.ddp_wire import _PAD_SAMPLES
from pipedpeer_shim.ddp_sampler import _push_window


# ---- transparent PyTorch DDP (PIPEDPEER_DDP=1) ----
# pipedpeer run --ddp K starts K ranks, each with PIPEDPEER_RANK /
# PIPEDPEER_WORLD_SIZE / PIPEDPEER_DDP_SYNC (the lead daemon's sync URL) /
# PIPEDPEER_DDP=1 set, so plain single-process training code runs
# data-parallel with zero changes: at the first optimizer.step the ranks
# compare their models and average the weights once, then average gradients
# every step through the lead daemon, broadcast buffers on first forward, and
# split the script's batch and dataset between them. Native torch DDP (the
# user wraps the model themselves) is detected and left to its own sync.
class _Ring(_RingState, _Sync, _Fast, _Membership, _Loader):
    """One rank's training run: the state (ddp_state) and every part that
    reads it, plus the hooks below that torch calls into."""

    def _manifest(self, params):
        """What the lead compares at the opening barrier: every parameter's
        name, shape and dtype, and where this rank runs."""
        names = {}
        for m in list(self.roots):
            try:
                for name, p in m.named_parameters():
                    names.setdefault(id(p), name)
            except Exception as e:
                _log("ddp: cannot name the parameters of %s (%s)" % (type(m).__name__, e))
        device = ""
        if params:
            dev = params[0].device
            device = dev.type
            if dev.type == "cuda":
                try:
                    device = "cuda, " + self.th.cuda.get_device_name(dev)
                except Exception as e:
                    _log("ddp: cannot name this rank's GPU (%s)" % e)
        return {"names": [names.get(id(p), "") for p in params],
                "shapes": [list(p.shape) for p in params],
                "dtypes": [str(p.dtype).replace("torch.", "") for p in params],
                "device": device,
                "host": self.hosts[self.rank] if self.rank < len(self.hosts) else ""}

    def _opt_init(self, opt, *args, **kw):
        self.orig_opt_init(opt, *args, **kw)
        if self.world <= 1 or self.native_ddp:
            return
        self.opts.append(weakref.ref(opt))
        _orig_step = opt.step

        def _step(_self, *sa, **skw):
            if self.catchup:
                # A joiner's placeholder batch for an epoch the ring already
                # trained: no update, no sync, no step counted.
                self.batch_n = 0
                return None
            if not self.sync_url:
                if not self.warned_no_sync:
                    self.warned_no_sync = True
                    _warn("ddp: no sync endpoint was handed to rank %d, so it trains its "
                          "own slice without averaging with the others" % self.rank)
                return _orig_step(*sa, **skw)
            if self._live_ranks() <= 1:
                # Released down to this rank, or every other share gone: a
                # ring of one trains like the script alone, syncing nothing.
                return _orig_step(*sa, **skw)
            n = self.stepn.get(id(opt), 0)
            self.stepn[id(opt)] = n + 1
            params = [p for g in opt.param_groups for p in g["params"]]
            if opt not in self.stepped:
                self.stepped.add(opt)
                # The opening weight broadcast, not a gradient: every rank
                # starts from the same seed, so these are identical by
                # construction and must not be read as duplicated work.
                self._daemon_allreduce([p.data for p in params], half=False, kind="weights",
                                  manifest=self._manifest(params))
                _log("ddp daemon-channel sync ready (rank %d/%d, every %d step%s)"
                     % (self.rank, self.world, self.sync_every, "" if self.sync_every == 1 else "s"))
                if self.fast and not self.told_fast:
                    self.told_fast = True
                    _log(_diloco_note(self.total))
                    if self.rank == 0:
                        # Shown without -v: the ranks no longer run the
                        # script's exact arithmetic.
                        every = ("%d" % ddp_diloco._DILOCO_FIXED_H if ddp_diloco._DILOCO_FIXED_H
                                 else "%d-%d" % (ddp_diloco._DILOCO_H_START, _DILOCO_H_MAX))
                        _warn("training with DiLoCo, sync every %s steps after %d "
                              "exact ones — --ddp-sync exact for per-step sync"
                              % (every, _diloco_warmup(self.total)))
            ep = self.epoch
            if ep is not None:
                ep[1] += 1
                if ep[0].pad and ep[1] == ep[0].total_steps:
                    # This rank's last batch of the epoch is a repeated sample
                    # standing in so every rank takes the same number of
                    # steps: it weighs nothing in the average.
                    self.batch_n = _PAD_SAMPLES
            _now = time.monotonic()
            if self.step_mark[0] is not None:
                _elapsed = _now - self.step_mark[0]
                _synced = self.exposed - self.step_mark[1]
                if _elapsed > _synced >= 0:
                    self.step_sec[0] += _elapsed - _synced
                    self.step_sec[1] += 1
                    _push_window(self.step_recent, _elapsed - _synced, self.step_window)
            self.step_mark[0], self.step_mark[1] = _now, self.exposed

            if self.fast:
                try:
                    return self._fast_step(opt, params, n, _orig_step, sa, skw)
                finally:
                    self.batch_n = 0
            every = self._tuned_sync_every()
            try:
                if every == 1:
                    if self.overlap and self.iters and not self.join:
                        return self._delayed_step(opt, params, _orig_step, sa, skw)
                    grads = [p.grad.coalesce().to_dense() if p.grad.is_sparse else p.grad
                             for p in params if p.grad is not None]
                    if grads:
                        self._daemon_allreduce(grads, key=("grads", id(opt)))
                    return _orig_step(*sa, **skw)
                out = _orig_step(*sa, **skw)
                if (n + 1) % every == 0:
                    # Weights average at full width: a rounding difference in
                    # a weight compounds over the run instead of averaging
                    # out as a gradient's does (R1 #2).
                    self._daemon_allreduce([p.data for p in params], half=False, kind="weights")
                return out
            finally:
                # This step's gradient has been sent with the batch behind
                # it; the next forward pass records its own. Left set, the
                # first batch of the run would be reported for every step
                # and a short final batch never seen.
                self.batch_n = 0

        # A bound method, as the optimizer's own step is: an LR scheduler
        # wraps opt.step through its __func__, and a plain function made
        # every script with a scheduler fail at the scheduler's constructor.
        import types as _types
        opt.step = _types.MethodType(_step, opt)

    def _forward(self, mod, *args, **kw):
        if (self.world > 1 and self.sync_url and not self.native_ddp and not self.catchup
                and mod not in self.fwd_seen and self._live_ranks() > 1):
            # Only sync once training is underway: the optimizer's first
            # step is the rendezvous.
            if self.stepped:
                self.fwd_seen.add(mod)
                bufs = [b for b in mod._buffers.values()
                        if b is not None and b.is_floating_point() and b.numel()]
                if bufs:
                    self._daemon_broadcast(bufs)
        return self.orig_fwd(mod, *args, **kw)

    def _ddp_init(self, ddp, *a, **kw):
        self.native_ddp.append(True)
        _log("native DistributedDataParallel detected; shim sync disabled")
        return self.orig_ddp(ddp, *a, **kw)

    def _batch_pre_hook(self, module, args):
        """Record how many samples this step's forward pass is over.

        A global pre-hook, not a patch of Module.forward. Assigning to
        nn.Module.forward intercepts nothing: every real module defines its
        own forward, and Python resolves the subclass's first - measured, a
        patched Module.forward saw not one call for an ordinary model. So the
        sample count was always zero on the wire, which made the weighted
        gradient average fall back to equal weights (wrong once batches are
        proportional) and left the mid-run refit with no rates to work with.

        Only the first module of a step is recorded. The hook fires for every
        layer too, and an inner layer's leading dimension is not the batch
        once a model reshapes.
        """
        if self.world <= 1:
            return
        if module._buffers and module not in self.buf_seen:
            self.buf_seen.add(module)
            self.buf_mods.append(weakref.ref(module))
        if self.batch_n or self.catchup:
            return
        self.roots.add(module)
        for a in args:
            if hasattr(a, "shape") and getattr(a, "ndim", 0) >= 1:
                try:
                    self.batch_n = int(a.shape[0])
                except Exception:
                    pass
                return

    def _install(self):
        """Patch torch so the script's own calls reach this ring."""
        th, ring = self.th, self
        try:
            th.nn.modules.module.register_module_forward_pre_hook(self._batch_pre_hook)
        except Exception as e:
            _warn("ddp: cannot observe batch sizes (%s); gradients will be averaged "
                  "as equals, which is only right when the batches are" % e)

        if self.fast or self.auto_sync:
            # The run length for DiLoCo's final tightening: a OneCycleLR is the
            # one common place a script states it in optimizer steps (a plain
            # epoch loop's count is not observable).
            ocl = getattr(th.optim.lr_scheduler, "OneCycleLR", None)
            if ocl is not None:
                orig_ocl_init = ocl.__init__

                def _ocl_init(sched, *a, **kw):
                    orig_ocl_init(sched, *a, **kw)
                    ring.total = int(sched.total_steps)
                    _log("ddp: the OneCycleLR says this run is %d steps" % ring.total)
                ocl.__init__ = _ocl_init

        # Plain functions, not bound methods: a class attribute must bind
        # to the instance torch calls it on (the optimizer, module, loader).
        self.orig_ddp = th.nn.parallel.DistributedDataParallel.__init__
        th.nn.parallel.DistributedDataParallel.__init__ = (
            lambda ddp, *a, **kw: ring._ddp_init(ddp, *a, **kw))
        th.optim.Optimizer.__init__ = lambda opt, *a, **kw: ring._opt_init(opt, *a, **kw)
        th.nn.Module.forward = lambda mod, *a, **kw: ring._forward(mod, *a, **kw)
        th.utils.data.DataLoader._pp_shard = lambda loader: ring._shard(loader)
        th.utils.data.DataLoader.__iter__ = lambda loader: ring._iter(loader)
        if self.join:
            th.Tensor.backward = lambda tensor, *a, **kw: ring._backward(tensor, *a, **kw)
            # A GradScaler finds no gradients after a no-op backward and asserts;
            # its step, update and unscale_ sit out the catch-up batch too.
            for cls in {getattr(getattr(th, "amp", None), "GradScaler", None),
                        getattr(getattr(th.cuda, "amp", None), "GradScaler", None)} - {None}:
                for name in ("step", "update", "unscale_"):
                    if name in cls.__dict__:
                        setattr(cls, name, self._catchup_noop(cls.__dict__[name]))
        _log("ddp interception installed")


def _install_ddp():
    if os.environ.get("PIPEDPEER_DDP") != "1":
        return
    try:
        import torch
    except ImportError:
        return
    import numpy
    _Ring(torch, numpy)._install()


_PENDING_PATCHES = {}
