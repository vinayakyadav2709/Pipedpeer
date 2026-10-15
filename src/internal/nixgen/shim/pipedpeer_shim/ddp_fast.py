"""Transparent DDP's DiLoCo driver (--ddp-sync fast) and the opt-in
one-step-delayed overlap (PIPEDPEER_DDP_OVERLAP=1)."""
import time

from pipedpeer_shim import ddp_diloco
from pipedpeer_shim.core import _log, _warn
from pipedpeer_shim.ddp_diloco import (
    _DILOCO_ENV_H, _DILOCO_REF_WINDOW, _DILOCO_ROUND_SLACK, _DILOCO_UNWEIGHTED,
    _DiLoCo, _diloco_cap, _diloco_epoch_cap, _diloco_next_interval,
    _diloco_round, _diloco_use_every, _diloco_warmup, _round_budget,
)
from pipedpeer_shim.ddp_sampler import _push_window
from pipedpeer_shim.ddp_wire import _SKIP_END, _DelayedGrads


class _Fast:
    """_RingState's DiLoCo driver and overlap (see ddp_state)."""

    def _go_fast(self, reply):
        """U32: the lead measured DiLoCo faster than both the exact split and
        one rank alone. Every rank reads this note in its reply to the same
        sync, so every rank's next optimizer step is its first under
        _fast_step (which still finishes the exact warm-up if the run is too
        young), with the interval and outer optimizer --ddp-sync fast uses."""
        self.fast = True
        self.told_fast = True
        if _DILOCO_ENV_H is None:
            # The lead's interval for the ring's live size (DILOCO-MANY), the
            # same number in every rank's reply.
            try:
                _diloco_use_every(max(1, int(reply["diloco"])))
            except (TypeError, ValueError):
                _log("ddp: the lead sent DiLoCo interval %r; keeping %d"
                     % (reply.get("diloco"), ddp_diloco._DILOCO_FIXED_H))
        _log("ddp: switching to DiLoCo after %s optimizer step(s)"
             % ",".join(str(n) for n in sorted(self.stepn.values())))
        if self.rank == 0:
            # Shown without -v, like an explicit --ddp-sync fast.
            _warn(reply.get("why") or "training with DiLoCo — --ddp-sync exact "
                  "for per-step sync")

    def _diloco_reduce(self, deltas, extras, samples):
        """_diloco_round's reduce. Pseudo-gradients go as int8 with error
        feedback; a round carrying optimizer state goes whole as float32:
        exp_avg_sq spans orders of magnitude within a block, so int8 would
        round its small entries to zero (huge Adam steps there) and the
        reply's error feedback could push them below zero. That round sends
        the carried int8 error at full width and clears it."""
        round_ms, self.round_ms = self.round_ms, None
        if self.dropped or self._live_ranks() <= 1:
            return None
        self.drift = None
        if _DILOCO_UNWEIGHTED:
            samples = 0         # the daemon weighs a rank that gives none as 1
        # A timed round reports its time per step of the round, skipped
        # batches included: samples / that is the rank's real throughput (x
        # the round length, as for every rank), which is what the refit needs.
        more = {"samples": int(samples)}
        if round_ms:
            more["step_ms"] = round_ms
        if extras or not self.int8_grads:
            err = self.err.pop("delta", None)
            wire, off = [], 0
            for d in deltas:
                d = d.astype("float32")
                if err is not None and err.size >= off + d.size:
                    d += err[off:off + d.size].reshape(d.shape)
                off += d.size
                wire.append(d)
            wire += [e.astype("float32", copy=False) for e in extras]
            means = self._daemon_reduce(wire, self.np.dtype("float32"), "delta", **more)
        else:
            means = self._daemon_reduce_int8(deltas, "delta", "delta", **more)
        return means, self.drift

    def _fast_epoch_cap(self, h):
        ep = self.epoch
        if ep is None:
            return h
        return _diloco_epoch_cap(h, ep[1], int(getattr(ep[0], "total_steps", 0) or 0))

    def _fast_step(self, opt, params, n, orig, sa, skw):
        """Step n of opt under --ddp-sync fast: exact sync through the
        warm-up and after the final tightening, else local steps and a
        DiLoCo round every H. Every branch depends only on the step count,
        the run length and reply data, which every rank shares."""
        st = self.diloco.get(id(opt))
        if st is None:
            st = self.diloco[id(opt)] = _DiLoCo()
        done = n + 1
        total = self.total
        if st.theta is None:
            grads = [p.grad.coalesce().to_dense() if p.grad.is_sparse else p.grad
                     for p in params if p.grad is not None]
            self.drift = None
            if grads:
                self._daemon_allreduce(grads, key=("grads", id(opt)))
            drift = self.drift
            out = orig(*sa, **skw)
            if st.started:
                return out          # tightened to exact for the end of the run
            if done < _diloco_warmup(total):
                if drift is not None:
                    _push_window(st.refs, drift, _DILOCO_REF_WINDOW)
                return out
            # The post-local switch, made once.
            if st.ref is None and st.refs:
                st.ref = sum(st.refs) / len(st.refs)
            cap = _diloco_cap(done, total)
            h = ddp_diloco._DILOCO_H_START if cap is None else min(ddp_diloco._DILOCO_H_START, cap)
            if h > 1:
                h = self._fast_epoch_cap(h)
            if h <= 1 and cap is not None and cap <= 1:
                st.started = True   # too near the end to train locally
                return out
            st.begin(params, h, done)
            self.sync_tuned = h
            if st.ref is None:
                _log("ddp: the lead daemon reports no drift between ranks (an older "
                     "build), so DiLoCo keeps syncing every %d steps" % h)
            _log("ddp: exact sync warm-up done after %d steps (drift reference %s); "
                 "training locally, syncing every %d steps"
                 % (done, "%.3g" % st.ref if st.ref is not None else "none", h))
            return out
        out = orig(*sa, **skw)
        st.samples += max(0, int(self.batch_n))  # a pad step adds no samples
        if done >= st.next_sync:
            self._fast_round_end(st, opt, params, done)
        return out

    def _fast_round_end(self, st, opt, params, done):
        """The DiLoCo round that ends a local phase's interval at step done,
        and the next interval. Reached by the round's last step, or by a
        timed-out rank skipping to it (_timed_rounds)."""
        total = self.total
        if st.t0 is not None and done > st.n0:
            self.round_ms = 1000.0 * (time.monotonic() - st.t0) / (done - st.n0)
        st.t0 = None
        cap = _diloco_cap(done, total)
        last = cap is not None and cap <= 1
        drift = _diloco_round(st, opt, params, self._diloco_reduce, force_state=last)
        if last:
            st.theta = None
            self.sync_tuned = 1
            _log("ddp: DiLoCo back to exact sync for the last %d step(s)" % max(0, total - done))
            return
        h = self._fast_epoch_cap(_diloco_next_interval(st.h, drift, st.ref, done, total))
        if h != st.h:
            _log("ddp: DiLoCo syncs every %d steps now (was %d; drift %s against %s)"
                 % (h, st.h, "%.3g" % drift if drift is not None else "not measured",
                    "%.3g" % st.ref if st.ref is not None else "no reference"))
        st.h, st.next_sync = h, done + h
        self.sync_tuned = h

    def _timed_state(self, ep, given):
        """(optimizer, its _DiLoCo) when this loader's next batch falls in a
        timed local round: fast mode, one optimizer, a local phase, and every
        batch handed out this epoch stepped once (a script that accumulates
        gradients over several batches is not timed: a skipped batch would
        not be a skipped step). None otherwise."""
        if not self.fast or _DILOCO_ROUND_SLACK <= 0 or len(self.diloco) != 1:
            return None
        if self.epoch is not ep or ep[1] != given or self.dropped or self._live_ranks() <= 1:
            return None
        key, st = next(iter(self.diloco.items()))
        if st.theta is None:
            return None
        opt = next((o for o in self._live_opts() if id(o) == key), None)
        if opt is None or st.next_sync - self.stepn.get(key, 0) < 1:
            return None
        return opt, st

    def _timed_rounds(self, it, ep):
        """A fast-mode loader's batches, with each local round held to its
        budget (_DILOCO_ROUND_SLACK). Before handing out a batch: start the
        round's clock if this is its first; if the budget has run out, skip
        the round's remaining batches (loaded, not trained), count them as
        steps so every rank's schedule stays in line, and sync now."""
        given = 0
        for batch in it:
            got = self._timed_state(ep, given)
            if got is not None:
                opt, st = got
                key = id(opt)
                now = time.monotonic()
                if st.t0 is None:
                    st.t0, st.n0 = now, self.stepn.get(key, 0)
                    st.budget = _round_budget(self.pace, st.next_sync - st.n0)
                    if st.budget is None and not self.told_pace:
                        self.told_pace = True
                        _log("ddp: no step times from the lead daemon (an older build, "
                             "or the budget is off), so local rounds are not timed: "
                             "each waits for its slowest rank")
                elif st.budget is not None and now - st.t0 >= st.budget:
                    left = st.next_sync - self.stepn.get(key, 0)
                    skipped = 1
                    while skipped < left and next(it, _SKIP_END) is not _SKIP_END:
                        skipped += 1
                    if skipped < left:
                        _log("ddp: the loader ended %d batch(es) before the round's "
                             "sync; syncing there anyway" % (left - skipped))
                    ep[1] += skipped
                    given += skipped
                    self.stepn[key] = st.next_sync
                    st.skipped += skipped
                    self.step_mark[0] = None    # the skip is not a step's compute
                    _log("ddp: round over its %.2f s budget after %d of %d steps; "
                         "skipping %d batch(es) and syncing"
                         % (st.budget, st.next_sync - st.n0 - left, st.next_sync - st.n0,
                            skipped))
                    params = [p for g in opt.param_groups for p in g["params"]]
                    self._fast_round_end(st, opt, params, st.next_sync)
                    continue
            given += 1
            yield batch

    def _diloco_settle(self):
        """Before the ring changes: end every local phase with a round that
        also syncs the optimizer state, so every rank holds the same model
        and optimizer (the snapshot a joiner takes is the ring's), then
        restart the local phase from there with a fresh outer momentum on
        every rank, the joiner included."""
        for o in self._live_opts():
            st = self.diloco.get(id(o))
            if st is None or st.theta is None:
                continue
            params = [p for g in o.param_groups for p in g["params"]]
            _diloco_round(st, o, params, self._diloco_reduce, force_state=True)
            st.theta, st.started = None, False
            _log("ddp: DiLoCo synced model and optimizer state for the ring change")

    def _delayed_step(self, opt, params, orig, sa, skw):
        """An exact-mode step with PIPEDPEER_DDP_OVERLAP=1: send this step's
        gradients on the sync thread and apply the previous step's average,
        which travelled while this step computed. A pass's first step only
        sends; _flush_delayed applies the last one when the pass ends."""
        entry = self.delayed.get(id(opt))
        if entry is None:
            entry = self.delayed[id(opt)] = (_DelayedGrads(self.sync_thread), orig)
        with_grad = [p for p in params if p.grad is not None]
        # Copies, taken now: the script reuses or frees p.grad next step.
        copies = [(p.grad.coalesce().to_dense() if p.grad.is_sparse else p.grad)
                  .detach().to("cpu", copy=True) for p in with_grad]
        samples = int(self.batch_n)

        def reduce(grads):
            self._daemon_allreduce(grads, key=("grads", id(opt)), samples=samples)
            return grads
        t0 = time.monotonic()
        prev = entry[0].swap(with_grad, copies, reduce)
        self.exposed += time.monotonic() - t0
        if not self.told_overlap:
            self.told_overlap = True
            _log("ddp: overlapping each gradient sync with the next step's compute "
                 "(PIPEDPEER_DDP_OVERLAP=1): every gradient is applied one step late")
        if prev is None:
            return None
        # A parameter with a gradient now but none last step has no average
        # to apply; left as is, the optimizer would apply its local gradient.
        had = {id(p) for p in prev[0]}
        for p in with_grad:
            if id(p) not in had:
                p.grad = None
        return self._apply_delayed(prev, orig, sa, skw, restore=False)

    def _apply_delayed(self, prev, orig, sa, skw, restore):
        """Run the optimizer's own step with an averaged gradient in .grad.
        restore: put the script's gradients back afterwards (a flush runs
        outside the script's step, and its next backward may add to them)."""
        ps, avg = prev
        saved = [p.grad for p in ps] if restore else None
        with self.th.no_grad():
            for p, g in zip(ps, avg):
                g = g.to(p.device)
                if p.grad is None or p.grad.is_sparse or p.grad.shape != g.shape:
                    p.grad = g.to(p.dtype)
                else:
                    p.grad.copy_(g)
        try:
            return orig(*sa, **skw)
        finally:
            if restore:
                for p, g in zip(ps, saved):
                    p.grad = g

    def _flush_delayed(self):
        """Apply every optimizer's pending average now: at the end of a
        loader's pass (so the model after the loop has every update), and
        before an epoch starts or the ring changes."""
        for d, orig in list(self.delayed.values()):
            t0 = time.monotonic()
            prev = d.take()
            self.exposed += time.monotonic() - t0
            if prev is not None:
                self._apply_delayed(prev, orig, (), {}, restore=True)

    def _diloco_shared(self, o):
        st = self.diloco.get(id(o))
        if st is None:
            return {}
        return {"refs": list(st.refs), "ref": st.ref, "started": st.started,
                "h": ddp_diloco._DILOCO_FIXED_H}
