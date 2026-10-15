"""Transparent DDP's ring changes: rebalance notes, releases, a rank
joining mid-run and its catch-up epochs."""
import os
import sys
import weakref

from pipedpeer_shim.core import _TOKEN, _log, _warn
from pipedpeer_shim.ddp_diloco import _DiLoCo, _diloco_use_every
from pipedpeer_shim.ddp_wire import _EXIT_RELEASED, _read_frames


class _CatchupOut:
    """The joiner's stdout during catch-up: each line is tagged, so the
    placeholder epochs' prints are not read as training."""

    def __init__(self, inner, rank):
        self._inner = inner
        self._rank = rank
        self._bol = True

    def write(self, s):
        out = []
        for part in s.splitlines(True):
            if self._bol:
                out.append("[rank %d catch-up, not trained] " % self._rank)
            out.append(part)
            self._bol = part.endswith("\n")
        self._inner.write("".join(out))
        return len(s)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _Membership:
    """_RingState's ring changes (see ddp_state)."""

    def _apply_rebalance(self, reply):
        """Take the daemon's verdict on this ring.

        Neither answer is acted on here. New shares wait for an epoch
        boundary, where the sampler re-slices anyway; a drop waits for the
        same place, because leaving mid-epoch would strand the other ranks at
        a barrier expecting a gradient that is no longer coming.
        """
        w = reply.get("weights")
        try:
            world = int(reply.get("world") or self.world)
        except (TypeError, ValueError):
            _log("ddp: ignoring a ring size the daemon sent as %r; keeping %d"
                 % (reply.get("world"), self.world))
            world = self.world
        if reply.get("diloco") and self.auto_sync and not self.fast:
            self._go_fast(reply)
        if reply.get("release") is not None:
            self._take_release(reply, w, world)
            return
        if w and len(w) == world and world >= self.world:
            try:
                w = [float(x) for x in w]
            except (TypeError, ValueError):
                _log("ddp: ignoring shares the daemon sent as %r" % (w,))
                w = None
            # A zero share is a rank that has left the ring. The daemon sends
            # six decimals, so thirds sum to 0.999999: renormalised here, the
            # same way on every rank.
            if w and abs(sum(w) - 1.0) < 1e-4 and all(x >= 0 for x in w) and w[self.rank] > 0:
                total = sum(w)
                p = dict(self.pending or {})
                p["weights"] = [x / total for x in w]
                p["world"] = world
                if reply.get("join") is not None:
                    p["join"] = int(reply["join"])
                    p["from"] = int(reply.get("from", 0))
                else:
                    # A note states the whole ring, and names a join for as
                    # long as one is pending. One without it means the join
                    # ended (the joiner died, or took its model), so an
                    # earlier note's join must not survive to the boundary.
                    p.pop("join", None)
                    p.pop("from", None)
                self.pending = p
        if reply.get("down") is not None:
            _log("ddp: %s" % reply.get("why", "rank %s left the ring" % reply.get("down")))
        elif reply.get("join") is not None:
            _log("ddp: %s" % reply.get("why", "rank %s joins the ring" % reply.get("join")))
        if reply.get("drop") == self.rank and not self.dropped:
            self.dropped = True
            _log("ddp: leaving the ring — %s. This rank finishes its own work "
                 "locally; the others carry on without waiting for it."
                 % reply.get("why", "the ring is faster without this rank"))

    def _take_release(self, reply, w, world):
        """D2: one rank alone beats the split. Every rank takes the change at
        the next epoch boundary (_ring_switch): the kept rank's share becomes
        the whole dataset and it stops syncing, the others exit 77. Rank 0
        says so in one plain line, since it is the output the user watches."""
        try:
            keep = int(reply["release"])
            w = [float(x) for x in w]
        except (TypeError, ValueError, KeyError) as e:
            _log("ddp: ignoring a release note the daemon sent as %r (%s)" % (reply, e))
            return
        if len(w) != world or keep < 0 or keep >= world or sum(w) <= 0:
            _log("ddp: ignoring a release note with shares %r for a ring of %d" % (w, world))
            return
        total = sum(w)
        self.pending = {"weights": [x / total for x in w], "world": world, "release": keep}
        if self.rank == 0:
            who = self.hosts[keep] if keep < len(self.hosts) and self.hosts[keep] else "rank %d" % keep
            n = int(reply.get("release_ranks") or self._live_ranks())
            # Shown without -v: it changes where the run trains.
            _warn("one machine is faster alone: %s takes about %.0f ms a step by itself "
                  "against %.0f ms split across %d machines, so it trains on alone from "
                  "the next epoch. To keep the split: --ddp %d"
                  % (who, float(reply.get("solo_ms") or 0), float(reply.get("split_ms") or 0),
                     n, n))

    def _buffer_list(self):
        return [b for m in (r() for r in self.buf_mods) if m is not None
                for b in m._buffers.values() if b is not None]

    def _load_join_buffers(self):
        """Take the ring's buffers once the catch-up batches are behind.

        Not at the join itself: this rank has run no forward pass then, so it
        cannot know its modules, and the catch-up batches would move the
        running stats again afterwards."""
        theirs, self.join_bufs = self.join_bufs, None
        if theirs is None:
            return
        mine = self._buffer_list()
        if len(mine) != len(theirs) or any(a.shape != b.shape for a, b in zip(mine, theirs)):
            _warn("ddp: the ring's %d buffer(s) do not match this rank's %d; keeping "
                 "this rank's own (BatchNorm running stats may differ)"
                 % (len(theirs), len(mine)))
            return
        with self.th.no_grad():
            for a, b in zip(mine, theirs):
                a.copy_(b.to(a.device))
        _log("ddp: took the ring's %d buffer(s) after the catch-up batches" % len(mine))

    def _ddp_base(self):
        return self.sync_url.rsplit("/sync", 1)[0]

    def _ddp_request(self, url, body=None, ctype="application/json", timeout=60):
        """One call to the lead daemon; on refusal, the daemon's own words."""
        import urllib.error
        import urllib.request
        req = urllib.request.Request(url, data=body)
        if body is not None:
            req.add_header("Content-Type", ctype)
        if _TOKEN:
            req.add_header("X-Pipedpeer-Token", _TOKEN)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()
        except (urllib.error.URLError, OSError) as e:
            # The lead daemon is gone or unreachable: status 0, so every
            # caller reports it in its own words instead of a traceback.
            _log("ddp: %s failed: %s" % (url.split("?")[0], getattr(e, "reason", e)))
            return 0, str(getattr(e, "reason", e)).encode()

    def _send_snapshot(self, join, boundary, world, weights):
        """Hand this rank's model and optimizer to a rank joining the ring.

        Taken at the epoch boundary where the ring grows, when every rank
        holds the same weights and optimizer state, so one copy is the ring's.
        """
        import io
        import json
        import struct
        opts = self._live_opts()
        state = {
            "params": [[p.detach().cpu() for g in o.param_groups for p in g["params"]]
                       for o in opts],
            "opts": [o.state_dict() for o in opts],
            "stepn": [self.stepn.get(id(o), 0) for o in opts],
            "buffers": [b.detach().cpu() for b in self._buffer_list()],
        }
        header = json.dumps({"group": self.group, "rank": self.rank, "join": join,
                             "boundary": boundary, "seq": self.seq, "world": world,
                             "weights": weights,
                             "sync_every": int(self.sync_tuned or 0),
                             # The DiLoCo schedule's shared state, per
                             # optimizer, so the joiner decides as the ring.
                             "diloco": [self._diloco_shared(o) for o in opts]}).encode()
        # Header, length and model in one buffer, sent as a view of it: the
        # model used to be copied out of the buffer and then again into the
        # body, three copies of a multi-GB model at the epoch boundary.
        buf = io.BytesIO()
        buf.write(header + b"\n" + b"\0\0\0\0")
        start = buf.tell()
        self.th.save(state, buf)
        size = buf.tell() - start
        buf.seek(start - 4)
        buf.write(struct.pack(">I", size))
        code, reply = self._ddp_request(self._ddp_base() + "/snapshot", buf.getbuffer(),
                                   "application/vnd.pipedpeer.ddp", timeout=240)
        if code != 200:
            raise RuntimeError("HTTP %d: %s" % (code, reply[:300]))
        _log("ddp: handed the model and optimizer to joining rank %d (%.1f MiB)"
             % (join, size / 1048576.0))

    def _drop_joiner(self, join, world, why):
        """Tell the lead daemon a joining rank will never get its model.

        The ring grew to count it at this boundary, so without this every
        sync would wait out the partial window for a rank that never posts,
        and the refit stays off. Dropped, its share moves back to the ranks
        the ring had."""
        import json
        code, reply = self._ddp_request(self._ddp_base() + "/down", json.dumps(
            {"group": self.group, "rank": join, "world": world,
             "why": "the model could not be handed to it: %s" % why}).encode())
        if code != 200:
            _log("ddp: could not tell the ring to drop joining rank %d (HTTP %d %s); "
                 "every sync waits out the partial window until it does"
                 % (join, code, reply[:200]))

    def _ring_switch(self, call):
        """Take the ring's pending change at this epoch boundary."""
        if self.fast:
            self._diloco_settle()
        p, self.pending = self.pending, None
        world = int(p.get("world") or self.world)
        w = p.get("weights")
        if p.get("release") is not None and p["release"] != self.rank:
            _log("ddp: rank %d released at this epoch: rank %s trains on alone"
                 % (self.rank, p["release"]))
            import atexit
            try:
                # The receipt and the ring's report, which os._exit would skip.
                atexit._run_exitfuncs()
            except Exception as e:
                _log("ddp: exit handlers failed while releasing rank %d (%s)" % (self.rank, e))
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(_EXIT_RELEASED)
        if world > self.world:
            if p.get("from") == self.rank and p.get("join") is not None:
                try:
                    self._send_snapshot(p["join"], call, world, w)
                except Exception as e:
                    _log("ddp: could not hand the model to joining rank %s (%s); "
                         "the ring carries on and drops it" % (p["join"], e))
                    self._drop_joiner(int(p["join"]), world, str(e)[:200])
            _log("ddp: the ring grows from %d to %d ranks at this epoch"
                 % (self._live_ranks(), self._live_ranks(w, world)))
        self.world = world
        if w is not None:
            self.weights = list(w)
            self.weights_applied = True
        self.ring_gen += 1

    def _join_ring(self):
        """Wait for the ring's next epoch boundary and take its model.

        Says it is ready, then waits for the rank handing over the model to
        reach the boundary. Loads the parameters and optimizer state into
        this rank's own optimizers (built by the same script, so the same
        shapes in the same order), takes the ring's sync count, size and
        shares, and returns the epoch it joins at.
        """
        import io
        import json
        import urllib.parse
        code, reply = self._ddp_request(self._ddp_base() + "/join", json.dumps(
            {"group": self.group, "rank": self.rank, "world": self.world}).encode())
        if code != 200:
            raise RuntimeError("ddp: the ring would not take rank %d: %s"
                               % (self.rank, reply[:300].decode("utf-8", "replace")))
        _log("ddp: rank %d is ready; waiting for the ring's next epoch to join" % self.rank)
        url = self._ddp_base() + "/snapshot?group=%s&rank=%d" % (urllib.parse.quote(self.group), self.rank)
        while True:
            code, data = self._ddp_request(url, timeout=120)
            if code == 200:
                break
            if code != 204:
                raise RuntimeError("ddp: rank %d could not join: HTTP %d %s"
                                   % (self.rank, code, data[:300].decode("utf-8", "replace")))
        nl = data.find(b"\n")
        hdr = json.loads(data[:nl])
        state = self.th.load(io.BytesIO(_read_frames(memoryview(data)[nl + 1:], 1)[0]),
                         map_location="cpu")
        opts = self._live_opts()
        if len(opts) != len(state["opts"]):
            raise RuntimeError(
                "ddp: this rank built %d optimizer(s) before its first epoch and the "
                "ring has %d, so it cannot take the ring's model"
                % (len(opts), len(state["opts"])))
        with self.th.no_grad():
            for o, params, sd, stepn in zip(opts, state["params"], state["opts"], state["stepn"]):
                mine = [p for g in o.param_groups for p in g["params"]]
                if len(mine) != len(params):
                    raise RuntimeError("ddp: the ring's model has %d tensors and this "
                                       "rank's %d" % (len(params), len(mine)))
                for p, v in zip(mine, params):
                    p.data.copy_(v.to(p.device))
                o.load_state_dict(sd)
                # The ring broadcast its opening weights long ago; this rank
                # has them now, and a broadcast of its own would be a sync
                # nobody else makes.
                self.stepped.add(o)
                self.stepn[id(o)] = int(stepn)
        for o, d in zip(opts, hdr.get("diloco") or []):
            if d:
                # The ring runs DiLoCo; an auto ring switched to it (U32).
                self.fast = self.fast or self.auto_sync
                st = self.diloco[id(o)] = _DiLoCo()
                st.refs = [float(x) for x in d.get("refs") or []]
                st.ref = d.get("ref")
                st.started = bool(d.get("started"))
                if d.get("h") is not None:
                    _diloco_use_every(int(d["h"]))   # the ring's interval, not this size's
        self.join_bufs = state.get("buffers")
        self.join_lr = [(weakref.ref(o), [g.get("lr") for g in o.param_groups])
                       for o in opts]
        self.seq = int(hdr["seq"])
        self.world = int(hdr["world"])
        if hdr.get("weights"):
            self.weights = [float(x) for x in hdr["weights"]]
            self.weights_applied = True
        if self.sync_auto and hdr.get("sync_every"):
            self.sync_tuned = int(hdr["sync_every"])
        _log("ddp: rank %d joins the ring of %d with rank %s's model and optimizer, "
             "skipping the %d epoch(s) the ring already did"
             % (self.rank, self._live_ranks(), hdr.get("rank", 0), int(hdr["boundary"])))
        return int(hdr["boundary"])

    def _restore_join_lr(self):
        """Put back the learning rate the ring handed over.

        The script's own per-epoch code still ran over the skipped epochs, so
        a scheduler stepped once per epoch decayed the loaded rate a second
        time for every one of them - the ring's rate is already decayed for
        those epochs. Its step count now matches the ring's, so only the
        rate is restored."""
        for ref, lrs in self.join_lr or ():
            o = ref()
            if o is None:
                continue
            for g, lr in zip(o.param_groups, lrs):
                if lr is not None and g.get("lr") != lr:
                    _log("ddp: learning rate %g restored to the ring's %g after "
                         "the skipped epochs stepped this rank's scheduler"
                         % (g.get("lr"), lr))
                    g["lr"] = lr
        self.join_lr = None

    def _begin_catchup(self):
        if not self.catchup:
            self.catchup = True
            self.catchup_out = sys.stdout
            sys.stdout = _CatchupOut(sys.stdout, self.rank)

    def _end_catchup(self):
        if self.catchup:
            self.catchup = False
            if isinstance(sys.stdout, _CatchupOut):
                sys.stdout.flush()
                sys.stdout = self.catchup_out

    def _catchup_batch(self, loader):
        """One real batch for an epoch the ring already did.

        An empty epoch left the script's loop variables unbound, so a joiner
        died on the first print(loss) after its loop. One batch binds them;
        backward and the optimizer step are no-ops while _CATCHUP is set, so
        nothing trains, syncs or counts twice."""
        for batch in self.orig_iter(loader):
            yield batch
            break

    def _backward(self, tensor, *a, **kw):
        if self.catchup:
            return None
        return self.orig_backward(tensor, *a, **kw)

    def _catchup_noop(self, fn):
        def wrapped(*a, **kw):
            if self.catchup:
                return None
            return fn(*a, **kw)
        return wrapped
