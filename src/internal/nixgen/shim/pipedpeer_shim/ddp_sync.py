"""Transparent DDP's wire: every sync with the lead daemon (or a shard's)."""
import math
import os
import sys
import time

from pipedpeer_shim.core import _TOKEN, _log, _warn
from pipedpeer_shim.ddp_wire import (
    _EXIT_ABORTED, _EXIT_MISSED_START, _ShardLost, _int8_blocks, _int8_decode,
    _int8_encode, _read_frames, _shard_cuts,
)


class _Sync:
    """_RingState's wire (see ddp_state)."""

    def _lead_name(self):
        if self.hosts and self.hosts[0]:
            return self.hosts[0]
        import urllib.parse
        return urllib.parse.urlparse(self.sync_url).hostname or "(unknown)"

    def _stop(self, code, reason):
        """End this rank now with a C3 code, after the one line that says why.

        os._exit rather than an exception: the ring is gone, and a script's
        own except/finally could wait on a sync that will never answer. The
        CLI stops every other rank when it sees 76."""
        sys.stdout.flush()
        sys.stderr.write("pipedpeer: training stopped: %s\n" % reason)
        sys.stderr.flush()
        os._exit(code)

    def _watch_lead(self):
        """Notice a lead that vanished mid-sync within about 30 s.

        A sync may legitimately wait minutes (the start barrier, a slow
        rank), and a machine that drops off the network never closes the
        connection, so the POST alone would sit out its whole timeout. While a
        sync is in flight a thread asks the lead's daemon for the round's
        status; three unanswered checks in a row end the rank (exit 76)."""
        if self.watching:
            return
        self.watching = True
        import threading
        import urllib.error
        import urllib.parse
        import urllib.request

        def run():
            fails = 0
            while True:
                time.sleep(self.lead_probe_sec)
                since = self.inflight
                if not since or time.monotonic() - since < self.lead_probe_sec:
                    fails = 0
                    continue
                req = urllib.request.Request("%s?group=%s&seq=0" % (
                    self.sync_url, urllib.parse.quote(self.group)))
                if _TOKEN:
                    req.add_header("X-Pipedpeer-Token", _TOKEN)
                try:
                    urllib.request.urlopen(req, timeout=5).close()
                    fails = 0
                except urllib.error.HTTPError:
                    fails = 0  # it answered, whatever it said
                except Exception as e:
                    fails += 1
                    _log("ddp: the lead machine %s did not answer a check (%s), %d of 3"
                         % (self._lead_name(), getattr(e, "reason", e), fails))
                    if fails >= 3 and self.inflight:
                        self._stop(_EXIT_ABORTED, "the lead machine %s stopped answering (%s)"
                              % (self._lead_name(), getattr(e, "reason", e)))
        threading.Thread(target=run, daemon=True, name="pipedpeer-ddp-lead").start()

    def _post(self, req, timeout, opening=False, lost=False):
        """Send one sync to the lead daemon and return the reply body.

        A refusal is reported in the daemon's own words: a bare "HTTP 400"
        hid why the demo's 3-rank run died. 409 with abort is the ring ending
        for every rank (C3 exit 76); 410 on the opening round is this rank
        missing the start barrier (exit 75); no answer at all is the lead
        lost (exit 76). lost=True (a butterfly shard's daemon, not the lead):
        no answer raises _ShardLost instead, and the lead watch is left to
        the lead's own syncs."""
        import json
        import urllib.error
        import urllib.request
        if not lost:
            self._watch_lead()
            self.inflight = time.monotonic()
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8", "replace")
            except Exception as re:
                body = "(no body: %s)" % re
            try:
                info = json.loads(body)
            except ValueError:
                info = None
            if not isinstance(info, dict):
                info = {}
            why = info.get("error") or body[:300]
            if e.code == 409 and info.get("abort"):
                self._stop(_EXIT_ABORTED, why)
            if e.code == 410 and opening:
                # This rank missed the start rather than failed, so pipedpeer
                # can start it again as a joiner.
                _log("ddp: rank %d was not ready when the ring started training, and the "
                     "ring went on without it; stopping this rank (it can join later)" % self.rank)
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit(_EXIT_MISSED_START)
            raise RuntimeError("ddp sync refused by the lead daemon (HTTP %d): %s"
                               % (e.code, why)) from None
        except (urllib.error.URLError, OSError) as e:
            if lost:
                raise _ShardLost(str(getattr(e, "reason", e))) from None
            self._stop(_EXIT_ABORTED, "the lead machine %s stopped answering (%s)"
                  % (self._lead_name(), getattr(e, "reason", e)))
        finally:
            if not lost:
                self.inflight = 0.0

    def _header(self, kind, **extra):
        """A sync's JSON header: who this rank is and what it measured."""
        import json
        h = {"group": self.group, "seq": self.seq, "rank": self.rank, "world": self.world,
             "kind": kind, "sync_every": int(self.sync_tuned or 0),
             "step_ms": self._mean_step_ms(), "sync_ms": self._mean_sync_ms(),
             "samples": int(self.batch_n), "sharded": bool(self.sharded_hash),
             "fixed": self.fixed,
             "sync_mode": "fast" if self.fast else "auto" if self.auto_sync else "exact"}
        h.update(extra)
        return json.dumps(h).encode()

    def _on_reply(self, reply, opening):
        """Everything a reduce reply says besides the numbers."""
        # Adopt the group's agreed interval. Ranks measure their own link and
        # arrive at different numbers, and averaging at different points in
        # each rank's step sequence is not local SGD - it is ranks combining
        # models that have taken different numbers of local steps, which
        # shows up as a worse final loss and nothing else.
        self.drift = reply.get("drift")
        if isinstance(reply.get("steps_ms"), dict):
            self.pace = reply["steps_ms"]
        agreed = reply.get("sync_every", 0)
        if agreed and agreed != self.sync_tuned:
            self.sync_tuned = int(agreed)
        ranks = int(reply.get("ranks", 0) or 0)
        # Not for the opening round: a rank missing there missed the start
        # barrier, and the lead's note that follows says so in its own words.
        if ranks and ranks < self.world and self.last_ranks != ranks and not opening:
            self.last_ranks = ranks
            _log("ddp: this step averaged %d of %d ranks — the others did not answer "
                 "in time. The run continues on the ranks that did; a smaller average "
                 "is a smaller step in the same direction, and every rank applies the "
                 "identical result, so nothing drifts." % (ranks, self.world))
        if (self.weights is not None and not self.weights_applied
                and not self.warned_weights_unused):
            self.warned_weights_unused = True
            _log("ddp: this run measured unequal shares (%s) but the script "
                 "splits its own data, so they were not applied - every rank "
                 "took an equal slice and the ring runs at the slowest rank's "
                 "pace. To use them: give each rank weights[rank] of the data "
                 "instead of X[rank::world]."
                 % ", ".join("%.0f%%" % (100 * w) for w in self.weights))
        self._apply_rebalance(reply)
        if reply.get("same_work") and not self.warned_same_work:
            self.warned_same_work = True
            _log("ddp: every rank produced the SAME gradients, so every rank is "
                 "training on the same data. Averaging identical gradients gives "
                 "exactly the single-process result, so this run is correct and "
                 "pure overhead - the extra machines are doing the same work "
                 "twice.\n"
                 "    Sharding is automatic for a DataLoader. A script that slices "
                 "its tensors by hand has to do it itself, e.g.\n"
                 "      import os\n"
                 "      r = int(os.environ.get('PIPEDPEER_RANK', 0))\n"
                 "      w = int(os.environ.get('PIPEDPEER_WORLD_SIZE', 1))\n"
                 "      X, y = X[r::w], y[r::w]")

    def _barrier_progress(self):
        """Say what this rank is waiting for while the start barrier holds it:
        two and a half silent minutes read as a hang (V4 80). Returns an event
        to set when the wait is over."""
        import threading
        import time as _tw
        done = threading.Event()
        hosts = self.hosts

        def missing():
            import json
            import urllib.parse
            import urllib.request
            try:
                req = urllib.request.Request(
                    "%s?group=%s&seq=1" % (self.sync_url, urllib.parse.quote(self.group)))
                if _TOKEN:
                    req.add_header("X-Pipedpeer-Token", _TOKEN)
                with urllib.request.urlopen(req, timeout=5) as resp:
                    arrived = set(json.loads(resp.read()).get("arrived") or [])
            except Exception as e:
                _log("ddp: could not ask the lead which ranks are ready (%s)" % e)
                arrived = {self.rank}
            names = ["rank %d%s" % (r, " on " + hosts[r] if r < len(hosts) and hosts[r] else "")
                     for r in range(self.world) if r not in arrived]
            return ", ".join(names) or "the other ranks"

        def run():
            t0 = _tw.monotonic()
            if done.wait(2):
                return
            limit = ("%d min" % (self.start_wait // 60) if self.start_wait % 60 == 0
                     else "%.0f s" % self.start_wait)
            _log("ddp: waiting for %s to be ready (closure still arriving, or the "
                 "script not started yet), up to %s" % (missing(), limit))
            while not done.wait(30):
                _log("ddp: still waiting for %s (%.0f s of up to %s)"
                     % (missing(), _tw.monotonic() - t0, limit))
        threading.Thread(target=run, daemon=True).start()
        return done

    def _daemon_exchange(self, payload):
        """One allreduce round trip through the lead rank's daemon.

        Payloads travel as a length-prefixed frame rather than base64 in
        JSON. A gradient is the largest thing this system sends per step and
        it is sent every step; base64 inflated it by a third both on the wire
        and in the daemon's memory, where a full model per rank is already
        held until the slowest rank arrives. The daemon never looks inside
        the payload, so the encoding bought nothing.
        """
        import json
        import struct
        import time as _t
        import urllib.request
        self.sync_thread.drain()
        _t0 = _t.monotonic()
        self.seq += 1
        header = json.dumps({"group": self.group, "seq": self.seq, "rank": self.rank,
                             "world": self.world}).encode()
        body = header + b"\n" + struct.pack(">I", len(payload)) + payload
        req = urllib.request.Request(
            self.sync_url, data=body,
            headers={"Content-Type": "application/vnd.pipedpeer.ddp"})
        if _TOKEN:
            req.add_header("X-Pipedpeer-Token", _TOKEN)
        data = self._post(req, 240)
        nl = data.find(b"\n")
        if nl < 0:
            raise RuntimeError("ddp sync returned no header: %r" % data[:200])
        count = json.loads(data[:nl]).get("blob_frames", 0)
        out = _read_frames(memoryview(data)[nl + 1:], count)
        if len(out) != self.world:
            raise RuntimeError("ddp sync returned %d of %d blobs" % (len(out), self.world))
        # Timed because a training run that is slower distributed than local
        # is the failure mode that matters most here, and without a number
        # for where the step went there is nothing to act on. Measured on two
        # machines over a WAN-ish link: 87 steps at 0.69s of sync each, on a
        # model whose whole compute was 33ms a step.
        self._count_sync(_t.monotonic() - _t0, len(body), len(data))
        return out

    def _pack(self, arrs):
        """Flatten arrays into one contiguous buffer of a single dtype.

        Raw bytes rather than pickle, because the daemon has to be able to
        average them: a reply carrying one model instead of one per rank is
        the difference between distributed training being worth doing on a
        normal link and being 21x slower than one machine. Dropping pickle
        also takes Python's serialiser out of every step.
        """
        if len(arrs) == 1:
            return arrs[0].reshape(-1), [arrs[0].shape], [arrs[0].size]
        flat = self.np.concatenate([a.reshape(-1) for a in arrs])
        return flat, [a.shape for a in arrs], [a.size for a in arrs]

    def _unpack(self, flat, shapes, sizes, dtypes):
        out, off = [], 0
        for shape, size, dt in zip(shapes, sizes, dtypes):
            out.append(flat[off:off + size].reshape(shape).astype(dt, copy=False))
            off += size
        return out

    def _daemon_reduce(self, arrs, wire_dtype, kind="grads", manifest=None, **more):
        """One averaged allreduce through the daemon, in reduce mode. more
        goes into the header (samples= overrides the last batch's count)."""
        flat, shapes, sizes = self._pack(arrs)
        flat = self.np.ascontiguousarray(flat.astype(wire_dtype, copy=False))
        self.sync_thread.drain()
        _t0 = time.monotonic()
        self.seq += 1
        opening = kind == "weights" and self.seq == 1
        extra = {"dtype": wire_dtype.name, "count": int(flat.size),
                 "start_wait": self.start_wait}
        extra.update(more)
        if opening and manifest is not None:
            # The lead compares every rank's parameter list before averaging
            # anything: ranks that built different models stop together with
            # both described, instead of a byte count from the accumulator.
            extra["manifest"] = manifest
        waiting = self._barrier_progress() if opening else None
        try:
            # The opening round is the start barrier and may wait the whole
            # start wait for a rank whose closure is still arriving.
            reply, mean, _, sent, recv = self._reduce_exchange(
                kind, flat, extra, _int8_blocks(sizes), opening=opening)
        finally:
            if waiting is not None:
                waiting.set()
        self._on_reply(reply, opening)
        self._count_sync(time.monotonic() - _t0, sent, recv)
        return self._unpack(mean, shapes, sizes, [a.dtype for a in arrs])

    def _reduce_post(self, url, kind, fields, payload, opening=False, lost=False):
        """One reduce contribution to url. Returns (reply header, mean
        bytes, bytes sent, bytes received)."""
        import json
        import struct
        import urllib.request
        # payload: any contiguous buffer; joined once, never copied twice.
        payload = memoryview(payload).cast("B")
        body = b"".join((self._header(kind, **fields), b"\n", struct.pack(">I", payload.nbytes), payload))
        req = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/vnd.pipedpeer.ddp.reduce"})
        if _TOKEN:
            req.add_header("X-Pipedpeer-Token", _TOKEN)
        # A shard's daemon answers within the daemon's partial wait (45 s)
        # when it is up; one that is not should cost the ring that, not 240 s.
        data = self._post(req, self.start_wait + 60 if opening else (90 if lost else 240),
                     opening=opening, lost=lost)
        nl = data.find(b"\n")
        if nl < 0:
            raise RuntimeError("ddp reduce returned no header: %r" % data[:200])
        return (json.loads(data[:nl]), _read_frames(memoryview(data)[nl + 1:], 1)[0],
                len(body), len(data))

    def _butterfly_on(self, kind, opening):
        """Every rank decides this from shared state alone (the CLI's peer
        list, the ring's size and shares, the sync mode), so all send a round
        the same way. Gradients only: DiLoCo's drift needs the whole vector
        in one place."""
        return (kind == "grads" and not opening and not self.fast
                and len(self.peers) == self.world >= 3 and self._live_ranks() == self.world)

    def _local_on(self, kind, opening):
        """Same-machine pre-averaging: gradients only (a forwarded sum hides
        each rank's spread from DiLoCo's drift), while every local rank is
        still training."""
        if kind != "grads" or opening or self.fast or len(self.local) < 2 or not self.local_url:
            return False
        if self.rank not in self.local or max(self.local) >= self.world:
            return False
        return self.weights is None or all(r < len(self.weights) and self.weights[r] > 0 for r in self.local)

    def _reduce_exchange(self, kind, flat, fields, blocks, opening=False, scales=None):
        """Average flat across the ring: one POST to the lead (the hub), one
        per shard to every rank's daemon (the butterfly), or one to this
        machine's daemon, which averages the ranks here and sends the lead
        one sync for the machine. scales: flat is int8 with one scale per
        block. Returns (the lead's reply header, the mean as flat's dtype,
        the reply's scales, bytes sent, bytes received)."""
        fields.setdefault("samples", int(self.batch_n))
        cuts = _shard_cuts(blocks, self.world) if self._butterfly_on(kind, opening) else None
        if cuts is None:
            url = self.sync_url
            if self._local_on(kind, opening):
                url = self.local_url + "/v1/ddp/sync"
                fields = dict(fields, local=self.local, upstream=self.sync_url)
            reply, raw, sent, recv = self._reduce_post(url, kind, fields, flat, opening)
            return (reply, self.np.frombuffer(raw, dtype=flat.dtype), reply.get("scales"),
                    sent, recv)

        def shard(k):
            lo, hi, blo, bhi = cuts[k]
            f = dict(fields, count=int(hi - lo), shard=k, shards=self.world)
            if scales is not None:
                f.update(scales=scales[blo:bhi], counts=blocks[blo:bhi], scale=scales[blo])
            part = flat[lo:hi]
            if k == 0 or k in self.peer_dead:
                return self._reduce_post(self.sync_url, kind, f, part)
            try:
                return self._reduce_post(self.peers[k], kind, f, part, lost=True)
            except _ShardLost as e:
                # Every other rank finds the same daemon silent and does the
                # same, so they meet on the lead under this shard's key.
                self.peer_dead.add(k)
                _warn("ddp: rank %d's machine did not answer its share of the sync (%s); "
                      "the lead averages that share from now on" % (k, e))
                return self._reduce_post(self.sync_url, kind, f, part)

        if not self.shard_pool:
            from concurrent.futures import ThreadPoolExecutor
            self.shard_pool.append(ThreadPoolExecutor(
                max(1, self.world - 1), thread_name_prefix="pipedpeer-ddp-shard"))
        futs = [self.shard_pool[0].submit(shard, k) for k in range(1, self.world)]
        got = [shard(0)] + [f.result() for f in futs]
        mean = self.np.concatenate([self.np.frombuffer(g[1], dtype=flat.dtype) for g in got])
        rscales = None
        if scales is not None:
            rscales = []
            for g in got:
                rscales.extend(g[0].get("scales") or [float(g[0].get("scale", 1.0)) or 1.0])
        return (got[0][0], mean, rscales, sum(g[2] for g in got), sum(g[3] for g in got))

    def _daemon_reduce_int8(self, arrs, key, kind="grads", **more):
        """Average gradients as signed bytes, carrying the rounding error.

        Each rank scales its values into [-127, 127], one scale per block of
        _INT8_BLOCK values, and sends the scales in the header; the daemon
        decodes, sums in float64 and returns the mean the same way. Every
        rank gets identical bytes back, so the ranks hold exactly the same
        model, which is the property that matters.

        What rounding drops is kept in _ERR and added to the next step's
        gradient; the lead keeps its reply's rounding the same way. Without
        that, small gradients round to zero every step and never move the
        model at all; with it they accumulate until they are large enough to
        survive a round - the standard error-feedback trick, and the reason
        this is usable rather than merely smaller.
        """
        flat, shapes, sizes = self._pack(arrs)
        counts = _int8_blocks(sizes)
        if int(more.get("samples", self.batch_n)) < 0:
            # A pad step weighs nothing: send zeros and keep the carried
            # error for the next real gradient.
            q = self.np.zeros(flat.size, dtype="int8")
            scales = [1.0] * len(counts)
        else:
            # Our own buffer, written in place below: _pack's concatenation
            # already is one; a lone tensor's array is the gradient itself.
            flat = flat.astype("float32", copy=len(arrs) == 1)
            prev = self.err.get(key)
            if prev is not None and prev.shape == flat.shape:
                flat += prev
            q, scales, dequant = _int8_encode(flat, counts)
            if not all(math.isfinite(s) for s in scales):
                # A block's peak is inf or NaN: int8 cannot carry it, and its
                # scale would reach the lead as JSON Infinity (HTTP 400).
                self._stop(_EXIT_ABORTED, self._nonfinite_why(kind))
            # Whatever quantisation lost: carried to the next step.
            flat -= dequant
            self.err[key] = flat

        self.sync_thread.drain()
        _t0 = time.monotonic()
        self.seq += 1
        fields = dict(dtype="int8", count=int(q.size), scale=scales[0] if scales else 1.0,
                      scales=scales, counts=counts, **more)
        reply, raw_mean, rscales, sent, recv = self._reduce_exchange(
            kind, q, fields, counts, scales=scales)
        self._on_reply(reply, False)
        mean = _int8_decode(raw_mean, rscales or [float(reply.get("scale", 1.0)) or 1.0], counts)
        self._count_sync(time.monotonic() - _t0, sent, recv)
        return self._unpack(mean, shapes, sizes, [a.dtype for a in arrs])

    def _nonfinite_why(self, kind):
        why = ("rank %d's %s for sync %d hold inf or NaN: the training diverged"
               % (self.rank, "gradients" if kind == "grads" else kind, self.seq + 1))
        if self.overlap and kind == "grads":
            # FIX-OVERLAP: a one-step-late gradient under heavy momentum
            # diverges where the same script on one machine does not.
            why += (". PIPEDPEER_DDP_OVERLAP=1 applies every gradient one step late, "
                    "which diverges at this script's learning rate and momentum: "
                    "run without it")
        return why

    def _daemon_allreduce(self, tensors, half=None, kind="grads", manifest=None, key=None,
                          samples=None):
        """Average tensors across ranks in place, through the daemon channel.

        Gradients cross the wire as float16 by default. This is the dominant
        cost of a step on any link slower than a datacentre LAN - the demo is
        bounded by sync time, not compute - and halving the bytes halves it.
        The averaging itself is still done in float64 and written back at the
        tensor's own dtype, so the loss of precision is confined to transport,
        which is the same trade PyTorch's own fp16 compression hook makes.
        PIPEDPEER_DDP_FP32=1 turns it off; weight broadcasts never use it.

        key: what the int8 rounding error is carried under - the optimizer,
        not id(tensors[0]): zero_grad(set_to_none=True) makes a new gradient
        tensor every step, so a tensor's id lost the error each step and left
        a model-sized array behind in _ERR. samples: the batch behind these
        gradients, when the caller read it earlier (an overlapped sync runs
        after the next step has reset _BATCH_N).
        """
        import pickle
        more = {} if samples is None else {"samples": int(samples)}
        if self.dropped or self._live_ranks() <= 1:
            # A ring of one (the others released, or every other share
            # gone) has nobody to average with: it stops syncing entirely.
            # Told to leave. Posting anyway would have the others average a
            # rank the daemon has stopped expecting; not posting is what lets
            # them complete without waiting. This rank keeps training on its
            # own shard, which is harmless - its gradients simply stop
            # reaching anybody.
            return
        if half is None:
            half = self.fp16_grads
        arrs = [t.detach().cpu().numpy() for t in tensors]
        # Only float32 narrows to float16; float64 tensors keep their range,
        # and integer buffers would be corrupted outright.
        f16 = self.np.dtype("float16")
        # Reduce mode when the payload is uniform enough for the daemon to
        # average it, which is the ordinary case: one dtype across every
        # gradient. Anything mixed falls back to the blackboard, where the
        # daemon does not need to understand what it is holding.
        wire_dtypes = {f16 if half and a.dtype == self.np.float32 else a.dtype for a in arrs}
        if self.int8_grads and kind == "grads" and len(wire_dtypes) == 1:
            # Checked before any float16 copy is made: int8 never uses one
            # (it cost ~0.25 s a step at 8.9M params, FIX-DDPCOPY).
            means = self._daemon_reduce_int8(arrs, key=key if key is not None else id(tensors[0]),
                                        **more)
            for i, t in enumerate(tensors):
                t.data.copy_(self.th.from_numpy(
                    self.np.ascontiguousarray(means[i].astype(arrs[i].dtype, copy=False))))
            return
        wire = arrs
        if half:
            wire = [a.astype("float16") if a.dtype == self.np.float32 else a for a in arrs]
        if len(wire_dtypes) == 1 and next(iter(wire_dtypes)).name in (
                "float16", "float32", "float64"):
            means = self._daemon_reduce(wire, next(iter(wire_dtypes)), kind, manifest, **more)
            for i, t in enumerate(tensors):
                t.data.copy_(self.th.from_numpy(
                    self.np.ascontiguousarray(means[i].astype(arrs[i].dtype))).to(t.device))
            return
        blobs = self._daemon_exchange(pickle.dumps(wire, protocol=pickle.HIGHEST_PROTOCOL))
        peers = [pickle.loads(b) for b in blobs]
        for i, t in enumerate(tensors):
            acc = peers[0][i].astype("float64", copy=True)
            for pr in peers[1:]:
                acc += pr[i]
            acc /= len(peers)
            t.data.copy_(self.th.from_numpy(acc.astype(arrs[i].dtype)).to(t.device))

    def _daemon_broadcast(self, tensors):
        """Overwrite tensors with rank 0's values, through the daemon channel.

        Never reduced precision: this is how every rank agrees on the model
        it is training, and a rounding difference here would compound over
        the run rather than average out as a gradient's would."""
        import pickle
        arrs = [t.detach().cpu().numpy() for t in tensors]
        blobs = self._daemon_exchange(pickle.dumps(arrs, protocol=pickle.HIGHEST_PROTOCOL))
        lead = pickle.loads(blobs[0])
        for i, t in enumerate(tensors):
            t.data.copy_(self.th.from_numpy(lead[i]).to(t.device))
