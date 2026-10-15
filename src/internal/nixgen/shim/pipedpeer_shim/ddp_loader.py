"""Transparent DDP's DataLoader wiring: each rank's shard, epoch
boundaries, and where the ring may change."""
import itertools

from pipedpeer_shim.core import _log
from pipedpeer_shim.ddp_diloco import _DILOCO_ROUND_SLACK
from pipedpeer_shim.ddp_sampler import _WeightedShardSampler
from pipedpeer_shim.ddp_wire import _PassEnd


class _Loader:
    """_RingState's DataLoader wiring (see ddp_state)."""

    def _shard(self, loader):
        """Give this rank a disjoint slice of the data.

        Averaging gradients over ranks that all read the SAME data is not
        data-parallel training: every rank recomputes the same work and the
        averaged gradient equals the single-process one, so N machines buy
        nothing but a slower step. Sharding is what makes the ring worth
        having, and it only happened before if the user had built a
        DistributedSampler themselves - which the bundled demo did not.

        The sampler swap is preferred because a skipped batch is still a
        loaded batch: the dataset would be read in full on every rank.
        DataLoader forbids assigning .sampler after construction, but the
        BatchSampler it wraps is an ordinary object, so batched loaders take
        that path and only the unbatched ones fall back to striding.
        """
        ds = getattr(loader, "dataset", None)
        if ds is None:
            return None
        shuffle = isinstance(getattr(loader, "sampler", None),
                             self.th.utils.data.RandomSampler)
        bs = getattr(loader, "batch_sampler", None)
        can_rebatch = (bs is not None and hasattr(bs, "sampler")
                       and isinstance(getattr(bs, "batch_size", None), int)
                       and bs.batch_size > 0)

        # Unequal shares need the batch to grow with the shard, not just the
        # shard. Every rank must take the same NUMBER of steps or the ring
        # deadlocks: each step ends at a barrier, so a rank with three times
        # the data and the same batch size runs three times as many steps and
        # the others are left waiting at a sync that never completes. Scaling
        # both keeps shard/batch - the step count - identical for everyone.
        #
        # So weights are only honoured where the batch size can be changed
        # too. Where it cannot, equal shards are the safe answer and the
        # reason is said out loud rather than discovered as a hang.
        if self.weights is not None:
            if not can_rebatch:
                _log("ddp: this loader's batch size cannot be changed, so the "
                     "measured shares (%s) cannot be used - every rank would "
                     "run a different number of steps and the ring would hang "
                     "at the first sync. Falling back to equal shards."
                     % ", ".join("%.2f" % w for w in self.weights))
            else:
                try:
                    sampler = _WeightedShardSampler(
                        len(ds), self.weights, self.rank, shuffle, bs.batch_size, self.world,
                        drop_last=bool(getattr(bs, "drop_last", False)))
                except Exception as e:
                    _log("ddp: cannot shard this dataset by measured share (%s); "
                         "falling back to equal shards" % e)
                else:
                    bs.sampler = sampler
                    bs.batch_size = sampler.batch
                    sampler.gen = self.ring_gen
                    self.sharded_hash = True
                    self.weights_applied = True
                    _log("ddp: rank %d takes %d of %d samples (share %.0f%%), "
                         "batch %d, %d steps an epoch - the step count is the "
                         "same on every rank, so none waits on another"
                         % (self.rank, len(sampler), len(ds), 100 * self.weights[self.rank],
                            sampler.batch, sampler.steps))
                    return sampler

        # Equal shares take the same hashed sampler, so a rank that dies
        # later hands over only its own samples; DistributedSampler's strided
        # slices would all move.
        if can_rebatch:
            try:
                sampler = _WeightedShardSampler(
                    len(ds), [1.0 / self.world] * self.world, self.rank, shuffle, bs.batch_size, self.world,
                    drop_last=bool(getattr(bs, "drop_last", False)))
            except Exception as e:
                _log("ddp: cannot shard this dataset by hash (%s); using "
                     "DistributedSampler, so a rank that dies loses its data" % e)
            else:
                bs.sampler = sampler
                bs.batch_size = sampler.batch
                sampler.gen = self.ring_gen
                self.sharded_hash = True
                _log("ddp: sharding %d samples across %d ranks" % (len(ds), self.world))
                return sampler

        try:
            sampler = self.th.utils.data.distributed.DistributedSampler(
                ds, num_replicas=self.world, rank=self.rank, shuffle=shuffle)
        except Exception as e:
            _log("ddp: cannot shard this dataset (%s); every rank reads all of it" % e)
            return None
        if can_rebatch or (bs is not None and hasattr(bs, "sampler")):
            bs.sampler = sampler
            _log("ddp: sharding %d samples across %d ranks" % (len(ds), self.world))
            return sampler
        return False        # sharding needed, but only striding is available

    def _iter(self, loader):
        it = self._iter_inner(loader)
        if self.overlap and self.world > 1 and not self.native_ddp and not self.catchup:
            # The pass's last average lands when the pass ends, not a step
            # into the next one, so whatever follows the loop (evaluation, a
            # save) sees the model with every update.
            return _PassEnd(it, self._flush_delayed)
        return it

    def _iter_inner(self, loader):
        sampler = getattr(loader, "sampler", None)
        if self.overlap and self.world > 1 and not self.native_ddp:
            # A loop broken off early never ended its pass: its last average
            # lands here, before the ring may change below.
            self._flush_delayed()
        if self.world > 1 and not self.native_ddp:
            call = self.iters
            self.iters += 1
            if self.join:
                if self.join_at is None:
                    self.join_at = self._join_ring()
                if call < self.join_at:
                    # An epoch the ring finished before this rank arrived.
                    self.epochs[loader] = self.epochs.get(loader, 0) + 1
                    self._begin_catchup()
                    return self._catchup_batch(loader)
                self._end_catchup()
                self._load_join_buffers()
                self.join = False
                self._restore_join_lr()
            # An epoch boundary is the one place the ring can change safely:
            # the sampler re-slices here anyway, so no rank is part-way
            # through a shard when the boundaries move.
            if self.pending is not None:
                self._ring_switch(call)
            if not isinstance(sampler, self.th.utils.data.distributed.DistributedSampler):
                shard = self.sharded.get(id(loader))
                if shard is None:
                    shard = loader._pp_shard()
                    self.sharded[id(loader)] = shard
                if shard is False:
                    # No sampler to swap: hand out every _WORLD-th batch. The
                    # slices are still disjoint, which is what correctness
                    # needs; only the loading is wasteful.
                    epoch = self.epochs.get(loader, 0)
                    self.epochs[loader] = epoch + 1
                    return itertools.islice(self.orig_iter(loader), self.rank, None, self.world)
                sampler = shard
            # A loader sharded before the ring last changed is resliced at its
            # own next epoch, whichever loader took the change.
            if (isinstance(sampler, _WeightedShardSampler)
                    and getattr(sampler, "gen", 0) != self.ring_gen):
                new = self.weights if self.weights is not None else [1.0 / self.world] * self.world
                bs = getattr(loader, "batch_sampler", None)
                try:
                    reshared = _WeightedShardSampler(
                        sampler.n, new, self.rank, sampler.shuffle, sampler.base, self.world,
                        drop_last=sampler.drop_last)
                except Exception as e:
                    sampler.gen = self.ring_gen
                    _log("ddp: cannot take the ring's new shares (%s); keeping "
                         "the ones in use" % e)
                else:
                    reshared.gen = self.ring_gen
                    if bs is not None:
                        bs.sampler = reshared
                        bs.batch_size = reshared.batch
                    self.sharded[id(loader)] = reshared
                    sampler = reshared
                    _log("ddp: rank %d of %d now takes %d samples, batch %d"
                         % (self.rank, self._live_ranks(), len(reshared), reshared.batch))

            if hasattr(sampler, "set_epoch"):
                sampler.set_epoch(self.epochs.get(loader, 0))
                self.epochs[loader] = self.epochs.get(loader, 0) + 1
            # Steps are counted from here, to know when this epoch's last
            # (possibly pad) batch is the one being stepped.
            # ponytail: one counter, for the loader iterated last; a loop that
            # interleaves two sharded loaders' steps would need one each.
            self.epoch = [sampler, 0] if isinstance(sampler, _WeightedShardSampler) else None
            if self.fast and self.epoch is not None and _DILOCO_ROUND_SLACK > 0:
                return self._timed_rounds(self.orig_iter(loader), self.epoch)
        return self.orig_iter(loader)
