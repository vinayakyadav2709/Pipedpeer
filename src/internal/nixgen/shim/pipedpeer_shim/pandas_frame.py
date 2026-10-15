"""_PartitionedFrame and its groupby/merge."""

from pipedpeer_shim import core
from pipedpeer_shim.core import (
    _OFFLOAD, _log, _offload_warn,
)
from pipedpeer_shim.stats import (
    _record,
)
from pipedpeer_shim.transfer import (
    _daemon_open,
)
from pipedpeer_shim.pandas_combine import (
    _combine_agg_worker_src,
    _combine_means,
    _combine_safe,
    _mean_split,
)


class _PartitionedFrame:
    """Proxy over an out-of-core file: chunks parsed on the cluster (cached
    content-addressed in the warm workers' _CACHE), metadata local. groupby
    and merge run distributed; anything else materializes the chunks back into
    one local DataFrame (the documented fallback)."""

    def __init__(self, keys, cols, meta, path, local=None):
        self._keys = keys
        self._cols = list(cols)
        self._meta = meta
        self._path = path
        # local(idx) re-parses chunks idx from the source file here, in
        # order, one at a time: the fallback when the cluster cannot serve them.
        self._local = local

    @property
    def shape(self):
        return (sum(m["rows"] for m in self._meta), len(self._cols))

    @property
    def columns(self):
        return self._cols

    @property
    def dtypes(self):
        return dict(self._meta[0]["dtypes"])  # ponytail: chunks are uniform

    @property
    def index(self):
        return self._materialize().index

    def __len__(self):
        return sum(m["rows"] for m in self._meta)

    def __iter__(self):
        return iter(self._cols)

    def __getitem__(self, key):
        return self._materialize()[key]

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        _log("partitioned frame: materializing (%s)" % name)
        return getattr(self._materialize(), name)

    def head(self, n=5):
        return self._chunk(0).head(n).reset_index(drop=True)

    def tail(self, n=5):
        if self._meta[-1]["rows"] >= n:
            return self._chunk(len(self._keys) - 1).tail(n).reset_index(drop=True)
        return self._materialize().tail(n)

    def groupby(self, by, **kw):
        return _PartitionedGroupBy(self, by, **kw)

    def merge(self, right, **kw):
        return _partitioned_merge(self, right, **kw)

    def _map(self, what, func_src, extra=None, idx=None):
        """Run func_src's run(df) on each chunk in idx (default: all), on the
        node that cached it; one result per chunk, in order.

        A node can lose its cache (restart, eviction, the pool routing a key
        to a peer that never parsed it): the daemon then answers HTTP 500.
        That must not fail the user's script, so the chunks are re-parsed
        here from the source file, one at a time, with a visible warning."""
        import base64
        import json
        import pickle
        import urllib.request
        idx = list(range(len(self._keys))) if idx is None else list(idx)
        req = {"func_src": func_src, "func_name": "run",
               "items": list(range(len(idx))), "starmap": False,
               "cache_keys": [self._keys[i] for i in idx], "no_split": True}
        if extra is not None:
            req["extra_b64"] = base64.b64encode(pickle.dumps(extra)).decode()
        try:
            r = urllib.request.Request(core._URL + "/v1/pool/map", data=json.dumps(req).encode(),
                                       headers={"Content-Type": "application/json",
                                                "X-Pipedpeer-Store": core._STORE,
                                                "X-Pipedpeer-Offload": _OFFLOAD})
            with _daemon_open(r, 3600) as resp:
                rs = json.loads(resp.read())["results"]
            if len(rs) != len(idx):
                raise RuntimeError("got %d results for %d chunks" % (len(rs), len(idx)))
            return [pickle.loads(base64.b64decode(x["pickle"])) for x in rs]
        except Exception as e:
            if self._local is None:
                raise
            _record("ooc-" + what, len(idx), False, str(e)[:300])
            _offload_warn("%s of %s" % (what, self._path), "the cluster failed (%s)" % str(e)[:200])
            _log("WARNING: %s on the cluster failed (%s); re-reading %d chunk(s) "
                 "of %s on this machine instead - slower, same result"
                 % (what, str(e)[:300], len(idx), self._path))
        ns = dict(extra or {})
        ns["_CACHE"] = {}
        exec(func_src, ns)
        return [ns["run"](df) for df in self._local(idx)]

    def _chunk(self, i):
        """Fetch one chunk (from its node's cache) as a local DataFrame."""
        return self._map("chunk fetch", "def run(df):\n    return df\n", idx=[i])[0]

    def _materialize(self):
        """Fetch all chunks and concat into one local DataFrame."""
        import pandas as _pd
        return _pd.concat(self._map("materialize", "def run(df):\n    return df\n"),
                          ignore_index=True)

    def _combine_agg(self, by, spec, args, kw, gb_kw):
        """Combine-safe agg over the chunks: each chunk node aggregates its
        rows (partials are tiny), the origin re-aggregates the partials —
        exact because sum/count/size/min/max/first/last/any/all compose, and
        mean recombines as sum(sum)/sum(count)."""
        import pandas as _pd
        as_index = gb_kw.get("as_index", True)
        dropna = gb_kw.get("dropna", True)
        named = (isinstance(spec, dict) and spec and all(
            isinstance(v, tuple) and len(v) == 2
            and isinstance(v[0], str) and isinstance(v[1], str)
            for v in spec.values()))
        has_mean, spec_sum, spec_count = _mean_split(spec)
        n = len(self._keys)
        _log('groupby: combining %d chunk partials from the cluster' % n)
        src = _combine_agg_worker_src(has_mean)
        extra = {"_by": by, "_as_index": as_index, "_dropna": dropna,
                 "_spec_sum": spec_sum, "_spec_count": spec_count,
                 "_args": args, "_kwargs": kw}
        if named:
            # named aggregations agg(vm=("v", "mean")) arrive as a dict of
            # pairs; pandas only accepts them as **kwargs on agg, so route
            # them through _named instead of a positional spec.
            extra["_spec_sum"] = None
            extra["_spec_count"] = None
            extra["_named"] = dict(spec_sum)
            extra["_named_count"] = dict(spec_count)
        parts = self._map("groupby", src, extra)
        if has_mean:
            sums = [p[0] for p in parts]
            counts = [p[1] for p in parts]
        else:
            sums = parts
            counts = []
        level = 0 if len(by) == 1 else list(range(len(by)))
        # The chunk partials carry MultiIndex columns (col, func); pandas 3.0
        # cannot dict-agg against MultiIndex labels, so flatten to "col_func"
        # unique names, re-aggregate each column with its own combine op
        # (sums add, mins min, firsts keep first, ...), then restore the
        # MultiIndex layout the caller expects.
        _COMBINE_OPS = {"sum": "sum", "count": "sum", "size": "sum",
                        "prod": "sum", "min": "min", "max": "max",
                        "first": "first", "last": "last", "any": "any",
                        "all": "all"}

        def _combine(parts):
            flat = _pd.concat(parts)
            cols = flat.columns
            if isinstance(cols, _pd.MultiIndex):
                tuples = list(cols)
                flat.columns = ["%s_%s" % (c, f) for c, f in tuples]
                ops = {"%s_%s" % (c, f): _COMBINE_OPS.get(f, "sum") for (c, f) in tuples}
            else:
                tuples = None
                by_cols = set(by)
                ops = {c: (spec if isinstance(spec, str) else "sum")
                       for c in cols if c not in by_cols}
            g = (flat.groupby(level=level, sort=False, dropna=dropna) if as_index
                 else flat.groupby(by, as_index=False, sort=False, dropna=dropna))
            res = g.agg(ops)
            if tuples is not None:
                res.columns = _pd.MultiIndex.from_tuples(tuples)
            return res

        res = _combine(sums)
        if has_mean:
            res_c = _combine(counts)
            res = _combine_means(res, res_c, spec)
        if as_index:
            return res.sort_index()
        return res.reset_index(drop=True).sort_values(by=by, kind="stable").reset_index(drop=True)


class _PartitionedGroupBy:
    """GroupBy over a _PartitionedFrame. agg() with combine-safe specs
    aggregates per chunk and combines at the origin (exact); anything else
    (median, std, nunique, custom funcs...) materializes the chunks first and
    runs the normal hash-shuffle groupby."""

    _NAMED = ("sum", "mean", "count", "size", "min", "max", "first", "last",
              "std", "var", "median", "nunique", "prod", "any", "all",
              "idxmin", "idxmax")

    def __init__(self, pf, by, **kw):
        self._pf = pf
        self._by = by
        self._kw = kw

    def agg(self, spec=None, *args, **kw):
        if spec is None:
            spec = dict(kw)
            kw = {}
        by = self._by
        if isinstance(by, str):
            by = [by]
        if (isinstance(by, list) and by
                and all(isinstance(b, str) and b in self._pf._cols for b in by)):
            if _combine_safe(spec):
                return self._pf._combine_agg(by, spec, args, kw, self._kw)
            _log("partitioned groupby: materializing (%s)" % spec)
            return self._pf._materialize().groupby(self._by, **self._kw).agg(spec, *args, **kw)
        _log("partitioned groupby: materializing (unsupported keys)")
        return self._pf._materialize().groupby(self._by, **self._kw).agg(spec, *args, **kw)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._NAMED:
            def _agg(*args, **kw):
                return self.agg(name, *args, **kw)
            return _agg
        raise AttributeError(name)

    def __getitem__(self, key):
        return _PartitionedGroupByCols(self, key if isinstance(key, list) else [key])


class _PartitionedGroupByCols:
    """gb["v"].mean() / gb[["a", "b"]].sum() over a _PartitionedGroupBy.
    Single-column selection squeezes to a Series like pandas does."""

    def __init__(self, gb, cols):
        self._gb = gb
        self._cols = cols

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in _PartitionedGroupBy._NAMED:
            def _agg(*args, **kw):
                res = self._gb.agg({c: name for c in self._cols}, *args, **kw)
                if len(self._cols) == 1:
                    return res.iloc[:, 0].rename(self._cols[0])
                return res
            return _agg
        raise AttributeError(name)


def _partitioned_merge(pf, right, how="inner", on=None, left_on=None,
                       right_on=None, left_index=False, right_index=False,
                       sort=False, suffixes=("_x", "_y"), **kw):
    """Merge a partitioned frame with a small in-RAM right frame: every chunk
    merges against the FULL right frame on its own node, so matching rows are
    found wherever they live. inner/left are exact (each left row exists in
    exactly one chunk); right/outer would duplicate right-only rows per chunk,
    so those materialize the left frame first (regular shuffle merge)."""
    import pandas as _pd
    if (how not in ("inner", "left")
            or not isinstance(right, _pd.DataFrame)
            or right.memory_usage(deep=False).sum() > 32 * 1024 * 1024):
        _log("partitioned merge: materializing left")
        return pf._materialize().merge(right, how=how, on=on, left_on=left_on,
                                       right_on=right_on, left_index=left_index,
                                       right_index=right_index, sort=sort,
                                       suffixes=suffixes, **kw)
    merge_kw = {"how": how, "on": on, "left_on": left_on, "right_on": right_on,
                "left_index": left_index, "right_index": right_index,
                "sort": False, "suffixes": suffixes}
    merge_kw.update(kw)
    res = _pd.concat(pf._map("merge", ("import pandas as _pd\n"
                                       "def run(df):\n"
                                       "    return _pd.merge(df, _right, **_merge_kw)\n"),
                             {"_right": right, "_merge_kw": merge_kw}),
                     ignore_index=True)
    if sort:
        if left_index or right_index:
            return res.sort_index()
        sc = left_on if left_on is not None else (right_on if right_on is not None else on)
        if isinstance(sc, str):
            sc = [sc]
        if sc:
            return res.sort_values(by=sc, kind="stable").reset_index(drop=True)
    return res
