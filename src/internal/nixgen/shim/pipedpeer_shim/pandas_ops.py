"""Distributed pandas: groupby/merge shuffles and out-of-core reads."""
import os
import time

from pipedpeer_shim import core
from pipedpeer_shim import transfer
from pipedpeer_shim.core import (
    _OFFLOAD, _OFFLOAD_HERE, _log, _offload_warn, _spill_min,
)
from pipedpeer_shim.stats import (
    _record,
)
from pipedpeer_shim.transfer import (
    _daemon_open, _pool_required, _pool_send, _should_spill,
)


def _groupby_worker_src():
    return ("import pandas as _pd\n"
            "def run(sub):\n"
            "    return sub.groupby(_by, as_index=_as_index, sort=False, dropna=_dropna, observed=True).agg(_spec, *_args, **_kwargs)\n")


def _groupby_observed(gb):
    """The observed= a groupby really runs with. pandas before 2.1 defaults
    to False, 2.1-2.x stores no_default and treats it as False, 3.x defaults
    to True; an explicit True or False from the call wins."""
    obs = getattr(gb, "observed", False)
    if obs is True or obs is False:
        return obs
    import pandas as _pd
    return int(_pd.__version__.split(".")[0]) >= 3


def _add_unobserved(df, keys, as_index, dropna, spec, args, kw, res):
    """Add the category combinations no row has, as observed=False shows them.

    Every partial runs with observed=True: with observed=False each node
    would emit every category and the concat would repeat them. The missing
    rows come from aggregating an empty frame of the same dtypes, which
    yields every combination with its empty-group value (0 for sum and
    count, NaN for mean) - the value pandas itself gives an empty group.

    A plain key beside a categorical one contributes its observed values to
    the product. An empty frame has none, so such a key is made categorical
    over its observed values for the empty aggregation and cast back after."""
    import pandas as _pd
    e = df.iloc[0:0].copy()
    plain = {}
    for k in keys:
        if not isinstance(df[k].dtype, _pd.CategoricalDtype):
            if not dropna and df[k].isna().any():
                raise ValueError("NaN in a plain key beside a categorical one")
            plain[k] = df[k].dtype
            e[k] = _pd.Categorical([], categories=df[k].dropna().unique())
    empty = e.groupby(keys, as_index=True, sort=False, dropna=dropna,
                      observed=False).agg(spec, *args, **kw)
    if plain:
        empty.index = _pd.MultiIndex.from_arrays(
            [empty.index.get_level_values(k).astype(plain[k]) if k in plain
             else empty.index.get_level_values(k) for k in keys], names=keys)
    if not as_index:
        res = res.set_index(keys)
        if not isinstance(empty, _pd.DataFrame):      # size() and the like
            empty = empty.to_frame(res.columns[0])
    missing = empty[~empty.index.isin(res.index)]
    if len(missing):
        res = _pd.concat([res, missing])
    if not as_index:
        res = res.reset_index()
    return res


def _groupby_shuffle(gb, spec, args, kw):
    _log('groupby: hash-shuffling %d rows across %d nodes' % (len(gb.obj), int(core._NUM_SHARDS)))
    """Hash-shuffle a DataFrameGroupBy.agg: rows bucketed by hash(key)%K,
    bucket 0 aggregated locally, buckets 1..K-1 shipped as no_split items
    (item i -> peer i), so every key's rows land complete on one node and the
    per-node agg is exact for any spec. NaN keys dropped here when
    dropna=True. Result sorted by key (pandas default sort=True)."""
    import pickle
    import pandas as _pd
    df = gb.obj
    names = gb._grouper.names
    keys = [n for n in names if isinstance(n, str) and n in df.columns]
    if not keys or len(keys) != len(names):
        raise ValueError("unsupported groupby keys")
    K = int(core._NUM_SHARDS)
    as_index = gb.as_index
    dropna = getattr(gb, "dropna", True)
    key_vals = df[keys]
    buckets = _pd.util.hash_pandas_object(key_vals, index=False) % K
    if dropna:
        keep = key_vals.notna().all(axis=1)
        buckets = buckets[keep]
        df = df[keep]
    parts = []
    # --offload ships bucket 0 too: this machine keeps no share.
    first = 0 if _OFFLOAD_HERE else 1
    local = df[buckets == 0]
    if first and len(local):
        parts.append(local.groupby(keys, as_index=as_index, sort=False,
                                   dropna=dropna, observed=True).agg(spec, *args, **kw))
    remote = [df[buckets == b] for b in range(first, K) if (buckets == b).any()]
    if remote:
        globals_pickle = pickle.dumps({
            "_by": keys, "_as_index": as_index, "_dropna": dropna,
            "_spec": spec, "_args": args, "_kwargs": kw,
        })
        header = {
            "func_src": _groupby_worker_src(),
            "func_name": "run",
            "items_frames": len(remote),
            "globals": True,
            "no_split": True,
        }
        header["required_mem"] = _pool_required(globals_pickle, [pickle.dumps(d) for d in remote])
        if core._FORCE:
            header["force"] = True
        blobs = _pool_send(header, globals_pickle, [pickle.dumps(d) for d in remote], 1800)
        parts.extend(pickle.loads(p) for p in blobs)
    res = _pd.concat(parts)
    if (any(isinstance(df[k].dtype, _pd.CategoricalDtype) for k in keys)
            and not _groupby_observed(gb)):
        res = _add_unobserved(gb.obj, keys, as_index, dropna, spec, args, kw, res)
    if as_index:
        return res.sort_index()
    return res.reset_index(drop=True).sort_values(by=keys, kind="stable").reset_index(drop=True)


def _groupby_gate(gb):
    """True when a DataFrameGroupBy.agg should shuffle: big enough to pay for
    the transfer, keys are plain or categorical columns (no index/array
    keys)."""
    from pandas.core.groupby import generic as _gbg
    if not isinstance(gb, _gbg.DataFrameGroupBy):
        return False
    if gb.level is not None or getattr(gb, "axis", 0) != 0:
        return False
    df = gb.obj
    try:
        nbytes = df.memory_usage(deep=False).sum()
    except Exception:
        return False
    if not _should_spill(nbytes, 4, op="pandas.groupby"):
        return False
    for n in gb._grouper.names:
        if not (isinstance(n, str) and n in df.columns):
            return False
    return True


def _merge_keys(frame, cols, index):
    """Join-key values of one merge side as a DataFrame (one column per key).
    The key frame keeps the frame's index (duplicates included) so the hash
    buckets align with the rows."""
    if index:
        kf = frame.index.to_frame(index=False)
        kf.index = frame.index
        return kf
    if isinstance(cols, str):
        cols = [cols]
    if not isinstance(cols, list) or not cols:
        raise ValueError("unsupported merge keys")
    if not all(isinstance(c, str) and c in frame.columns for c in cols):
        raise ValueError("unsupported merge keys")
    return frame[cols]


def _merge_shuffle(left, right, kw):
    """Hash-shuffle merge: both frames bucketed by hash of their own join keys
    % K (equal key values hash equal, so matching rows share a bucket), each
    (left_bucket, right_bucket) pair merged on one node, origin concatenates.
    Buckets with no rows on a side ship an empty frame (columns intact) so
    left/right joins keep unmatched rows. sort=True sorts by the join keys;
    sort=False leaves bucket order (documented deviation)."""
    import pickle
    import pandas as _pd
    K = int(core._NUM_SHARDS)
    how = kw.get("how", "inner")
    sort = kw.get("sort", False)
    on = kw.get("on")
    left_on = kw.get("left_on")
    right_on = kw.get("right_on")
    left_index = kw.get("left_index", False)
    right_index = kw.get("right_index", False)
    if how == "cross":
        raise ValueError("cross join unsupported")
    if not (on or left_on is not None or right_on is not None or (left_index and right_index)):
        raise ValueError("unsupported merge keys")
    l_keys = _merge_keys(left, left_on if left_on is not None else on, left_index)
    r_keys = _merge_keys(right, right_on if right_on is not None else on, right_index)
    lb = _pd.util.hash_pandas_object(l_keys, index=False) % K
    rb = _pd.util.hash_pandas_object(r_keys, index=False) % K
    worker_kw = dict(kw)
    worker_kw["sort"] = False
    local_part = None
    items = []
    for b in range(K):
        L = left[lb == b]
        R = right[rb == b]
        if not len(L) and not len(R):
            continue
        if not len(R):
            R = right.iloc[0:0]
        if not len(L):
            L = left.iloc[0:0]
        if b == 0 and not _OFFLOAD_HERE:   # --offload ships bucket 0 too
            local_part = (L, R)
        else:
            items.append((L, R))
    parts = []
    transfer._IN_MERGE = True
    try:
        if local_part is not None:
            L, R = local_part
            parts.append(_pd.merge(L, R, **worker_kw))
    finally:
        transfer._IN_MERGE = False
    if items:
        globals_pickle = pickle.dumps({"_mw": worker_kw})
        header = {
            "func_src": ("import pandas as _pd\n"
                         "def run(item):\n"
                         "    L, R = item\n"
                         "    return _pd.merge(L, R, **_mw)\n"),
            "func_name": "run",
            "items_frames": len(items),
            "globals": True,
            "no_split": True,
        }
        header["required_mem"] = _pool_required(globals_pickle, [pickle.dumps(i) for i in items])
        if core._FORCE:
            header["force"] = True
        blobs = _pool_send(header, globals_pickle, [pickle.dumps(i) for i in items], 1800)
        parts.extend(pickle.loads(p) for p in blobs)
    res = _pd.concat(parts)
    if left_index or right_index:
        if sort:
            return res.sort_index()
        return res
    res = res.reset_index(drop=True)
    if sort:
        sc = left_on if left_on is not None else (right_on if right_on is not None else on)
        if isinstance(sc, str):
            sc = [sc]
        return res.sort_values(by=sc, kind="stable").reset_index(drop=True)
    return res


def _merge_gate(left, right):
    try:
        nbytes = left.memory_usage(deep=False).sum() + right.memory_usage(deep=False).sum()
    except Exception:
        return False
    return _should_spill(nbytes, 2, op="pandas.merge")


def _install_pandas():
    try:
        import pandas as _pd
    except ImportError:
        return
    from pandas.core.groupby import generic as _gbg

    _orig_agg = _gbg.DataFrameGroupBy.agg
    _orig_named = {}
    for _name in ("sum", "mean", "count", "size", "min", "max", "first",
                  "last", "std", "var", "median", "nunique", "prod", "any",
                  "all"):
        _orig_named[_name] = getattr(_gbg.DataFrameGroupBy, _name, None)

    def _named_agg(name, orig):
        def _wrapped(self, *args, **kw):
            try:
                if _groupby_gate(self):
                    return _groupby_shuffle(self, name, args, kw)
            except Exception as e:
                _log("groupby fallback (%s)" % e)
            return orig(self, *args, **kw)
        return _wrapped

    def _agg(self, spec, *args, **kw):
        try:
            if _groupby_gate(self):
                return _groupby_shuffle(self, spec, args, kw)
        except Exception as e:
            _log("groupby fallback (%s)" % e)
        return _orig_agg(self, spec, *args, **kw)

    for _name, _orig in _orig_named.items():
        if _orig is not None:
            setattr(_gbg.DataFrameGroupBy, _name, _named_agg(_name, _orig))
    _gbg.DataFrameGroupBy.agg = _agg

    _orig_merge = _pd.DataFrame.merge
    _orig_join = _pd.DataFrame.join

    def _merge(self, right, how="inner", on=None, left_on=None, right_on=None,
               left_index=False, right_index=False, sort=False,
               suffixes=("_x", "_y"), copy=None, indicator=False,
               validate=None):
        if (isinstance(right, _pd.DataFrame) and not transfer._IN_MERGE
                and _merge_gate(self, right)):
            kw = {"how": how, "on": on, "left_on": left_on,
                  "right_on": right_on, "left_index": left_index,
                  "right_index": right_index, "sort": sort,
                  "suffixes": suffixes, "validate": validate}
            if indicator:
                kw["indicator"] = indicator
            if copy is not None:
                kw["copy"] = copy
            try:
                return _merge_shuffle(self, right, kw)
            except Exception as e:
                _log("merge fallback (%s)" % e)
        return _orig_merge(self, right, how=how, on=on, left_on=left_on,
                           right_on=right_on, left_index=left_index,
                           right_index=right_index, sort=sort,
                           suffixes=suffixes, copy=copy, indicator=indicator,
                           validate=validate)

    def _join(self, other, on=None, how="left", lsuffix="", rsuffix="",
              sort=False, validate=None):
        if (isinstance(other, _pd.DataFrame) and not transfer._IN_MERGE
                and _merge_gate(self, other)):
            kw = {"how": how, "on": None, "left_on": on, "right_on": None,
                  "left_index": on is None, "right_index": True, "sort": sort,
                  "suffixes": (lsuffix, rsuffix), "validate": validate}
            try:
                return _merge_shuffle(self, other, kw)
            except Exception as e:
                _log("join fallback (%s)" % e)
        return _orig_join(self, other, on=on, how=how, lsuffix=lsuffix,
                          rsuffix=rsuffix, sort=sort, validate=validate)

    _pd.DataFrame.merge = _merge
    _pd.DataFrame.join = _join
    _log("pandas groupby/merge/join interception installed")


# ---- out-of-core reads: chunked parse + partitioned frame ----
_OOC_CHUNK = int(float(os.environ.get("PIPEDPEER_OOC_CHUNK") or (64 * 1024 * 1024)))
_COMBINE_SAFE = ("sum", "count", "size", "mean", "min", "max", "first",
                 "last", "any", "all")
_OOC_BATCH_MAX = int(os.environ.get("PIPEDPEER_OOC_BATCH_MAX") or 8)
_OOC_BATCH_CACHE = {"t": 0.0, "n": 2}


def _ooc_batch():
    """Auto-size how many chunks ship per request: parse working set is ~5x a
    chunk, and a node must never hold more than ~40% of its free RAM at once.
    Probes the local daemon's /health available_mem, clamped to
    [1, _OOC_BATCH_MAX]; falls back to 2 when the daemon is unreachable."""
    now = time.monotonic()
    if now - _OOC_BATCH_CACHE["t"] < 5.0:
        return _OOC_BATCH_CACHE["n"]
    n = 2
    if core._URL:
        try:
            import json
            import urllib.request
            hreq = urllib.request.Request(core._URL + "/health")
            with _daemon_open(hreq, 2) as r:
                h = json.loads(r.read().decode())
                avail = h.get("available_mem") or 0
                if avail > 0:
                    n = max(1, int(0.4 * avail / (5 * _OOC_CHUNK)))
        except Exception:
            n = 2
    n = min(n, _OOC_BATCH_MAX)
    _OOC_BATCH_CACHE["t"] = now
    _OOC_BATCH_CACHE["n"] = n
    return n


def _ooc_eligible(path):
    """Out-of-core read when the file is too big to parse in one node's RAM:
    size > 0.5 * MemAvailable (the parse working set is a few x the file).
    PIPEDPEER_OOC_MIN overrides the threshold (bytes) for tests."""
    try:
        size = os.path.getsize(path)
        if not size:
            return False
        override = os.environ.get("PIPEDPEER_OOC_MIN")
        if override:
            return size > float(override)
        if core._FORCE:
            return size > _spill_min()
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    avail = int(line.split()[1]) * 1024
                    return size > 0.5 * avail
    except Exception:
        return False


def _partitioned_read(func_src, extra, keys, items):
    """Send one batch of chunk frames. Batch sizes stay bounded (_ooc_batch),
    so an out-of-core read never builds the whole file in memory; item_keys
    pin each chunk to the peer the daemon's key-hash routing chooses, so a
    later cache_keys fetch for the same key resolves on the node that parsed
    it no matter which batch carried it."""
    import pickle
    globals_pickle = pickle.dumps(extra)
    header = {
        "func_src": func_src,
        "func_name": "run",
        "items_frames": len(items),
        "globals": True,
        "no_split": True,
        "item_keys": keys,
    }
    header["required_mem"] = _pool_required(globals_pickle, items)
    if core._FORCE:
        header["force"] = True
    blobs = _pool_send(header, globals_pickle, items, 3600)
    return [pickle.loads(p) for p in blobs]


def _partitioned_read_csv(path, kw):
    """Chunk the file at line boundaries, parse each chunk on its own node (the
    parse working set is then one chunk), cache chunks in the workers' _CACHE
    and return a _PartitionedFrame proxy over them. Reads and ships a bounded
    batch of chunks at a time, so origin RAM never holds the whole file."""
    import hashlib
    import os
    import pickle
    import pandas as _pd
    import io
    _log('read_csv: streaming %.0f MB out-of-core' % (os.path.getsize(path) / 1e6))
    header = kw.get("header", "infer")
    if header == "infer":
        header = 0
    if header not in (0, None):
        raise ValueError("unsupported header")
    names = kw.get("names")
    if names is None and header == 0:
        sniff_kw = dict(kw)
        sniff_kw["nrows"] = 0
        names = list(_pd.read_csv(str(path), **sniff_kw).columns)
    parse_kw = dict(kw)
    parse_kw.pop("header", None)
    parse_kw.pop("names", None)
    src = ("import hashlib\n"
           "import io\n"
           "import os\n"
           "import pickle\n"
           "import pandas as _pd\n"
           "def run(raw):\n"
           "    k = 'df:' + hashlib.sha256(raw).hexdigest()\n"
           "    if k not in _CACHE:\n"
           "        _CACHE[k] = _pd.read_csv(io.BytesIO(raw), names=_names, header=None, **_csv_kw)\n"
           "        if globals().get('_CHUNK_DIR', ''):\n"
           "            with open(os.path.join(globals()['_CHUNK_DIR'], hashlib.sha256(k.encode()).hexdigest()), 'wb') as f:\n"
           "                pickle.dump(_CACHE[k], f)\n"
           "    return {'rows': len(_CACHE[k]), 'dtypes': {c: str(t) for c, t in _CACHE[k].dtypes.items()}}\n")
    extra = {"_names": names, "_csv_kw": parse_kw}
    all_keys = []
    keys, items, metas = [], [], []
    batch = _ooc_batch()
    for start, end in _csv_ranges(str(path), header):
        with open(str(path), "rb") as f:
            f.seek(start)
            data = f.read(end - start)
        key = "df:" + hashlib.sha256(data).hexdigest()
        all_keys.append(key)
        keys.append(key)
        items.append(pickle.dumps(data))
        if len(items) == batch:
            metas.extend(_partitioned_read(src, extra, keys, items))
            keys, items = [], []
    if items:
        metas.extend(_partitioned_read(src, extra, keys, items))
    cols = names if names is not None else list(metas[0]["dtypes"])

    def _local(idx):
        want = set(idx)
        for i, (start, end) in enumerate(_csv_ranges(str(path), header)):
            if i in want:
                with open(str(path), "rb") as f:
                    f.seek(start)
                    raw = f.read(end - start)
                yield _pd.read_csv(io.BytesIO(raw), names=names, header=None, **parse_kw)
    return _PartitionedFrame(all_keys, cols, metas, str(path), _local)


def _csv_ranges(path, header):
    """Yield (start, end) byte ranges of the data rows, split at line
    boundaries. The header line (header=0) is skipped by starting after the
    first newline; every chunk then parses identically (names, header=None)."""
    import os
    with open(path, "rb") as f:
        head = f.read(4096)
    start = 0
    if header == 0:
        nl = head.find(b"\n")
        if nl < 0:
            raise ValueError("no header line")
        start = nl + 1
    size = os.path.getsize(path)
    pos = start
    with open(path, "rb") as f:
        while pos < size:
            f.seek(pos)
            data = f.read(_OOC_CHUNK)
            if not data:
                break
            nl = data.rfind(b"\n")
            if nl < 0:
                nl = len(data)  # ponytail: pathological single-line file
            end = pos + nl + 1
            yield start, end
            start = end
            pos = end


def _partitioned_read_parquet(path, kw):
    """Chunk at row-group boundaries (origin RAM = one row group), parse each
    on its own node, cache and proxy as _PartitionedFrame."""
    import hashlib
    import io
    import os
    import pickle
    import pandas as _pd
    import pyarrow.parquet as _pq
    _log('read_parquet: streaming %.0f MB out-of-core' % (os.path.getsize(path) / 1e6))
    pf = _pq.ParquetFile(str(path))
    src = ("import hashlib\n"
           "import io\n"
           "import os\n"
           "import pickle\n"
           "import pandas as _pd\n"
           "def run(raw):\n"
           "    k = 'df:' + hashlib.sha256(raw).hexdigest()\n"
           "    if k not in _CACHE:\n"
           "        _CACHE[k] = _pd.read_parquet(io.BytesIO(raw), **_pq_kw)\n"
           "        if globals().get('_CHUNK_DIR', ''):\n"
           "            with open(os.path.join(globals()['_CHUNK_DIR'], hashlib.sha256(k.encode()).hexdigest()), 'wb') as f:\n"
           "                pickle.dump(_CACHE[k], f)\n"
           "    return {'rows': len(_CACHE[k]), 'dtypes': {c: str(t) for c, t in _CACHE[k].dtypes.items()}}\n")
    extra = {"_pq_kw": kw}
    all_keys = []
    keys, items, metas = [], [], []
    batch = _ooc_batch()
    for i in range(pf.metadata.num_row_groups):
        buf = io.BytesIO()
        _pq.write_table(pf.read_row_group(i), buf)
        data = buf.getvalue()
        key = "df:" + hashlib.sha256(data).hexdigest()
        all_keys.append(key)
        keys.append(key)
        items.append(pickle.dumps(data))
        if len(items) == batch:
            metas.extend(_partitioned_read(src, extra, keys, items))
            keys, items = [], []
    if items:
        metas.extend(_partitioned_read(src, extra, keys, items))

    def _local(idx):
        for i in sorted(set(idx)):
            buf = io.BytesIO()
            _pq.write_table(pf.read_row_group(i), buf)
            buf.seek(0)
            yield _pd.read_parquet(buf, **kw)
    return _PartitionedFrame(all_keys, list(metas[0]["dtypes"]), metas, str(path), _local)


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


def _combine_agg_worker_src(has_mean):
    if has_mean:
        return ("import pandas as _pd\n"
                "def run(df):\n"
                "    g = df.groupby(_by, as_index=_as_index, sort=False, dropna=_dropna)\n"
                "    if _spec_sum is None:\n"
                "        s = g.agg(*_args, **_kwargs, **_named)\n"
                "        return (s, g.agg(**_named_count))\n"
                "    s = g.agg(_spec_sum, *_args, **_kwargs)\n"
                "    return (s, g.agg(_spec_count))\n")
    return ("import pandas as _pd\n"
            "def run(df):\n"
            "    g = df.groupby(_by, as_index=_as_index, sort=False, dropna=_dropna)\n"
            "    if _spec_sum is None:\n"
            "        return g.agg(*_args, **_kwargs, **_named)\n"
            "    return g.agg(_spec_sum, *_args, **_kwargs)\n")


def _transform_spec(spec, fn):
    """Map fn over the agg funcs of a spec (str | list | dict), preserving
    shape. Two-element tuples inside a list are named-agg pairs (col, func):
    only the func slot is transformed."""
    if isinstance(spec, str):
        return fn(spec)
    if isinstance(spec, dict):
        return {k: _transform_spec(v, fn) for k, v in spec.items()}
    if isinstance(spec, (list, tuple)):
        if (len(spec) == 2 and isinstance(spec[0], str)
                and isinstance(spec[1], (list, tuple)) and len(spec[1]) == 2
                and isinstance(spec[1][0], str) and isinstance(spec[1][1], str)):
            return (spec[0], _transform_spec(spec[1], fn))  # (name, (col, func))
        if (len(spec) == 2 and isinstance(spec[0], str) and isinstance(spec[1], str)
                and not isinstance(spec, list)):
            return (spec[0], _transform_spec(spec[1], fn))  # (col, func) pair
        out = []
        for s in spec:
            if (isinstance(s, (list, tuple)) and len(s) == 2
                    and isinstance(s[1], str) and not isinstance(s[0], (list, tuple, dict))):
                out.append((s[0], _transform_spec(s[1], fn)))
            else:
                out.append(_transform_spec(s, fn))
        return out
    return spec


def _mean_split(spec):
    """Split a spec into sum- and count- variants (mean recombines as
    sum(sum)/sum(count) across chunks). Returns (has_mean, sum_spec,
    count_spec)."""
    has_mean = [False]

    def _to_sum(s):
        if s == "mean":
            has_mean[0] = True
            return "sum"
        return s

    def _to_count(s):
        return "count" if s == "mean" else s

    spec_sum = _transform_spec(spec, _to_sum)
    spec_count = _transform_spec(spec, _to_count)
    # pandas 3.0 rejects duplicate funcs in list/named specs (which the mean
    # transforms produce, e.g. mean+mean); duplicates carry no information.
    def _dedupe(s):
        if isinstance(s, list):
            out = []
            for x in s:
                if x not in out:
                    out.append(x)
            return out
        if isinstance(s, dict):
            return {c: _dedupe(v) for c, v in s.items()}
        return s
    return has_mean[0], _dedupe(spec_sum), _dedupe(spec_count)


def _spec_has_mean(spec):
    if isinstance(spec, str):
        return spec == "mean"
    if isinstance(spec, dict):
        return any(_spec_has_mean(v) for v in spec.values())
    if isinstance(spec, (list, tuple)):
        return any(_spec_has_mean(s) for s in spec)
    return False


def _combine_means(res_sum, res_count, spec):
    """Divide the sum-partials by the count-partials at the positions where the
    original spec asked for mean, keeping the original column layout. Columns
    explicitly requested as sum/count stay; the working columns behind a mean
    are dropped."""
    import pandas as _pd

    def _first(frame, key):
        c = frame[key]
        return c.iloc[:, 0] if isinstance(c, _pd.DataFrame) else c

    if isinstance(spec, str):
        return res_sum / res_count
    out = res_sum.copy()
    if isinstance(spec, dict):
        multi = isinstance(out.columns, _pd.MultiIndex)
        for col, s in spec.items():
            if _spec_has_mean(s):
                funcs = s if isinstance(s, list) else [s]
                if multi:
                    if "sum" in funcs:
                        keep = [True] * len(out.columns)
                        seen = False
                        for i, c in enumerate(out.columns):
                            if c == (col, "sum"):
                                if seen:
                                    keep[i] = False
                                seen = True
                        out = out.loc[:, keep]
                    out[(col, "mean")] = _first(res_sum, (col, "sum")) / _first(res_count, (col, "count"))
                    drops = []
                    if "sum" not in funcs and (col, "sum") in out.columns:
                        drops.append((col, "sum"))
                    if "count" not in funcs and (col, "count") in out.columns:
                        drops.append((col, "count"))
                    if drops:
                        out = out.drop(columns=drops)
                else:
                    if "sum" not in funcs:
                        out = out.drop(columns=[col])
                    out[col] = _first(res_sum, col) / _first(res_count, col)
        if multi:
            layout = [(c, f) for c, s in spec.items()
                      for f in (s if isinstance(s, list) else [s])
                      if (c, f) in out.columns]
        else:
            layout = [c for c in spec if c in out.columns]
        return out.reindex(columns=layout)
    if isinstance(res_sum.columns, _pd.MultiIndex):
        for col in res_sum.columns.get_level_values(0).unique():
            if "mean" in spec and (col, "sum") in out.columns and (col, "count") in res_count.columns:
                out[(col, "mean")] = _first(res_sum, (col, "sum")) / _first(res_count, (col, "count"))
                if "sum" not in spec and (col, "sum") in out.columns:
                    out = out.drop(columns=[(col, "sum")])
        cols0 = list(res_sum.columns.get_level_values(0).unique())
        layout = [(c, f) for c in cols0 for f in spec if (c, f) in out.columns]
        return out.reindex(columns=layout)
    for name, s in spec:
        if isinstance(s, (list, tuple)) and len(s) == 2 and s[1] == "mean":
            if "sum" not in s:
                out = out.drop(columns=[name])
            out[name] = res_sum[name] / res_count[name]
    return out


def _combine_safe(spec):
    if isinstance(spec, str):
        return spec in _COMBINE_SAFE
    if isinstance(spec, dict):
        return all(_combine_safe(v) for v in spec.values())
    if isinstance(spec, tuple):
        if len(spec) == 2 and isinstance(spec[0], str) and isinstance(spec[1], str):
            return spec[1] in _COMBINE_SAFE  # named pair (name, func)
        return all(_combine_safe(s) for s in spec)
    if isinstance(spec, list):
        for s in spec:
            if (isinstance(s, (list, tuple)) and len(s) == 2
                    and isinstance(s[1], str)
                    and not isinstance(s[0], (list, tuple, dict))):
                if s[1] not in _COMBINE_SAFE:
                    return False
            elif (isinstance(s, tuple) and len(s) == 2 and isinstance(s[0], str)
                    and isinstance(s[1], tuple) and len(s[1]) == 2
                    and isinstance(s[1][0], str) and isinstance(s[1][1], str)):
                if s[1][1] not in _COMBINE_SAFE:
                    return False
            elif not _combine_safe(s):
                return False
        return True
    return False


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


def _install_io():
    try:
        import pandas as _pd
    except ImportError:
        return

    _orig_read_csv = _pd.read_csv
    _orig_read_parquet = _pd.read_parquet
    _FORBIDDEN_CSV = {"skiprows", "skipfooter", "nrows", "usecols"}

    def _read_csv(path, *args, **kw):
        try:
            if (not args and not (_FORBIDDEN_CSV & set(kw))
                    and kw.get("header", "infer") in (0, None, "infer")
                    and _ooc_eligible(str(path))
                    and int(core._NUM_SHARDS) >= 2):
                return _partitioned_read_csv(path, kw)
        except Exception as e:
            _log("ooc csv fallback (%s)" % e)
        return _orig_read_csv(path, *args, **kw)

    def _read_parquet(path, *args, **kw):
        try:
            if (not args and _ooc_eligible(str(path))
                    and int(core._NUM_SHARDS) >= 2):
                return _partitioned_read_parquet(path, kw)
        except Exception as e:
            _log("ooc parquet fallback (%s)" % e)
        return _orig_read_parquet(path, *args, **kw)

    _pd.read_csv = _read_csv
    _pd.read_parquet = _read_parquet
    _log("pandas read_csv/read_parquet interception installed")


