"""Distributed pandas: groupby/merge shuffles and the install hooks."""

from pipedpeer_shim import core
from pipedpeer_shim import transfer
from pipedpeer_shim.core import (
    _OFFLOAD_HERE, _log,
)
from pipedpeer_shim.transfer import (
    _pool_required, _pool_send, _should_spill,
)
from pipedpeer_shim.pandas_ooc import (
    _ooc_eligible,
    _partitioned_read_csv,
    _partitioned_read_parquet,
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
