"""Combining per-partition aggregates (sum/count/mean) into one result."""

_COMBINE_SAFE = ("sum", "count", "size", "mean", "min", "max", "first",
                 "last", "any", "all")


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
