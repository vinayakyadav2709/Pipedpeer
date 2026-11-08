#!/usr/bin/env python3
"""Versions of the Python stack a campaign system runs, on one line.

`VERSIONS python=<x.y.z> numpy=<v> pandas=<v> sklearn=<v> joblib=<v> torch=<v>
ray=<v> dask=<v>`, `none` for a module that is not installed. It never exits
non-zero, so it runs the same in the PipedPeer closure, in the campaign venv
and under a plain python3.
"""

import importlib
import platform
import sys



def _closure_hint():
    """Never called. pipedpeer builds its closure from the imports it can see
    in the source, and `importlib.import_module` below hides them, so the
    modules pins.sh reads would be missing from the closure."""
    import joblib  # noqa: F401
    import numpy  # noqa: F401
    import pandas  # noqa: F401
    import sklearn  # noqa: F401
    import torch  # noqa: F401


MODULES = ("numpy", "pandas", "sklearn", "joblib", "torch", "ray", "dask")


def version(name):
    try:
        module = importlib.import_module(name)
    except Exception as exc:  # noqa: BLE001
        print(f"versions.py: {name}: {exc}", file=sys.stderr)
        return "none"
    return getattr(module, "__version__", "none") or "none"


def main():
    fields = [f"python={platform.python_version()}"]
    fields.extend(f"{name}={version(name)}" for name in MODULES)
    print("VERSIONS " + " ".join(fields))


if __name__ == "__main__":
    main()
