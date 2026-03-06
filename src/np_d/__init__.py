"""
np_d — distributed numpy.

Drop-in replacement for numpy. Everything is inherited from numpy directly;
only the operations that the current P2P infrastructure can distribute
are overridden. Currently that is matrix multiplication (matmul / dot).

Usage:
    import np_d as np

    np.connect()                       # join the P2P network
    result = np.matmul(A, B)           # distributed across peers
    result = np.dot(A, B)              # same
    result = A @ B                     # ndarray.__matmul__ still uses numpy
    np.shutdown()                      # leave the network

When not connected (or no peers available), every call falls back
transparently to the real numpy implementation.
"""

# Step 1: inherit *everything* from numpy
from numpy import *  # noqa: F401,F403
import numpy as _np

# Step 2: keep a reference to the originals we are about to shadow
_original_matmul = _np.matmul
_original_dot = _np.dot

# Step 3: import our distributed backend
from .core import (
    connect,
    shutdown,
    is_connected,
    compute_distributed,
    _ensure_connected,
)  # noqa: F401

# Step 4: override matmul ------------------------------------------------


def matmul(A, B, *args, **kwargs):
    """Matrix product of two arrays — distributed when connected.

    Falls back to numpy.matmul when the network is unavailable or the
    operands are not plain 2-D matrices.
    """
    A = _np.asarray(A)
    B = _np.asarray(B)

    # only distribute 2-D @ 2-D; everything else goes to numpy
    if A.ndim == 2 and B.ndim == 2 and _ensure_connected():
        try:
            return compute_distributed(A, B)
        except Exception:
            pass

    return _original_matmul(A, B, *args, **kwargs)


# Step 5: override dot ---------------------------------------------------


def dot(A, B, out=None):
    """Dot product — distributed for 2-D matrix pairs when connected.

    Falls back to numpy.dot for everything else.
    """
    print("In d_np")
    A = _np.asarray(A)
    B = _np.asarray(B)

    if A.ndim == 2 and B.ndim == 2 and out is None and _ensure_connected():
        try:
            return compute_distributed(A, B)
        except Exception:
            pass

    return _original_dot(A, B, out=out)
