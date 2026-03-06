"""
Distributed computation backend.
Manages connection to the P2P network for distributed operations.
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from node.core import UnifiedNode
from config import NODE_BASE_PORT


_node = None
_loop = None
_connected = False


def _get_or_create_loop():
    """Return a usable event loop (create one if needed)."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError
        return loop
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def connect(coordinator_address=None, port=None, node_id=None):
    """Connect to the distributed network.

    Args:
        coordinator_address: Override coordinator address (default from config).
        port: Port for this client node (default: NODE_BASE_PORT + 100).
        node_id: Identifier for this client (default: 'np_d_client').
    """
    global _node, _loop, _connected

    if _connected:
        return

    _port = port if port is not None else NODE_BASE_PORT + 100
    _node_id = node_id if node_id is not None else "np_d_client"

    if coordinator_address is not None:
        os.environ["COORDINATOR_ADDRESS"] = coordinator_address

    _loop = _get_or_create_loop()
    _node = UnifiedNode(_node_id, _port, latency_simulation=0.0)
    _loop.run_until_complete(_node.start())
    _connected = True


def shutdown():
    """Disconnect from the network."""
    global _node, _loop, _connected

    if _node and _connected:
        _loop.run_until_complete(_node.shutdown())
        _connected = False
        _node = None


def is_connected():
    """Return True if connected to the distributed network."""
    return _connected


def _ensure_connected():
    """Try to connect silently; returns True if connected."""
    global _connected
    if _connected:
        return True
    try:
        connect()
        return True
    except Exception:
        _connected = False
        return False


def compute_distributed(A, B):
    """Run A @ B across the P2P network. Caller must ensure connection."""
    return _loop.run_until_complete(_node.compute_distributed(A, B))
