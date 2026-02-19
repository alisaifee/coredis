from __future__ import annotations

import warnings

from .client.sentinel import Sentinel
from .connection._sentinel import SentinelManagedConnection
from .pool._sentinel import SentinelConnectionPool

warnings.warn(
    (
        "The coredis.sentinel submodule has been moved to coredis.{client,pool,connection}"
        "The Sentinel client should be imported from the top level ``coredis`` module."
    ),
    DeprecationWarning,
    stacklevel=2,
)
__all__ = ["Sentinel", "SentinelManagedConnection", "SentinelConnectionPool"]
