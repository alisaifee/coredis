"""
coredis
-------

coredis is an async redis client with support for redis server,
cluster & sentinel.
"""

from __future__ import annotations

from coredis._version import __version__
from coredis.client import Redis, RedisCluster
from coredis.config import Config
from coredis.connection import (
    BaseConnection,
    ClusterConnection,
    Connection,
    UnixDomainSocketConnection,
)
from coredis.pool import ClusterConnectionPool, ConnectionPool
from coredis.sentinel import Sentinel
from coredis.tokens import PureToken

__all__ = [
    "Config",
    "Redis",
    "RedisCluster",
    "BaseConnection",
    "Connection",
    "UnixDomainSocketConnection",
    "ClusterConnection",
    "ConnectionPool",
    "ClusterConnectionPool",
    "PureToken",
    "Sentinel",
    "__version__",
]
