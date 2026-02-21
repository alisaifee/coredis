"""
coredis
-------

coredis is an async redis client with support for redis server,
cluster & sentinel.
"""

from __future__ import annotations

from coredis._version import __version__
from coredis.client import Redis, RedisCluster, Sentinel
from coredis.config import Config
from coredis.connection import (
    BaseConnection,
    ClusterConnection,
    Connection,
    SentinelManagedConnection,
    TCPConnection,
    UnixDomainSocketConnection,
)
from coredis.pool import (
    BaseConnectionPool,
    ClusterConnectionPool,
    ConnectionPool,
    SentinelConnectionPool,
)
from coredis.tokens import PureToken

__all__ = [
    "Config",
    "Redis",
    "RedisCluster",
    "BaseConnection",
    "Connection",
    "TCPConnection",
    "UnixDomainSocketConnection",
    "BaseConnectionPool",
    "ClusterConnection",
    "ConnectionPool",
    "ClusterConnectionPool",
    "PureToken",
    "Sentinel",
    "SentinelManagedConnection",
    "SentinelConnectionPool",
    "__version__",
]
