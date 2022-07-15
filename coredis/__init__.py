"""
coredis
-------

coredis is an async redis client with support for redis server,
cluster & sentinel.
"""

from __future__ import annotations

from coredis.client import KeyDB, KeyDBCluster, Redis, RedisCluster
from coredis.connection import (
    BaseConnection,
    ClusterConnection,
    Connection,
    UnixDomainSocketConnection,
)
from coredis.pool import BlockingConnectionPool, ClusterConnectionPool, ConnectionPool
from coredis.tokens import PureToken

from . import _version

__all__ = [
    "KeyDB",
    "KeyDBCluster",
    "Redis",
    "RedisCluster",
    "BaseConnection",
    "Connection",
    "UnixDomainSocketConnection",
    "ClusterConnection",
    "BlockingConnectionPool",
    "ConnectionPool",
    "ClusterConnectionPool",
    "PureToken",
]

__version__ = _version.get_versions()["version"]  # type: ignore
