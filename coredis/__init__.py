"""
coredis
-------

coredis is an async redis client with support for redis server,
cluster & sentinel.
"""

from __future__ import annotations

from typing import cast

from coredis.client import KeyDB, KeyDBCluster, Redis, RedisCluster
from coredis.config import Config
from coredis.connection import (
    BaseConnection,
    ClusterConnection,
    Connection,
    UnixDomainSocketConnection,
)
from coredis.pool import (
    BlockingClusterConnectionPool,
    BlockingConnectionPool,
    ClusterConnectionPool,
    ConnectionPool,
)
from coredis.tokens import PureToken

from . import _version

__all__ = [
    "Config",
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
    "BlockingClusterConnectionPool",
    "ClusterConnectionPool",
    "PureToken",
]

__version__ = cast(str, _version.get_versions()["version"])  # type: ignore
