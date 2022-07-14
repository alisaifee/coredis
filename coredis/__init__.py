"""
coredis
-------

coredis is an async redis client with support for redis server,
cluster & sentinel.
"""

from __future__ import annotations

from coredis.client import Redis, RedisCluster
from coredis.connection import (
    BaseConnection,
    ClusterConnection,
    Connection,
    UnixDomainSocketConnection,
)
from coredis.constants import NodeFlag
from coredis.pool import BlockingConnectionPool, ClusterConnectionPool, ConnectionPool
from coredis.tokens import PureToken
from coredis.typing import Node

from . import _version

__all__ = [
    "Redis",
    "RedisCluster",
    "BaseConnection",
    "Connection",
    "UnixDomainSocketConnection",
    "ClusterConnection",
    "BlockingConnectionPool",
    "ConnectionPool",
    "ClusterConnectionPool",
    "Node",
    "NodeFlag",
    "PureToken",
]

# For backward compatibility
StrictRedis = Redis
StrictRedisCluster = RedisCluster


__version__ = _version.get_versions()["version"]  # type: ignore
