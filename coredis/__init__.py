"""
coredis
-------

coredis is an async redis client with support for redis server,
cluster & sentinel.
"""

from __future__ import annotations

import logging
from typing import cast

from coredis.client import Redis, RedisCluster
from coredis.config import Config
from coredis.connection import (
    BaseConnection,
    ClusterConnection,
    Connection,
    UnixDomainSocketConnection,
)
from coredis.pool import (
    BlockingClusterConnectionPool,
    ClusterConnectionPool,
    ConnectionPool,
)
from coredis.tokens import PureToken

from . import _version

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__all__ = [
    "Config",
    "Redis",
    "RedisCluster",
    "BaseConnection",
    "Connection",
    "UnixDomainSocketConnection",
    "ClusterConnection",
    "ConnectionPool",
    "BlockingClusterConnectionPool",
    "ClusterConnectionPool",
    "PureToken",
]

__version__ = cast(str, _version.get_versions()["version"])  # type: ignore
