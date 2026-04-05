from __future__ import annotations

import dataclasses
import time
import weakref
from typing import TYPE_CHECKING, Any

from coredis._telemetry import get_telemetry_provider
from coredis.connection import BaseConnection

if TYPE_CHECKING:
    from ._base import BaseConnectionPool


@dataclasses.dataclass
class ConnectionPoolStatistics:
    """
    Connection pool statistics
    """

    pool: dataclasses.InitVar[BaseConnectionPool[Any]]
    created_at: float = dataclasses.field(init=False, default_factory=lambda: time.perf_counter())
    active_connections: int = 0
    in_use_connections: int = 0
    connection_used_time: weakref.WeakKeyDictionary[BaseConnection, float] = dataclasses.field(
        init=False, default_factory=weakref.WeakKeyDictionary
    )
    _connection_leases: weakref.WeakKeyDictionary[BaseConnection, float] = dataclasses.field(
        init=False, default_factory=weakref.WeakKeyDictionary
    )
    _pool: weakref.ProxyType[BaseConnectionPool[Any]] = dataclasses.field(init=False)

    def __post_init__(self, pool: BaseConnectionPool[Any]) -> None:
        self._pool = weakref.proxy(pool)

    def connection_created(self, connection: BaseConnection) -> None:
        get_telemetry_provider().observe_connection(connection)
        self.active_connections += 1

    def connection_released(self, connection: BaseConnection) -> None:
        self.in_use_connections -= 1
        if leased_at := self._connection_leases.pop(connection):
            get_telemetry_provider().capture_connection_use_time(
                time.perf_counter() - leased_at,
                self._pool,  # type: ignore
            )

    def connection_leased(self, connection: BaseConnection) -> None:
        self.in_use_connections += 1
        self._connection_leases[connection] = time.perf_counter()
