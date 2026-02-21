from __future__ import annotations

import random
from typing import TYPE_CHECKING, Any

from coredis._utils import nativestr
from coredis.connection import BaseConnectionParams, TCPLocation
from coredis.connection._sentinel import SentinelManagedConnection
from coredis.exceptions import PrimaryNotFoundError, ReplicaNotFoundError
from coredis.patterns.cache import AbstractCache
from coredis.typing import StringT, Unpack

from ._basic import ConnectionPool

if TYPE_CHECKING:
    from coredis.client.sentinel import Sentinel


class SentinelConnectionPool(ConnectionPool[SentinelManagedConnection]):
    """
    Sentinel backed connection pool.
    """

    def __init__(
        self,
        service_name: StringT,
        sentinel_manager: Sentinel[Any],
        is_primary: bool = True,
        check_connection: bool = False,
        _cache: AbstractCache | None = None,
        **kwargs: Unpack[BaseConnectionParams],
    ):
        super().__init__(connection_class=SentinelManagedConnection, _cache=_cache, **kwargs)
        self.is_primary = is_primary
        self.service_name = nativestr(service_name)
        self.sentinel_manager = sentinel_manager
        self.check_connection = check_connection
        self.location: TCPLocation | None = None
        self.replicas: list[TCPLocation] = []
        self.replica_counter: int | None = None

    async def _construct_connection(self) -> SentinelManagedConnection:
        if self.is_primary:
            location = await self.get_primary_location()
        else:
            location = await self.get_replica()

        return SentinelManagedConnection(
            location, self.service_name, self.is_primary, **self.connection_kwargs
        )

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}"
            f"<service={self.service_name}"
            f"({'primary' if self.is_primary else 'replica'})>"
        )

    async def get_primary_location(self) -> TCPLocation:
        """
        Returns the location of the primary instance
        """
        primary_location = await self.sentinel_manager.discover_primary(self.service_name)
        location = TCPLocation(primary_location[0], primary_location[1])
        if self.is_primary:
            if self.location != location and self.location is not None:
                # Primary location changed, disconnect all connections in this pool
                self.disconnect()
            self.location = location

        return location

    async def get_replica(self) -> TCPLocation:
        """
        Returns the location of a replica using a round robin approach
        """

        if not self.replicas:
            self.replicas = [
                TCPLocation(*replica)
                for replica in await self.sentinel_manager.discover_replicas(self.service_name)
            ]

        if self.replicas:
            if self.replica_counter is None:
                self.replica_counter = random.randint(0, len(self.replicas) - 1)
            self.replica_counter = (self.replica_counter + 1) % len(self.replicas)

            return self.replicas[self.replica_counter]
        else:
            try:
                return await self.get_primary_location()
            except PrimaryNotFoundError:
                pass
            raise ReplicaNotFoundError(f"No replica found for {self.service_name!r}")
