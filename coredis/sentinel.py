from __future__ import annotations

import random
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any, overload

from anyio import AsyncContextManagerMixin

from coredis import Redis
from coredis._utils import nativestr
from coredis.connection import BaseConnectionParams, TCPConnection, TCPLocation
from coredis.exceptions import (
    ConnectionError,
    PrimaryNotFoundError,
    ReplicaNotFoundError,
    ResponseError,
)
from coredis.patterns.cache import AbstractCache
from coredis.pool import ConnectionPool
from coredis.retry import NoRetryPolicy, RetryPolicy
from coredis.typing import (
    AnyStr,
    AsyncGenerator,
    Generic,
    Iterable,
    Literal,
    ResponsePrimitive,
    Self,
    StringT,
    TypeAdapter,
    Unpack,
)


class SentinelManagedConnection(TCPConnection):
    def __init__(
        self, location: TCPLocation, primary_name: str, **kwargs: Unpack[BaseConnectionParams]
    ):
        self.primary_name = primary_name
        super().__init__(location=location, **kwargs)

    def __repr__(self) -> str:
        if self.location:
            host_info = f",host={self.location.host},port={self.location.port}"
        else:
            host_info = ""
        return f"{type(self).__name__}<service={self.primary_name}{host_info}>"


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
        return SentinelManagedConnection(location, self.service_name, **self.connection_kwargs)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}"
            f"<service={self.service_name}"
            f"({'primary' if self.is_primary else 'replica'})>"
        )

    async def get_primary_location(self) -> TCPLocation:
        primary_location = await self.sentinel_manager.discover_primary(self.service_name)
        location = TCPLocation(primary_location[0], primary_location[1])
        if self.is_primary:
            if self.location != location and self.location is not None:
                # Primary location changed, disconnect all clients in this pool
                self.disconnect()
            self.location = location

        return location

    async def get_replica(self) -> TCPLocation:
        """Round-robin replicas balancer"""
        replicas = await self.sentinel_manager.discover_replicas(self.service_name)
        if not self.replicas:
            self.replicas = [
                TCPLocation(*replica)
                for replica in await self.sentinel_manager.discover_replicas(self.service_name)
            ]

        if self.replicas:
            if self.replica_counter is None:
                self.replica_counter = random.randint(0, len(replicas) - 1)
            self.replica_counter = (self.replica_counter + 1) % len(replicas)
            return self.replicas[self.replica_counter]
        else:
            try:
                return await self.get_primary_location()
            except PrimaryNotFoundError:
                pass
            raise ReplicaNotFoundError(f"No replica found for {self.service_name!r}")


class Sentinel(AsyncContextManagerMixin, Generic[AnyStr]):
    """
    Example use::

        from coredis.sentinel import Sentinel
        sentinel = Sentinel([('localhost', 26379)], stream_timeout=0.1)
        async def test():
            primary = await sentinel.primary_for('my-instance', stream_timeout=0.1)
            await primary.set('foo', 'bar')
            replica = await sentinel.replica_for('my-instance', stream_timeout=0.1)
            await replica.get('foo')

    """

    @overload
    def __init__(
        self: Sentinel[bytes],
        sentinels: Iterable[tuple[str, int]],
        min_other_sentinels: int = ...,
        sentinel_kwargs: dict[str, Any] | None = ...,
        decode_responses: Literal[False] = ...,
        cache: AbstractCache | None = None,
        type_adapter: TypeAdapter | None = ...,
        retry_policy: RetryPolicy = ...,
        **connection_kwargs: Any,
    ) -> None: ...

    @overload
    def __init__(
        self: Sentinel[str],
        sentinels: Iterable[tuple[str, int]],
        min_other_sentinels: int = ...,
        sentinel_kwargs: dict[str, Any] | None = ...,
        decode_responses: Literal[True] = ...,
        cache: AbstractCache | None = None,
        type_adapter: TypeAdapter | None = None,
        retry_policy: RetryPolicy = ...,
        **connection_kwargs: Any,
    ) -> None: ...

    def __init__(
        self,
        sentinels: Iterable[tuple[str, int]],
        min_other_sentinels: int = 0,
        sentinel_kwargs: dict[str, Any] | None = None,
        decode_responses: bool = False,
        cache: AbstractCache | None = None,
        type_adapter: TypeAdapter | None = None,
        retry_policy: RetryPolicy = NoRetryPolicy(),
        **connection_kwargs: Any,
    ) -> None:
        """
        Changes
          - .. versionadded:: 3.10.0
               Accept :paramref:`cache` parameter to be used with primaries
               and replicas returned from the sentinel instance.

        :param sentinels: is a list of sentinel nodes. Each node is represented by
         a pair (hostname, port).
        :param min_other_sentinels: defined a minimum number of peers for a sentinel.
         When querying a sentinel, if it doesn't meet this threshold, responses
         from that sentinel won't be considered valid.
        :param sentinel_kwargs: is a dictionary of connection arguments used when
         connecting to sentinel instances. Any argument that can be passed to
         a normal Redis connection can be specified here. If :paramref:`sentinel_kwargs` is
         not specified, ``stream_timeout``, ``socket_keepalive`` and ``decode_responses``
         options specified in :paramref:`connection_kwargs` will be used.
        :param cache: If provided the cache will be shared between both primaries and replicas
         returned by this sentinel.
        :param type_adapter: The adapter to use for serializing / deserializing customs types
         when interacting with redis commands. If provided this adapter will be used for both
         primaries and replicas returned by this sentinel.
        :param retry_policy: The retry policy to use when interacting with the the primary
         and replica instances.
        :param connection_kwargs: are keyword arguments that will be used when
         establishing a connection to a Redis server (i.e. are passed on to the
         constructor of :class:`Redis` for all primary and replicas).
        """
        # if sentinel_kwargs isn't defined, use the socket_* options from
        # connection_kwargs
        if sentinel_kwargs is None:
            sentinel_kwargs = {
                k: v
                for k, v in connection_kwargs.items()
                if k
                in {
                    "connect_timeout",
                    "socket_timeout",
                    "socket_keepalive",
                    "encoding",
                }
            }
        self.sentinel_kwargs = sentinel_kwargs
        self.min_other_sentinels = min_other_sentinels
        self.connection_kwargs = connection_kwargs
        self.__cache = cache
        self.__type_adapter = type_adapter
        self.__retry_policy = retry_policy
        self.connection_kwargs["decode_responses"] = self.sentinel_kwargs["decode_responses"] = (
            decode_responses
        )
        self.sentinels = [
            Redis(hostname, port, retry_policy=self.__retry_policy, **self.sentinel_kwargs)
            for hostname, port in sentinels
        ]

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        async with AsyncExitStack() as stack:
            for sentinel in self.sentinels:
                await stack.enter_async_context(sentinel)
            yield self

    def __repr__(self) -> str:
        sentinels = [
            f"{location}"
            for sentinel in self.sentinels
            if (location := sentinel.connection_pool.location)
        ]
        return f"Sentinel<sentinels=[{','.join(sentinels)}]>"

    def __check_primary_state(
        self,
        state: dict[str, ResponsePrimitive],
    ) -> bool:
        if not state["is_master"] or state["is_sdown"] or state["is_odown"]:
            return False
        if int(state["num-other-sentinels"] or 0) < self.min_other_sentinels:
            return False
        return True

    def __filter_replicas(
        self, replicas: Iterable[dict[str, ResponsePrimitive]]
    ) -> list[tuple[str, int]]:
        """Removes replicas that are in an ODOWN or SDOWN state"""
        replicas_alive: list[tuple[str, int]] = []
        for replica in replicas:
            if replica["is_odown"] or replica["is_sdown"]:
                continue
            ip, port = replica["ip"], replica["port"]
            assert ip and port
            replicas_alive.append((nativestr(ip), int(port)))
        return replicas_alive

    async def discover_primary(self, service_name: str) -> tuple[str, int]:
        """
        Asks sentinel servers for the Redis primary's location corresponding
        to the service labeled :paramref:`service_name`.

        :return: A pair (location, port) or raises :exc:`~coredis.exceptions.PrimaryNotFoundError`
         if no primary is found.
        """
        for sentinel_no, sentinel in enumerate(self.sentinels):
            try:
                primaries = await sentinel.sentinel_masters()
            except (ConnectionError, TimeoutError):
                continue
            state = primaries.get(service_name)

            if state and self.__check_primary_state(state):
                # Put this sentinel at the top of the list
                self.sentinels[0] = sentinel
                self.sentinels[sentinel_no] = self.sentinels[0]
                return nativestr(state["ip"]), int(state["port"] or -1)
        raise PrimaryNotFoundError(f"No primary found for {service_name!r}")

    async def discover_replicas(self, service_name: str) -> list[tuple[str, int]]:
        """Returns a list of alive replicas for service :paramref:`service_name`"""
        for sentinel in self.sentinels:
            try:
                replicas = await sentinel.sentinel_replicas(service_name)
            except (ConnectionError, ResponseError, TimeoutError):
                continue
            return self.__filter_replicas(replicas)
        return []

    @overload
    def primary_for(
        self: Sentinel[bytes],
        service_name: str,
        *,
        redis_class: type[Redis[bytes]] = ...,
        connection_pool_class: type[SentinelConnectionPool] = ...,
        **kwargs: Any,
    ) -> Redis[bytes]: ...

    @overload
    def primary_for(
        self: Sentinel[str],
        service_name: str,
        *,
        redis_class: type[Redis[str]] = ...,
        connection_pool_class: type[SentinelConnectionPool] = ...,
        **kwargs: Any,
    ) -> Redis[str]: ...

    def primary_for(
        self,
        service_name: str,
        *,
        redis_class: type[Redis[Any]] = Redis[Any],
        connection_pool_class: type[SentinelConnectionPool] = SentinelConnectionPool,
        **kwargs: Any,
    ) -> Redis[bytes] | Redis[str]:
        """
        Returns a redis client instance for the :paramref:`service_name` primary.

        A :class:`coredis.sentinel.SentinelConnectionPool` class is used to
        retrive the primary's location before establishing a new connection.

        NOTE: If the primary's location has changed, any cached connections to
        the old primary are closed.

        By default clients will be a :class:`~coredis.Redis` instances.
        Specify a different class to the :paramref:`redis_class` argument if you desire
        something different.

        The :paramref:`connection_pool_class` specifies the connection pool to use.
        The :class:`~coredis.sentinel.SentinelConnectionPool` will be used by default.

        All other keyword arguments are merged with any :paramref:`Sentinel.connection_kwargs`
        passed to this class and passed to the connection pool as keyword
        arguments to be used to initialize Redis connections.
        """
        kwargs["is_primary"] = True
        connection_kwargs = dict(self.connection_kwargs)
        connection_kwargs.update(kwargs)

        return redis_class(
            connection_pool=connection_pool_class(
                service_name,
                self,
                _cache=self.__cache,
                **connection_kwargs,
            ),
            retry_policy=self.__retry_policy,
            type_adapter=self.__type_adapter,
        )

    @overload
    def replica_for(
        self: Sentinel[bytes],
        service_name: str,
        redis_class: type[Redis[bytes]] = ...,
        connection_pool_class: type[SentinelConnectionPool] = ...,
        **kwargs: Any,
    ) -> Redis[bytes]: ...

    @overload
    def replica_for(
        self: Sentinel[str],
        service_name: str,
        redis_class: type[Redis[str]] = ...,
        connection_pool_class: type[SentinelConnectionPool] = ...,
        **kwargs: Any,
    ) -> Redis[str]: ...

    def replica_for(
        self,
        service_name: str,
        redis_class: type[Redis[Any]] = Redis[Any],
        connection_pool_class: type[SentinelConnectionPool] = SentinelConnectionPool,
        **kwargs: Any,
    ) -> Redis[bytes] | Redis[str]:
        """
        Returns redis client instance for the :paramref:`service_name` replica(s).

        A SentinelConnectionPool class is used to retrieve the replica's
        location before establishing a new connection.

        By default clients will be a redis.Redis instance. Specify a
        different class to the :paramref:`redis_class` argument if you desire
        something different.

        The :paramref:`connection_pool_class` specifies the connection pool to use.
        The SentinelConnectionPool will be used by default.

        All other keyword arguments are merged with any :paramref:`Sentinel.connection_kwargs`
        passed to this class and passed to the connection pool as keyword
        arguments to be used to initialize Redis connections.
        """
        kwargs["is_primary"] = False
        connection_kwargs = dict(self.connection_kwargs)
        connection_kwargs.update(kwargs)

        return redis_class(
            connection_pool=connection_pool_class(
                service_name,
                self,
                _cache=self.__cache,
                **connection_kwargs,
            ),
            retry_policy=self.__retry_policy,
            type_adapter=self.__type_adapter,
        )
