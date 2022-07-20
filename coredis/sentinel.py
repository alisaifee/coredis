from __future__ import annotations

import random
import ssl
import weakref
from typing import Any, cast, overload

from coredis import Redis
from coredis._utils import nativestr
from coredis.cache import AbstractCache
from coredis.connection import Connection
from coredis.exceptions import (
    ConnectionError,
    PrimaryNotFoundError,
    ReadOnlyError,
    ReplicaNotFoundError,
    ResponseError,
    SentinelConnectionError,
    TimeoutError,
)
from coredis.pool import ConnectionPool
from coredis.typing import (
    AnyStr,
    Dict,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    ResponseType,
    Set,
    StringT,
    Tuple,
    Type,
    Union,
    ValueT,
)


class SentinelManagedConnection(Connection, Generic[AnyStr]):
    def __init__(
        self,
        connection_pool: SentinelConnectionPool,
        host: str = "127.0.0.1",
        port: int = 6379,
        username: Optional[str] = None,
        password: Optional[str] = None,
        db: int = 0,
        retry_on_timeout: bool = False,
        stream_timeout: Optional[float] = None,
        connect_timeout: Optional[float] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        socket_keepalive: Optional[bool] = None,
        socket_keepalive_options: Optional[Dict[int, Union[int, bytes]]] = None,
        *,
        client_name: Optional[str] = None,
        protocol_version: Literal[2, 3] = 2,
    ):
        self.connection_pool: SentinelConnectionPool = weakref.proxy(connection_pool)
        super().__init__(
            host=host,
            port=port,
            username=username,
            password=password,
            db=db,
            retry_on_timeout=retry_on_timeout,
            stream_timeout=stream_timeout,
            connect_timeout=connect_timeout,
            ssl_context=ssl_context,
            encoding=encoding,
            decode_responses=decode_responses,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            client_name=client_name,
            protocol_version=protocol_version,
        )

    def __repr__(self) -> str:
        pool = self.connection_pool
        if self.host:
            host_info = f",host={self.host},port={self.port}"
        else:
            host_info = ""
        s = f"{type(self).__name__}<service={pool.service_name}{host_info}>"
        return s

    async def connect_to(self, address: Tuple[str, int]) -> None:
        self.host, self.port = address
        await super().connect()

    async def connect(self) -> None:
        if not self.is_connected:
            if self.connection_pool.is_primary:
                await self.connect_to(await self.connection_pool.get_primary_address())
            else:
                for replica in await self.connection_pool.rotate_replicas():
                    try:
                        return await self.connect_to(replica)
                    except ConnectionError:
                        continue
                raise ReplicaNotFoundError  # Never be here
        return None

    async def read_response(
        self,
        decode: Optional[ValueT] = None,
        push_message_types: Optional[Set[bytes]] = None,
        raise_exceptions: bool = True,
    ) -> ResponseType:
        try:
            return await super().read_response(
                decode=decode,
                push_message_types=push_message_types,
                raise_exceptions=raise_exceptions,
            )
        except ReadOnlyError:
            if self.connection_pool.is_primary:
                # When talking to a primary, a ReadOnlyError when likely
                # indicates that the previous primary that we're still connected
                # to has been demoted to a replica and there's a new primary.
                # calling disconnect will force the connection to re-query
                # sentinel during the next connect() attempt.
                self.disconnect()
                raise SentinelConnectionError("The previous primary is now a replica")
            raise


class SentinelConnectionPool(ConnectionPool):
    """
    Sentinel backed connection pool.
    """

    primary_address: Optional[Tuple[str, int]]
    replica_counter: Optional[int]

    def __init__(
        self,
        service_name: StringT,
        sentinel_manager: Sentinel[Any],
        is_primary: bool = True,
        check_connection: bool = True,
        **kwargs: Any,
    ):
        self.is_primary = is_primary
        kwargs["connection_class"] = cast(
            Type[Connection],
            kwargs.get("connection_class", SentinelManagedConnection[AnyStr]),
        )
        super().__init__(**kwargs)
        self.connection_kwargs["connection_pool"] = self
        self.service_name = nativestr(service_name)
        self.sentinel_manager = sentinel_manager
        self.check_connection = check_connection

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}"
            f"<service={self.service_name}"
            f"({'primary' if self.is_primary else 'replica'})"
        )

    def reset(self) -> None:
        super().reset()
        self.primary_address = None
        self.replica_counter = None

    async def get_primary_address(self) -> Tuple[str, int]:
        primary_address = await self.sentinel_manager.discover_primary(
            self.service_name
        )
        if self.is_primary:
            if self.primary_address is None:
                self.primary_address = primary_address
            elif primary_address != self.primary_address:
                # Primary address changed, disconnect all clients in this pool
                self.disconnect()
        return primary_address

    async def rotate_replicas(self) -> List[Tuple[str, int]]:
        """Round-robin replicas balancer"""
        replicas = await self.sentinel_manager.discover_replicas(self.service_name)
        replica_addresses: List[Tuple[str, int]] = []
        if replicas:
            if self.replica_counter is None:
                self.replica_counter = random.randint(0, len(replicas) - 1)
            for _ in range(len(replicas)):
                self.replica_counter = (self.replica_counter + 1) % len(replicas)
                replica_addresses.append(replicas[self.replica_counter])
            return replica_addresses
        # Fallback to primary
        try:
            return [await self.get_primary_address()]
        except PrimaryNotFoundError:
            pass
        raise ReplicaNotFoundError("No replica found for %r" % (self.service_name))


class Sentinel(Generic[AnyStr]):
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
        sentinels: Iterable[Tuple[str, int]],
        min_other_sentinels: int = ...,
        sentinel_kwargs: Optional[Dict[str, Any]] = ...,
        decode_responses: Literal[False] = ...,
        cache: Optional[AbstractCache] = None,
        **connection_kwargs: Any,
    ):
        ...

    @overload
    def __init__(
        self: Sentinel[str],
        sentinels: Iterable[Tuple[str, int]],
        min_other_sentinels: int = ...,
        sentinel_kwargs: Optional[Dict[str, Any]] = ...,
        decode_responses: Literal[True] = ...,
        cache: Optional[AbstractCache] = None,
        **connection_kwargs: Any,
    ):
        ...

    def __init__(
        self,
        sentinels: Iterable[Tuple[str, int]],
        min_other_sentinels: int = 0,
        sentinel_kwargs: Optional[Dict[str, Any]] = None,
        decode_responses: bool = False,
        cache: Optional[AbstractCache] = None,
        **connection_kwargs: Any,
    ):
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
         not specified, ``stream_timeout``, ``socket_keepalive``, ``decode_responses``
         and ``protocol_version`` options specified in :paramref:`connection_kwargs` will be used.
        :param cache: If provided the cache will be shared between both primaries and replicas
         returned by this sentinel.
        :param connection_kwargs: are keyword arguments that will be used when
         establishing a connection to a Redis server (i.e. are passed on to the
         constructor of :class:`Redis` for all primary and replicas).
        """
        # if sentinel_kwargs isn't defined, use the socket_* options from
        # connection_kwargs
        if not sentinel_kwargs:
            sentinel_kwargs = {
                k: v
                for k, v in iter(connection_kwargs.items())
                if k
                in {
                    "socket_timeout",
                    "socket_keepalive",
                    "encoding",
                    "protocol_version",
                }
            }

        self.sentinel_kwargs = sentinel_kwargs
        self.min_other_sentinels = min_other_sentinels
        self.connection_kwargs = connection_kwargs
        self.__cache = cache
        self.connection_kwargs["decode_responses"] = self.sentinel_kwargs[
            "decode_responses"
        ] = decode_responses

        self.sentinels = [
            Redis(hostname, port, **self.sentinel_kwargs)
            for hostname, port in sentinels
        ]

    def __repr__(self) -> str:
        sentinel_addresses: List[str] = []
        for sentinel in self.sentinels:
            sentinel_addresses.append(
                "{}:{}".format(
                    sentinel.connection_pool.connection_kwargs["host"],
                    sentinel.connection_pool.connection_kwargs["port"],
                )
            )
        return "{}<sentinels=[{}]>".format(
            type(self).__name__, ",".join(sentinel_addresses)
        )

    def __check_primary_state(
        self,
        state: Dict[str, Union[int, bool, str]],
    ) -> bool:
        if not state["is_master"] or state["is_sdown"] or state["is_odown"]:
            return False
        if int(state["num-other-sentinels"]) < self.min_other_sentinels:
            return False
        return True

    def __filter_replicas(
        self, replicas: Iterable[Dict[str, Union[str, int, bool]]]
    ) -> List[Tuple[str, int]]:
        """Removes replicas that are in an ODOWN or SDOWN state"""
        replicas_alive: List[Tuple[str, int]] = []
        for replica in replicas:
            if replica["is_odown"] or replica["is_sdown"]:
                continue
            replicas_alive.append((nativestr(replica["ip"]), int(replica["port"])))
        return replicas_alive

    async def discover_primary(self, service_name: str) -> Tuple[str, int]:
        """
        Asks sentinel servers for the Redis primary's address corresponding
        to the service labeled :paramref:`service_name`.

        :return: A pair (address, port) or raises :exc:`~coredis.exceptions.PrimaryNotFoundError`
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
                self.sentinels[0], self.sentinels[sentinel_no] = (
                    sentinel,
                    self.sentinels[0],
                )
                return nativestr(state["ip"]), int(state["port"])
        raise PrimaryNotFoundError(f"No primary found for {service_name!r}")

    async def discover_replicas(self, service_name: str) -> List[Tuple[str, int]]:
        """Returns a list of alive replicas for service :paramref:`service_name`"""
        for sentinel in self.sentinels:
            try:
                replicas = await sentinel.sentinel_replicas(service_name)
            except (ConnectionError, ResponseError, TimeoutError):
                continue
            filtered_replicas = self.__filter_replicas(replicas)
            if filtered_replicas:
                return filtered_replicas
        return []

    @overload
    def primary_for(
        self: Sentinel[bytes],
        service_name: str,
        *,
        redis_class: Type[Redis[bytes]] = ...,
        connection_pool_class: Type[SentinelConnectionPool] = ...,
        **kwargs: Any,
    ) -> Redis[bytes]:
        ...

    @overload
    def primary_for(
        self: Sentinel[str],
        service_name: str,
        *,
        redis_class: Type[Redis[str]] = ...,
        connection_pool_class: Type[SentinelConnectionPool] = ...,
        **kwargs: Any,
    ) -> Redis[str]:
        ...

    def primary_for(
        self,
        service_name: str,
        *,
        redis_class: Type[Redis[Any]] = Redis[Any],
        connection_pool_class: Type[SentinelConnectionPool] = SentinelConnectionPool,
        **kwargs: Any,
    ) -> Union[Redis[bytes], Redis[str]]:
        """
        Returns a redis client instance for the :paramref:`service_name` primary.

        A :class:`coredis.sentinel.SentinelConnectionPool` class is used to
        retrive the primary's address before establishing a new connection.

        NOTE: If the primary's address has changed, any cached connections to
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
                **connection_kwargs,
            ),
            cache=self.__cache,
        )

    @overload
    def replica_for(
        self: Sentinel[bytes],
        service_name: str,
        redis_class: Type[Redis[bytes]] = ...,
        connection_pool_class: Type[SentinelConnectionPool] = ...,
        **kwargs: Any,
    ) -> Redis[bytes]:
        ...

    @overload
    def replica_for(
        self: Sentinel[str],
        service_name: str,
        redis_class: Type[Redis[str]] = ...,
        connection_pool_class: Type[SentinelConnectionPool] = ...,
        **kwargs: Any,
    ) -> Redis[str]:
        ...

    def replica_for(
        self,
        service_name: str,
        redis_class: Type[Redis[Any]] = Redis[Any],
        connection_pool_class: Type[SentinelConnectionPool] = SentinelConnectionPool,
        **kwargs: Any,
    ) -> Union[Redis[bytes], Redis[str]]:
        """
        Returns redis client instance for the :paramref:`service_name` replica(s).

        A SentinelConnectionPool class is used to retrieve the replica's
        address before establishing a new connection.

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
                **connection_kwargs,
            ),
            cache=self.__cache,
        )
