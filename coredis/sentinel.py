from __future__ import annotations

import os
import random
from typing import overload

from deprecated.sphinx import deprecated

from coredis import Redis
from coredis.commands import CommandName
from coredis.connection import Connection
from coredis.exceptions import (
    ConnectionError,
    PrimaryNotFoundError,
    ReadOnlyError,
    ReplicaNotFoundError,
    ResponseError,
    TimeoutError,
)
from coredis.pool import ConnectionPool
from coredis.typing import AnyStr, Generic, Iterable, Literal, StringT, Tuple, Type
from coredis.utils import nativestr


class SentinelManagedConnection(Connection):
    def __init__(self, connection_pool: SentinelConnectionPool, **kwargs):
        self.connection_pool = connection_pool
        super().__init__(**kwargs)

    def __repr__(self):
        pool = self.connection_pool
        if self.host:
            host_info = f",host={self.host},port={self.port}"
        else:
            host_info = ""
        s = f"{type(self).__name__}<service={pool.service_name}{host_info}>"
        return s

    async def connect_to(self, address):
        self.host, self.port = address
        await super().connect()
        if self.connection_pool.check_connection:
            await self.send_command(CommandName.PING)
            if await self.read_response() != b"PONG":
                raise ConnectionError("PING failed")

    async def connect(self):
        if self._reader and self._writer:
            return  # already connected
        if self.connection_pool.is_master:
            await self.connect_to(await self.connection_pool.get_master_address())
        else:
            for slave in await self.connection_pool.rotate_replicas():
                try:
                    return await self.connect_to(slave)
                except ConnectionError:
                    continue
            raise ReplicaNotFoundError  # Never be here

    async def read_response(self, decode=False):
        try:
            return await super().read_response(decode=decode)
        except ReadOnlyError:
            if self.connection_pool.is_master:
                # When talking to a master, a ReadOnlyError when likely
                # indicates that the previous master that we're still connected
                # to has been demoted to a slave and there's a new master.
                # calling disconnect will force the connection to re-query
                # sentinel during the next connect() attempt.
                self.disconnect()
                raise ConnectionError("The previous master is now a replica")
            raise


class SentinelConnectionPool(ConnectionPool):
    """
    Sentinel backed connection pool.

    If :paramref:`check_connection` is ``True``,
    :class:`~coredis.sentinel.SentinelManagedConnection`
    sends a ``PING`` command right after establishing the connection.
    """

    def __init__(
        self,
        service_name: StringT,
        sentinel_manager: Sentinel,
        is_master: bool = True,
        check_connection: bool = True,
        **kwargs,
    ):
        self.is_master = is_master
        self.check_connection = check_connection
        kwargs["connection_class"] = kwargs.get(
            "connection_class", SentinelManagedConnection
        )
        super().__init__(**kwargs)
        self.connection_kwargs["connection_pool"] = self
        self.service_name = nativestr(service_name)
        self.sentinel_manager = sentinel_manager

    def __repr__(self):
        return "{}<service={}({})".format(
            type(self).__name__,
            self.service_name,
            self.is_master and "master" or "slave",
        )

    def reset(self):
        super().reset()
        self.master_address = None
        self.slave_rr_counter = None

    async def get_master_address(self):
        master_address = await self.sentinel_manager.discover_master(self.service_name)
        if self.is_master:
            if self.master_address is None:
                self.master_address = master_address
            elif master_address != self.master_address:
                # Master address changed, disconnect all clients in this pool
                self.disconnect()
        return master_address

    async def rotate_replicas(self):
        """Round-robin replicas balancer"""
        slaves = await self.sentinel_manager.discover_slaves(self.service_name)
        slave_address = list()
        if slaves:
            if self.slave_rr_counter is None:
                self.slave_rr_counter = random.randint(0, len(slaves) - 1)
            for _ in range(len(slaves)):
                self.slave_rr_counter = (self.slave_rr_counter + 1) % len(slaves)
                slave_address.append(slaves[self.slave_rr_counter])
            return slave_address
        # Fallback to the master connection
        try:
            return [await self.get_master_address()]
        except PrimaryNotFoundError:
            pass
        raise ReplicaNotFoundError("No replica found for %r" % (self.service_name))

    def _checkpid(self):
        if self.pid != os.getpid():
            self.disconnect()
            self.reset()
            SentinelConnectionPool(
                self.service_name,
                self.sentinel_manager,
                is_master=self.is_master,
                check_connection=self.check_connection,
                connection_class=self.connection_class,
                max_connections=self.max_connections,
                **self.connection_kwargs,
            )


class Sentinel(Generic[AnyStr]):
    """
    Redis Sentinel client

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
        sentinel_kwargs=None,
        decode_responses: Literal[False] = ...,
        protocol_version: Literal[2, 3] = ...,
        **connection_kwargs,
    ):
        ...

    @overload
    def __init__(
        self: Sentinel[str],
        sentinels: Iterable[Tuple[str, int]],
        min_other_sentinels: int = ...,
        sentinel_kwargs=None,
        decode_responses: Literal[True] = ...,
        protocol_version: Literal[2, 3] = ...,
        **connection_kwargs,
    ):
        ...

    def __init__(
        self,
        sentinels: Iterable[Tuple[str, int]],
        min_other_sentinels: int = 0,
        sentinel_kwargs=None,
        decode_responses: bool = False,
        protocol_version: Literal[2, 3] = 2,
        **connection_kwargs,
    ):
        """
        :param sentinels: is a list of sentinel nodes. Each node is represented by
         a pair (hostname, port).
        :param min_other_sentinels: defined a minimum number of peers for a sentinel.
         When querying a sentinel, if it doesn't meet this threshold, responses
         from that sentinel won't be considered valid.
        :param sentinel_kwargs: is a dictionary of connection arguments used when
         connecting to sentinel instances. Any argument that can be passed to
         a normal Redis connection can be specified here. If :paramref:`sentinel_kwargs` is
         not specified, any ``stream_timeout`` and ``socket_keepalive`` options specified
         in :paramref:`connection_kwargs` will be used.
        :param connection_kwargs: are keyword arguments that will be used when
         establishing a connection to a Redis server.
        """
        # if sentinel_kwargs isn't defined, use the socket_* options from
        # connection_kwargs
        if sentinel_kwargs is None:
            sentinel_kwargs = {
                k: v
                for k, v in iter(connection_kwargs.items())
                if k.startswith("socket_")
            }

        self.sentinel_kwargs = sentinel_kwargs
        self.min_other_sentinels = min_other_sentinels
        self.connection_kwargs = connection_kwargs

        self.connection_kwargs["decode_responses"] = self.sentinel_kwargs[
            "decode_responses"
        ] = decode_responses
        self.connection_kwargs["protocol_version"] = self.sentinel_kwargs[
            "protocol_version"
        ] = protocol_version

        self.sentinels = [
            Redis(hostname, port, **self.sentinel_kwargs)
            for hostname, port in sentinels
        ]

    def __repr__(self):
        sentinel_addresses = []
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

    def check_master_state(self, state, service_name):
        if not state["is_master"] or state["is_sdown"] or state["is_odown"]:
            return False
        # Check if our sentinel doesn't see other nodes
        if state["num_other_sentinels"] < self.min_other_sentinels:
            return False
        return True

    async def discover_primary(self, service_name):
        """
        Asks sentinel servers for the Redis master's address corresponding
        to the service labeled :paramref:`service_name`.

        :return: A pair (address, port) or raises MasterNotFoundError if no
         master is found.
        """
        for sentinel_no, sentinel in enumerate(self.sentinels):
            try:
                masters = await sentinel.sentinel_masters()
            except (ConnectionError, TimeoutError):
                continue
            state = masters.get(service_name)
            if state and self.check_master_state(state, service_name):
                # Put this sentinel at the top of the list
                self.sentinels[0], self.sentinels[sentinel_no] = (
                    sentinel,
                    self.sentinels[0],
                )
                return state["ip"], state["port"]
        raise PrimaryNotFoundError(f"No primary found for {service_name!r}")

    def filter_replicas(self, replicas):
        """Removes replicas that are in an ODOWN or SDOWN state"""
        replicas_alive = []
        for replica in replicas:
            if replica["is_odown"] or replica["is_sdown"]:
                continue
            replicas_alive.append((replica["ip"], replica["port"]))
        return replicas_alive

    async def discover_replicas(self, service_name):
        """Returns a list of alive slaves for service :paramref:`service_name`"""
        for sentinel in self.sentinels:
            try:
                replicas = await sentinel.sentinel_replicas(service_name)
            except (ConnectionError, ResponseError, TimeoutError):
                continue
            replicas = self.filter_replicas(replicas)
            if replicas:
                return replicas
        return []

    def primary_for(
        self,
        service_name: str,
        redis_class=Redis,
        connection_pool_class: Type[SentinelConnectionPool] = SentinelConnectionPool,
        **kwargs,
    ) -> Redis[AnyStr]:
        """
        Returns a redis client instance for the :paramref:`service_name` master.

        A :class:`coredis.sentinel.SentinelConnectionPool` class is used to
        retrive the master's address before establishing a new connection.

        NOTE: If the master's address has changed, any cached connections to
        the old master are closed.

        By default clients will be a :class:`~coredis.Redis` instances.
        Specify a different class to the :paramref:`redis_class` argument if you desire
        something different.

        The :paramref:`connection_pool_class` specifies the connection pool to use.
        The :class:`~coredis.sentinel.SentinelConnectionPool` will be used by default.

        All other keyword arguments are merged with any :paramref:`Sentinel.connection_kwargs`
        passed to this class and passed to the connection pool as keyword
        arguments to be used to initialize Redis connections.
        """
        kwargs["is_master"] = True
        connection_kwargs = dict(self.connection_kwargs)
        connection_kwargs.update(kwargs)
        return redis_class(
            connection_pool=connection_pool_class(
                service_name, self, **connection_kwargs
            )
        )

    def replica_for(
        self,
        service_name,
        redis_class=Redis,
        connection_pool_class=SentinelConnectionPool,
        **kwargs,
    ) -> Redis[AnyStr]:
        """
        Returns redis client instance for the :paramref:`service_name` slave(s).

        A SentinelConnectionPool class is used to retrive the slave's
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
        kwargs["is_master"] = False
        connection_kwargs = dict(self.connection_kwargs)
        connection_kwargs.update(kwargs)
        return redis_class(
            connection_pool=connection_pool_class(
                service_name, self, **connection_kwargs
            )
        )

    @deprecated(version="3.1.0", reason="Use :meth:`discover_primaries()` instead")
    async def discover_master(self, *a, **k):
        return await self.discover_primary(*a, **k)

    @deprecated(version="3.1.0", reason="Use :meth:`discover_replicas()` instead")
    async def discover_slaves(self, *a, **k):
        return await self.discover_replicas(*a, **k)

    @deprecated(version="3.1.0", reason="Use :meth:`replica_for()` instead")
    def slave_for(self, *a, **kw):
        return self.replica_for(*a, **kw)

    @deprecated(version="3.1.0", reason="Use :meth:`primary_for()` instead")
    def master_for(self, *a, **kw) -> Redis[AnyStr]:
        return self.primary_for(*a, **kw)
