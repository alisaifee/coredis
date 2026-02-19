from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, cast

from anyio import (
    TASK_STATUS_IGNORED,
    CapacityLimiter,
    fail_after,
)
from anyio.abc import TaskStatus

from coredis._concurrency import Queue
from coredis.connection._base import (
    BaseConnectionParams,
    ConnectionT,
    Location,
)
from coredis.connection._tcp import TCPConnection, TCPLocation
from coredis.connection._uds import UnixDomainSocketConnection, UnixDomainSocketLocation
from coredis.exceptions import RedisError
from coredis.patterns.cache import AbstractCache, NodeTrackingCache, TrackingCache
from coredis.typing import (
    AsyncGenerator,
    Callable,
    ClassVar,
    NotRequired,
    Self,
    Unpack,
)

from ._base import BaseConnectionPool, BaseConnectionPoolParams


class ConnectionPoolParams(BaseConnectionPoolParams[ConnectionT]):
    """
    Parameters accepted by :class:`coredis.pool.ConnectionPool`
    """

    #: :meta private:
    _cache: NotRequired[AbstractCache | None]


class ConnectionPool(BaseConnectionPool[ConnectionT]):
    URL_QUERY_ARGUMENT_PARSERS: ClassVar[
        dict[str, Callable[..., int | float | bool | str | None]]
    ] = {
        **BaseConnectionPool.URL_QUERY_ARGUMENT_PARSERS,
    }

    def __init__(
        self,
        *,
        connection_class: type[ConnectionT] | None = None,
        location: Location | None = None,
        max_connections: int | None = None,
        timeout: float | None = None,
        _cache: AbstractCache | None = None,
        # host/port retained for backward compatibility
        host: str | None = None,
        port: int | None = None,
        **connection_kwargs: Unpack[BaseConnectionParams],
    ) -> None:
        """
        Blocking connection pool for single instance redis clients

        :param connection_class: The connection class to use when creating new connections
        :param max_connections: Maximum connections to grow the pool.
         Once the limit is reached clients will block to wait for a connection
         to be returned to the pool.
        :param timeout: Number of seconds to block when trying to obtain a connection.
        :param connection_kwargs: arguments to pass to the :paramref:`connection_class`
         constructor when creating a new connection
        """
        if connection_class is None:
            if isinstance(location, TCPLocation):
                connection_class = cast(type[ConnectionT], TCPConnection)
            elif isinstance(location, UnixDomainSocketLocation):
                connection_class = cast(type[ConnectionT], UnixDomainSocketConnection)
            elif host is not None and port is not None:
                connection_class = cast(type[ConnectionT], TCPConnection)
                location = TCPLocation(host, port)
        if not connection_class:
            raise RuntimeError("Unable to initialize pool without a `connection_class`")
        super().__init__(
            connection_class=connection_class,
            location=location,
            max_connections=max_connections,
            timeout=timeout,
            **connection_kwargs,
        )
        # TODO: Use the `max_failures` argument of tracking cache
        self.cache: TrackingCache[Any] | None = NodeTrackingCache(self, _cache) if _cache else None
        # The pool of available connections
        self._available_connections: Queue[ConnectionT] = Queue(self.max_connections)
        # All connections that are still active
        self._online_connections: set[ConnectionT] = set()
        # This should be used by the connection to limit concurrently entering
        # CPU hotspots to ensure all fairness between connections in the pool.
        # The main observed scenario where this can happen is if the connection pool
        # is being used by multiple push message consumers that are constantly
        # receiving data in the read task.
        self._connection_processing_budget = CapacityLimiter(1)
        self.connection_kwargs["processing_budget"] = self._connection_processing_budget

    @classmethod
    def from_url(
        cls: type[Self],
        url: str,
        *,
        decode_components: bool = False,
        **kwargs: Unpack[ConnectionPoolParams[Any]],
    ) -> Self:
        """
        Returns a connection pool configured from the given URL.

        For example:

        - ``redis://[:password]@localhost:6379/0``
        - ``rediss://[:password]@localhost:6379/0``
        - ``unix://[:password]@/path/to/socket.sock?db=0``

        Three URL schemes are supported:

        - `redis:// <http://www.iana.org/assignments/uri-schemes/prov/redis>`__
          creates a normal TCP socket connection
        - `rediss:// <http://www.iana.org/assignments/uri-schemes/prov/rediss>`__
          creates a SSL wrapped TCP socket connection
        - ``unix://`` creates a Unix Domain Socket connection

        There are several ways to specify a database number. The parse function
        will return the first specified option:

        - A ``db`` querystring option, e.g. ``redis://localhost?db=0``
        - If using the ``redis://`` scheme, the path argument of the url, e.g.
          ``redis://localhost/0``
        - The ``db`` argument to this function.

        If none of these options are specified, ``db=0`` is used.

        The :paramref:`decode_components` argument allows this function to work with
        percent-encoded URLs. If this argument is set to ``True`` all ``%xx``
        escapes will be replaced by their single-character equivalents after
        the URL has been parsed. This only applies to the ``hostname``,
        ``path``, ``username`` & ``password`` components.

        Any additional querystring arguments and keyword arguments will be
        passed along to the class constructor.

        .. note:: In the case of conflicting arguments, querystring arguments always win.
        """
        location, merged_options = cls._parse_url(
            url, decode_components, kwargs, ConnectionPoolParams
        )
        if isinstance(location, UnixDomainSocketLocation):
            merged_options["connection_class"] = UnixDomainSocketConnection
        else:
            merged_options["connection_class"] = TCPConnection
        return cls(
            location=location,
            **merged_options,
        )

    async def _initialize(self) -> None:
        if self.cache:
            # TODO: handle cache failure so that the pool doesn't die
            #  if the cache fails.
            await self._task_group.start(self.cache.run)

    async def get_connection(self, **_: Any) -> ConnectionT:
        """
        Gets or create a connection from the pool. Be careful to only release the
        connection AFTER all commands are sent, or race conditions are possible.
        """
        with fail_after(self.timeout):
            # if stack has a connection, use that
            connection = await self._available_connections.get()
            # if None, we need to create a new connection
            if connection is None or not connection.usable:
                connection = await self._construct_connection()
                if err := await self._task_group.start(self.__wrap_connection, connection):
                    raise err
                self._online_connections.add(connection)
            return connection

    @asynccontextmanager
    async def acquire(self, **_: Any) -> AsyncGenerator[ConnectionT]:
        """
        Gets or creates a connection from the pool, then release it afterwards.
        Multiplexing is automatic if you exit the context manager before
        waiting for command results.

        .. caution:: Do not explicitly release connections acquired
           using this context manager.
        """
        connection = await self.get_connection()
        yield connection
        self.release(connection)

    def release(self, connection: ConnectionT) -> None:
        """
        Checks connection for liveness and releases it back to the pool.
        """
        if connection.usable:
            self._available_connections.put_nowait(connection)

    def disconnect(self) -> None:
        """
        Disconnect all active connections in the pool
        """
        for connection in self._online_connections:
            connection.terminate()
        self._online_connections.clear()

    def _reset(self) -> None:
        # TODO: seems like something should be cleared?
        pass

    async def _construct_connection(self) -> ConnectionT:
        assert self.location
        return self.connection_class(self.location, **self.connection_kwargs)

    async def __wrap_connection(
        self,
        connection: ConnectionT,
        *,
        task_status: TaskStatus[None | Exception] = TASK_STATUS_IGNORED,
    ) -> None:
        try:
            await connection.run(task_status=task_status)
        except RedisError as error:
            # Only coredis.exception.RedisError is explictly caught and returned with the task status
            # As these are clear signals that an error case was handled by the connection
            task_status.started(error)
        finally:
            self._online_connections.discard(connection)
            if connection in self._available_connections:
                self._available_connections.remove(connection)
                self._available_connections.append_nowait(None)
