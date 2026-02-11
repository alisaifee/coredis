from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from anyio import (
    TASK_STATUS_IGNORED,
    CapacityLimiter,
    create_task_group,
    fail_after,
)
from anyio.abc import TaskStatus

from coredis._concurrency import Queue
from coredis._utils import query_param_to_bool
from coredis.connection._base import BaseConnection, BaseConnectionParams, RedisSSLContext
from coredis.connection._tcp import Connection
from coredis.connection._uds import UnixDomainSocketConnection
from coredis.exceptions import RedisError
from coredis.patterns.cache import AbstractCache, NodeTrackingCache, TrackingCache
from coredis.typing import AsyncGenerator, Callable, ClassVar, NotRequired, Self, TypeVar, Unpack

_CPT = TypeVar("_CPT", bound="ConnectionPool")


class ConnectionPool:
    class PoolParams(BaseConnectionParams):
        """
        :meta private:
        """

        connection_class: NotRequired[type[BaseConnection]]
        max_connections: NotRequired[int | None]
        timeout: NotRequired[float | None]
        _cache: NotRequired[AbstractCache | None]

    URL_QUERY_ARGUMENT_PARSERS: ClassVar[
        dict[str, Callable[..., int | float | bool | str | None]]
    ] = {
        "max_connections": int,
        "timeout": int,
        "client_name": str,
        "stream_timeout": float,
        "connect_timeout": float,
        "max_idle_time": int,
        "noreply": query_param_to_bool,
        "noevict": query_param_to_bool,
        "notouch": query_param_to_bool,
    }

    def __init__(
        self,
        *,
        connection_class: type[BaseConnection] = Connection,
        max_connections: int | None = None,
        timeout: float | None = None,
        _cache: AbstractCache | None = None,
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
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections or 64
        self.timeout = timeout
        self.decode_responses = bool(self.connection_kwargs.get("decode_responses", False))
        self.encoding = str(self.connection_kwargs.get("encoding", "utf-8"))
        # TODO: Use the `max_failures` argument of tracking cache
        self.cache: TrackingCache | None = NodeTrackingCache(self, _cache) if _cache else None
        self._connections: Queue[BaseConnection] = Queue(self.max_connections)
        # This should be used by the connection to limit concurrently entering
        # CPU hotspots to ensure all fairness between connections in the pool.
        # The main observed scenario where this can happen is if the connection pool
        # is being used by multiple push message consumers that are constantly
        # receiving data in the read task.
        self._connection_processing_budget = CapacityLimiter(1)
        self.connection_kwargs["processing_budget"] = self._connection_processing_budget
        # Rubbish hack just so that we can provide a useful __repr__ for this pool.
        self.location = self._construct_connection().location
        # reference count for context manager to support this pool being re-entered.
        self._counter = 0

    @classmethod
    def from_url(
        cls: type[_CPT],
        url: str,
        *,
        decode_components: bool = False,
        **kwargs: Unpack[PoolParams],
    ) -> _CPT:
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
        connection_class, merged_options, extra_connection_params = cls._parse_url(
            url, decode_components, kwargs
        )
        extra_connection_params.update(merged_options)
        return cls(
            connection_class=connection_class,
            **extra_connection_params,
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}<{self.location}>"

    async def __aenter__(self) -> Self:
        if self._counter == 0:
            self._task_group = create_task_group()
            self._counter += 1
            await self._task_group.__aenter__()
            if self.cache:
                # TODO: handle cache failure so that the pool doesn't die
                #  if the cache fails.
                await self._task_group.start(self.cache.run)
        else:
            self._counter += 1
        return self

    async def __aexit__(self, *args: Any) -> None:
        self._counter -= 1
        if self._counter == 0:
            self._task_group.cancel_scope.cancel()
            await self._task_group.__aexit__(*args)

    async def get_connection(self, **_: Any) -> BaseConnection:
        """
        Gets or create a connection from the pool. Be careful to only release the
        connection AFTER all commands are sent, or race conditions are possible.
        """
        with fail_after(self.timeout):
            # if stack has a connection, use that
            connection = await self._connections.get()
            # if None, we need to create a new connection
            if connection is None or not connection.usable:
                connection = self._construct_connection()
                if err := await self._task_group.start(self.__wrap_connection, connection):
                    raise err
            return connection

    @asynccontextmanager
    async def acquire(self, **_: Any) -> AsyncGenerator[BaseConnection]:
        """
        Gets or creates a connection from the pool, then release it afterwards.
        Multiplexing is automatic if you exit the context manager before
        waiting for command results. Be careful to only release the connection
        AFTER all commands are sent, or race conditions are possible.
        """
        connection = await self.get_connection()
        yield connection
        self.release(connection)

    def release(self, connection: BaseConnection) -> None:
        """
        Checks connection for liveness and releases it back to the pool.
        """
        if connection.usable:
            self._connections.put_nowait(connection)

    @classmethod
    def _parse_url(
        cls, url: str, decode_components: bool, kwargs: PoolParams
    ) -> tuple[type[BaseConnection], PoolParams, BaseConnectionParams]:
        parsed_url = urlparse(url)
        query_args = parse_qs(parsed_url.query)
        url_options = cls.PoolParams(
            **{  # type: ignore
                name: cls.URL_QUERY_ARGUMENT_PARSERS.get(name, lambda value: value)(value[0])  # type: ignore
                for name, value in query_args.items()
                if name in cls.PoolParams.__annotations__ and value
            }
        )

        username: str | None = parsed_url.username
        password: str | None = parsed_url.password
        path: str = parsed_url.path
        hostname: str | None = parsed_url.hostname

        if decode_components:
            username = unquote(username) if username else None
            password = unquote(password) if password else None
            path = unquote(path)
            hostname = unquote(hostname) if hostname else None

        # We only support redis:// and unix:// schemes.
        connection_class: type[BaseConnection] = Connection

        if username:
            url_options["username"] = username
        if password:
            url_options["password"] = password

        tcp_params = Connection.Params()
        uds_params = UnixDomainSocketConnection.Params({"path": ""})

        if parsed_url.scheme == "unix":
            connection_class = UnixDomainSocketConnection
            uds_params["path"] = path
        else:
            if hostname:
                tcp_params["host"] = hostname
            tcp_params["port"] = int(parsed_url.port or 6379)

            # If there's a path argument, use it as the db argument if a
            # querystring value wasn't specified

            if "db" not in url_options and path:
                try:
                    url_options["db"] = int(path.replace("/", ""))
                except (AttributeError, ValueError):
                    pass
            if parsed_url.scheme == "rediss" and "ssl_context" not in kwargs:
                ssl_args: dict[str, str | None] = {
                    k: value[0]
                    for k, value in query_args.items()
                    if k
                    in {
                        "ssl_keyfile",
                        "ssl_certfile",
                        "ssl_check_hostname",
                        "ssl_ca_certs",
                        "ssl_cert_reqs",
                    }
                }
                keyfile = ssl_args.get("ssl_keyfile", None)
                certfile = ssl_args.get("ssl_certfile", None)
                check_hostname = query_param_to_bool(ssl_args.get("ssl_check_hostname", None))
                cert_reqs = ssl_args.get("ssl_cert_reqs", None)
                ca_certs = ssl_args.get("ssl_ca_certs", None)
                url_options["ssl_context"] = RedisSSLContext(
                    keyfile, certfile, cert_reqs, ca_certs, check_hostname
                ).get()
            if db := url_options.get("db", kwargs.get("db", None)):
                url_options["db"] = int(db)

        return connection_class, {**kwargs, **url_options}, tcp_params or uds_params

    def _construct_connection(self) -> BaseConnection:
        return self.connection_class(**self.connection_kwargs)

    async def __wrap_connection(
        self,
        connection: BaseConnection,
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
            if connection in self._connections:
                self._connections.remove(connection)
                self._connections.append_nowait(None)
