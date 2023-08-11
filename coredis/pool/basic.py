from __future__ import annotations

import asyncio
import os
import threading
import time
import warnings
from itertools import chain
from ssl import SSLContext, VerifyMode
from typing import Any, cast
from urllib.parse import parse_qs, unquote, urlparse

import async_timeout

from coredis.connection import (
    BaseConnection,
    Connection,
    RedisSSLContext,
    UnixDomainSocketConnection,
)
from coredis.exceptions import ConnectionError
from coredis.typing import (
    Callable,
    ClassVar,
    Dict,
    List,
    Optional,
    Set,
    StringT,
    Type,
    TypeVar,
    Union,
    ValueT,
)

FALSE_STRINGS = ("0", "F", "FALSE", "N", "NO")


def to_bool(value: Optional[StringT]) -> Optional[bool]:
    if value is None or value == "":
        return None

    if isinstance(value, str) and value.upper() in FALSE_STRINGS:
        return False

    return bool(value)


_CPT = TypeVar("_CPT", bound="ConnectionPool")


class ConnectionPool:
    """Generic connection pool"""

    #: Mapping of querystring arguments to their parser functions
    URL_QUERY_ARGUMENT_PARSERS: ClassVar[
        Dict[str, Callable[..., Optional[Union[int, float, bool, str]]]]
    ] = {
        "client_name": str,
        "stream_timeout": float,
        "connect_timeout": float,
        "max_connections": int,
        "max_idle_time": int,
        "protocol_version": int,
        "idle_check_interval": int,
        "noreply": bool,
        "noevict": bool,
        "notouch": bool,
    }

    @classmethod
    def from_url(
        cls: Type[_CPT],
        url: str,
        db: Optional[int] = None,
        decode_components: bool = False,
        **kwargs: Any,
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
        ``path``, and ``password`` components. See :attr:`URL_QUERY_ARGUMENT_PARSERS`
        for a comprehensive mapping of querystring parameters to how they are parsed.

        Any additional querystring arguments and keyword arguments will be
        passed along to the class constructor. The querystring
        arguments ``connect_timeout`` and ``stream_timeout`` if supplied
        are parsed as float values.

        .. note:: In the case of conflicting arguments, querystring arguments always win.
        """
        parsed_url = urlparse(url)
        qs = parsed_url.query

        url_options: Dict[
            str,
            Optional[Union[int, float, bool, str, Type[BaseConnection], SSLContext]],
        ] = {}
        for name, value in iter(parse_qs(qs).items()):
            if value and len(value) > 0:
                parser = cls.URL_QUERY_ARGUMENT_PARSERS.get(name)

                if parser:
                    try:
                        url_options[name] = parser(value[0])
                    except (TypeError, ValueError):
                        warnings.warn(
                            UserWarning(
                                "Invalid value for `%s` in connection URL." % name
                            )
                        )
                else:
                    url_options[name] = value[0]

        username: Optional[str] = parsed_url.username
        password: Optional[str] = parsed_url.password
        path: Optional[str] = parsed_url.path
        hostname: Optional[str] = parsed_url.hostname

        if decode_components:
            username = unquote(username) if username else None
            password = unquote(password) if password else None
            path = unquote(path) if path else None
            hostname = unquote(hostname) if hostname else None

        # We only support redis:// and unix:// schemes.

        if parsed_url.scheme == "unix":
            url_options.update(
                {
                    "username": username,
                    "password": password,
                    "path": path,
                    "connection_class": UnixDomainSocketConnection,
                }
            )

        else:
            url_options.update(
                {
                    "host": hostname,
                    "port": int(parsed_url.port or 6379),
                    "username": username,
                    "password": password,
                }
            )

            # If there's a path argument, use it as the db argument if a
            # querystring value wasn't specified

            if "db" not in url_options and path:
                try:
                    url_options["db"] = int(path.replace("/", ""))
                except (AttributeError, ValueError):
                    pass

            if parsed_url.scheme == "rediss":
                keyfile = cast(Optional[str], url_options.pop("ssl_keyfile", None))
                certfile = cast(Optional[str], url_options.pop("ssl_certfile", None))
                check_hostname = cast(
                    Optional[bool], url_options.pop("ssl_check_hostname", None)
                )
                cert_reqs = cast(
                    Optional[Union[str, VerifyMode]],
                    url_options.pop("ssl_cert_reqs", None),
                )
                ca_certs = cast(Optional[str], url_options.pop("ssl_ca_certs", None))
                url_options["ssl_context"] = RedisSSLContext(
                    keyfile, certfile, cert_reqs, ca_certs, check_hostname
                ).get()

        # last shot at the db value
        _db = url_options.get("db", db or 0)
        assert isinstance(_db, (int, str, bytes))
        url_options["db"] = int(_db)

        # update the arguments from the URL values
        kwargs.update(url_options)

        return cls(**kwargs)

    def __init__(
        self,
        *,
        connection_class: Optional[Type[Connection]] = None,
        max_connections: Optional[int] = None,
        max_idle_time: int = 0,
        idle_check_interval: int = 1,
        **connection_kwargs: Optional[Any],
    ) -> None:
        """
        Creates a connection pool. If :paramref:`max_connections` is set, then this
        object raises :class:`~coredis.ConnectionError` when the pool's limit is reached.

        By default, TCP connections are created :paramref:`connection_class` is specified.
        Use :class:`~coredis.UnixDomainSocketConnection` for unix sockets.

        Any additional keyword arguments are passed to the constructor of
        connection_class.
        """
        self.connection_class = connection_class or Connection
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections or 2**31
        self.max_idle_time = max_idle_time
        self.idle_check_interval = idle_check_interval
        self.initialized = False
        self.reset()

    async def initialize(self) -> None:
        self.initialized = True

    def __repr__(self) -> str:
        return "{}<{}>".format(
            type(self).__name__, self.connection_class.describe(self.connection_kwargs)
        )

    def __del__(self) -> None:
        self.disconnect()

    async def disconnect_on_idle_time_exceeded(self, connection: Connection) -> None:
        while True:
            if (
                time.time() - connection.last_active_at > self.max_idle_time
                and not connection.requests_pending
            ):
                connection.disconnect()
                if connection in self._available_connections:
                    self._available_connections.remove(connection)
                self._created_connections -= 1
                break
            await asyncio.sleep(self.idle_check_interval)

    def reset(self) -> None:
        self.pid = os.getpid()
        self._created_connections = 0
        self._available_connections: List[Connection] = []
        self._in_use_connections: Set[Connection] = set()
        self._check_lock = threading.Lock()

    def checkpid(self) -> None:  # noqa
        if self.pid != os.getpid():
            with self._check_lock:
                # Double check
                if self.pid == os.getpid():
                    return
                self.disconnect()
                self.reset()

    def peek_available(self) -> Optional[BaseConnection]:
        return self._available_connections[0] if self._available_connections else None

    async def get_connection(
        self,
        command_name: Optional[bytes] = None,
        *args: ValueT,
        acquire: bool = True,
        **kwargs: Optional[ValueT],
    ) -> Connection:
        """Gets a connection from the pool"""
        self.checkpid()
        try:
            connection = self._available_connections.pop()
            if connection.is_connected and connection.needs_handshake:
                await connection.perform_handshake()
        except IndexError:
            if self._created_connections >= self.max_connections:
                raise ConnectionError("Too many connections")
            connection = self._make_connection(**kwargs)

        if acquire:
            self._in_use_connections.add(connection)
        else:
            self._available_connections.append(connection)

        return connection

    def release(self, connection: Connection) -> None:
        """
        Releases the :paramref:`connection` back to the pool
        """
        self.checkpid()

        if connection.pid == self.pid:
            self._in_use_connections.remove(connection)
            self._available_connections.append(connection)

    def disconnect(self) -> None:
        """Closes all connections in the pool"""
        all_conns = chain(self._available_connections, self._in_use_connections)

        for connection in all_conns:
            connection.disconnect()
            self._created_connections -= 1

    def _make_connection(self, **options: Optional[ValueT]) -> Connection:
        """
        Creates a new connection
        """

        self._created_connections += 1
        connection = self.connection_class(
            **self.connection_kwargs,  # type: ignore
        )

        if self.max_idle_time > self.idle_check_interval > 0:
            # do not await the future
            asyncio.ensure_future(self.disconnect_on_idle_time_exceeded(connection))

        return connection


class BlockingConnectionPool(ConnectionPool):
    """
    Blocking connection pool::

        >>> from coredis import Redis
        >>> client = Redis(connection_pool=BlockingConnectionPool())

    It performs the same function as the default
    :class:`~coredis.ConnectionPool`, in that, it maintains a pool of reusable
    connections that can be shared by multiple redis clients.

    The difference is that, in the event that a client tries to get a
    connection from the pool when all of the connections are in use, rather than
    raising a :exc:`~coredis.ConnectionError` (as the default
    :class:`~coredis.ConnectionPool` implementation does), it
    makes the client blocks for a specified number of seconds until
    a connection becomes available.

    Use :paramref:`max_connections` to increase / decrease the pool size::

        >>> pool = BlockingConnectionPool(max_connections=10)

    Use :paramref:`timeout` to tell it either how many seconds to wait for a
    connection to become available, or to block forever::

        >>> # Block forever.
        >>> pool = BlockingConnectionPool(timeout=None)
        >>> # Raise a ``ConnectionError`` after five seconds if a connection is
        >>> # not available.
        >>> pool = BlockingConnectionPool(timeout=5)
    """

    def __init__(
        self,
        connection_class: Optional[Type[Connection]] = None,
        queue_class: Type[asyncio.Queue[Optional[Connection]]] = asyncio.LifoQueue,
        max_connections: Optional[int] = None,
        timeout: int = 20,
        max_idle_time: int = 0,
        idle_check_interval: int = 1,
        **connection_kwargs: Optional[ValueT],
    ):
        self.timeout = timeout
        self.queue_class = queue_class
        self.total_wait = 0
        self.total_allocated = 0
        max_connections = max_connections or 50

        super().__init__(
            connection_class=connection_class or Connection,
            max_connections=max_connections,
            max_idle_time=max_idle_time,
            idle_check_interval=idle_check_interval,
            **connection_kwargs,
        )

    async def disconnect_on_idle_time_exceeded(self, connection: Connection) -> None:
        while True:
            if time.time() - connection.last_active_at > self.max_idle_time:
                # Unlike the non blocking pool, we don't free the connection object,
                # but always reuse it
                connection.disconnect()

                break
            await asyncio.sleep(self.idle_check_interval)

    def reset(self) -> None:
        self._pool: asyncio.Queue[Optional[Connection]] = self.queue_class(
            self.max_connections
        )

        while True:
            try:
                self._pool.put_nowait(None)
            except asyncio.QueueFull:
                break

        super().reset()

    def peek_available(self) -> Optional[BaseConnection]:
        return (
            self._pool._queue[-1]  # type: ignore
            if (self._pool and not self._pool.empty())
            else None
        )

    async def get_connection(
        self,
        command_name: Optional[bytes] = None,
        *args: ValueT,
        acquire: bool = True,
        **kwargs: Optional[ValueT],
    ) -> Connection:
        """Gets a connection from the pool"""
        self.checkpid()

        try:
            async with async_timeout.timeout(self.timeout):
                connection = await self._pool.get()
            if connection and connection.is_connected and connection.needs_handshake:
                await connection.perform_handshake()
        except asyncio.TimeoutError:
            raise ConnectionError("No connection available.")
        if connection is None:
            connection = self._make_connection()

        if acquire:
            self._in_use_connections.add(connection)
        else:
            self._pool.put_nowait(connection)

        return connection

    def release(self, connection: Connection) -> None:
        """Releases the connection back to the pool"""
        _connection: Optional[Connection] = connection

        self.checkpid()

        if _connection and _connection.pid == self.pid:
            self._in_use_connections.remove(_connection)
            try:
                self._pool.put_nowait(_connection)
            except asyncio.QueueFull:
                _connection.disconnect()

    def disconnect(self) -> None:
        """Closes all connections in the pool"""
        pooled_connections: List[Optional[Connection]] = []

        while True:
            try:
                pooled_connections.append(self._pool.get_nowait())
            except asyncio.QueueEmpty:
                break
        for conn in pooled_connections:
            self._pool.put_nowait(conn)

        all_conns = chain(pooled_connections, self._in_use_connections)

        for connection in all_conns:
            if connection is not None:
                connection.disconnect()
