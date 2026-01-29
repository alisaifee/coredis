from __future__ import annotations

import warnings
from contextlib import asynccontextmanager
from ssl import SSLContext, VerifyMode
from typing import Any, AsyncGenerator, cast
from urllib.parse import parse_qs, unquote, urlparse

from anyio import (
    TASK_STATUS_IGNORED,
    create_task_group,
    fail_after,
)
from anyio.abc import TaskStatus
from typing_extensions import Self

from coredis._concurrency import Queue
from coredis._utils import query_param_to_bool
from coredis.cache import AbstractCache, NodeTrackingCache, TrackingCache
from coredis.connection import (
    BaseConnection,
    Connection,
    RedisSSLContext,
    UnixDomainSocketConnection,
)
from coredis.exceptions import RedisError
from coredis.typing import Callable, ClassVar, TypeVar

_CPT = TypeVar("_CPT", bound="ConnectionPool")


class ConnectionPool:
    """
    Generic connection pool
    """

    #: Mapping of querystring arguments to their parser functions
    URL_QUERY_ARGUMENT_PARSERS: ClassVar[
        dict[str, Callable[..., int | float | bool | str | None]]
    ] = {
        "client_name": str,
        "stream_timeout": float,
        "connect_timeout": float,
        "max_connections": int,
        "max_idle_time": int,
        "noreply": bool,
        "noevict": bool,
        "notouch": bool,
    }

    @classmethod
    def from_url(
        cls: type[_CPT],
        url: str,
        db: int | None = None,
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

        url_options: dict[
            str,
            int | float | bool | str | type[BaseConnection] | SSLContext | None,
        ] = {}
        for name, value in iter(parse_qs(qs).items()):
            if value and len(value) > 0:
                parser = cls.URL_QUERY_ARGUMENT_PARSERS.get(name)

                if parser:
                    try:
                        url_options[name] = parser(value[0])
                    except (TypeError, ValueError):
                        warnings.warn(UserWarning(f"Invalid value for `{name}` in connection URL."))
                else:
                    url_options[name] = value[0]

        username: str | None = parsed_url.username
        password: str | None = parsed_url.password
        path: str | None = parsed_url.path
        hostname: str | None = parsed_url.hostname

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
                if "ssl_context" not in kwargs:
                    keyfile = cast(str | None, url_options.pop("ssl_keyfile", None))
                    certfile = cast(str | None, url_options.pop("ssl_certfile", None))
                    check_hostname = query_param_to_bool(
                        url_options.pop("ssl_check_hostname", None)
                    )
                    cert_reqs = cast(
                        str | VerifyMode | None,
                        url_options.pop("ssl_cert_reqs", None),
                    )
                    ca_certs = cast(str | None, url_options.pop("ssl_ca_certs", None))
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

    def __repr__(self) -> str:
        return f"{type(self).__name__}<{self.connection_class.describe(self.connection_kwargs)}>"

    def __init__(
        self,
        *,
        connection_class: type[BaseConnection] = Connection,
        max_connections: int | None = None,
        timeout: float | None = None,
        _cache: AbstractCache | None = None,
        **connection_kwargs: Any,
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
        self.cache: TrackingCache | None = NodeTrackingCache(_cache) if _cache else None
        self._connections: Queue[BaseConnection] = Queue(self.max_connections)
        self._counter = 0

    async def __aenter__(self) -> Self:
        if self._counter == 0:
            self._task_group = create_task_group()
            self._counter += 1
            await self._task_group.__aenter__()
            if self.cache:
                # TODO: handle cache failure so that the pool doesn't die
                #  if the cache fails.
                await self._task_group.start(self.cache.run, self)
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
            if connection is None or not connection.is_connected:
                connection = self.connection_class(**self.connection_kwargs)
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
        if connection.is_connected:
            self._connections.put_nowait(connection)

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
