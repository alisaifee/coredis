from __future__ import annotations

import warnings
from contextlib import asynccontextmanager
from ssl import SSLContext, VerifyMode
from typing import Any, AsyncGenerator, Generator, cast
from urllib.parse import parse_qs, unquote, urlparse

from anyio import AsyncContextManagerMixin, Condition, create_task_group, sleep
from typing_extensions import Self

from coredis._utils import query_param_to_bool
from coredis.connection import (
    BaseConnection,
    Connection,
    RedisSSLContext,
    UnixDomainSocketConnection,
)
from coredis.exceptions import ConnectionError
from coredis.typing import Callable, ClassVar, TypeVar

_CPT = TypeVar("_CPT", bound="ConnectionPool")


class ConnectionPool(AsyncContextManagerMixin):
    """Generic connection pool"""

    #: Mapping of querystring arguments to their parser functions
    URL_QUERY_ARGUMENT_PARSERS: ClassVar[
        dict[str, Callable[..., int | float | bool | str | None]]
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

    def __init__(
        self,
        *,
        connection_class: type[BaseConnection] | None = None,
        max_connections: int | None = None,
        max_idle_time: int | None = None,
        idle_check_interval: int = 1,
        blocking: bool = True,
        **connection_kwargs: Any,
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
        self.connection_kwargs["max_idle_time"] = max_idle_time
        self.max_connections = max_connections or 64
        self.max_idle_time = max_idle_time
        self.idle_check_interval = idle_check_interval
        self.decode_responses = bool(self.connection_kwargs.get("decode_responses", False))
        self.encoding = str(self.connection_kwargs.get("encoding", "utf-8"))
        self.blocking = blocking
        self._connections: set[BaseConnection] = set()
        self._condition = Condition()

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        async with create_task_group() as tg:
            self._task_group = tg
            yield self
            self._task_group.cancel_scope.cancel()

    def __repr__(self) -> str:
        return f"{type(self).__name__}<{self.connection_class.describe(self.connection_kwargs)}>"

    def get_connection_for_pipeline(self) -> Generator[BaseConnection, None, None]:
        return (c for c in self._connections if c.available and not c.pipeline and c.pending == 0)

    def get_connection_for_pubsub(self) -> Generator[BaseConnection, None, None]:
        return (c for c in self._connections if c.available and not c.pubsub)

    def get_connection_for_blocking(self) -> Generator[BaseConnection, None, None]:
        return (
            c
            for c in self._connections
            if c.available and not c.pubsub and not c.pipeline and c.pending == 0
        )

    def get_connection(self) -> Generator[BaseConnection, None, None]:
        return (c for c in self._connections if c.available and not c.pipeline)

    async def acquire(
        self, blocking: bool = False, pipeline: bool = False, pubsub: bool = False
    ) -> BaseConnection:
        """
        Gets a connection from the pool, or creates a new one if all are busy.
        """
        if pipeline:  # if connection has a pubsub it's fine
            gen = self.get_connection_for_pipeline
        elif pubsub:  # can't have two pubsubs on one connection
            gen = self.get_connection_for_pubsub
        elif blocking:  # needs completely dedicated connection
            gen = self.get_connection_for_blocking
        else:  # normal commands
            gen = self.get_connection
        while not (connection := next(gen(), None)):
            if len(self._connections) >= self.max_connections:
                if self.blocking:  # wait for a connection to become available
                    async with self._condition:
                        await self._condition.wait()
                else:
                    raise ConnectionError("Too many connections")
            else:
                connection = self.connection_class(**self.connection_kwargs)
                await self._task_group.start(connection.run, self)
                self._connections.add(connection)
                break
        if blocking:
            # set flag until the connection becomes unblocked
            connection.blocked = True
        elif pipeline:
            # set flag until the pipeline is done
            connection.pipeline = True
        elif pubsub:
            # set flag until the pubsub is closed
            connection.pubsub = True
        else:
            # increment counter until the command is sent
            connection.pending += 1
        await sleep(0)  # checkpoint
        return connection
