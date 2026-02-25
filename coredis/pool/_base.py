from __future__ import annotations

import contextvars
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from anyio import (
    Lock,
    create_task_group,
)
from anyio.abc import TaskGroup

from coredis._utils import query_param_to_bool
from coredis.connection._base import (
    BaseConnectionParams,
    ConnectionT,
    Location,
    RedisSSLContext,
)
from coredis.connection._tcp import TCPLocation
from coredis.connection._uds import UnixDomainSocketLocation
from coredis.typing import (
    AsyncGenerator,
    Callable,
    ClassVar,
    Generic,
    NotRequired,
    Self,
    TypeVar,
    Unpack,
)

BaseConnectionPoolParamsT = TypeVar(
    "BaseConnectionPoolParamsT", bound="BaseConnectionPoolParams[Any]"
)


class BaseConnectionPoolParams(BaseConnectionParams, Generic[ConnectionT]):
    """
    Connection pool parameters accepted by :class:`coredis.pool.BaseConnectionPool`
    """

    #: The connection class to use when creating new connections
    connection_class: NotRequired[type[ConnectionT]]
    #: Maximum connections to grow the pool.
    #: Once the limit is reached clients will block to wait for a connection
    #: to be returned to the pool.
    max_connections: NotRequired[int | None]
    #: Number of seconds to block when trying to obtain a connection.
    timeout: NotRequired[float | None]


class BaseConnectionPool(ABC, Generic[ConnectionT]):
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
        "db": int,
    }
    _task_group: TaskGroup

    def __init__(
        self,
        *,
        connection_class: type[ConnectionT],
        location: Location | None = None,
        max_connections: int | None = None,
        timeout: float | None = None,
        **connection_kwargs: Unpack[BaseConnectionParams],
    ) -> None:
        """
        Base (Abstract) connection pool

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
        self.location = location
        # reference count for context manager to support this pool being re-entered.
        self._counter = 0
        # context to track whether the intializing task (anchor) is active
        self._anchor_active = contextvars.ContextVar("parent_active", default=False)
        self._anchor_reset_token: contextvars.Token[bool] | None = None
        self._initialization_lock = Lock()

    def __repr__(self) -> str:
        return f"{type(self).__name__}<{self.location}>"

    async def __aenter__(self) -> Self:
        async with self._initialization_lock:
            if self._counter == 0:
                self._task_group = create_task_group()
                self._anchor_reset_token = self._anchor_active.set(True)
                self._counter += 1
                await self._task_group.__aenter__()
                await self._initialize()
            else:
                if not self._anchor_active.get():
                    raise RuntimeError(
                        "Implicit concurrent connection pool sharing detected. "
                        "You must explicitly enter the pool in a parent task "
                        "before spawning concurrent tasks that "
                        "share it. (For more details see "
                        "https://coredis.readthedocs.io/en/stable/handbook/connections.html#sharing-a-connection-pool)"
                    )
                self._counter += 1
            return self

    async def __aexit__(self, *args: Any) -> None:
        self._counter -= 1
        if self._counter == 0:
            self._task_group.cancel_scope.cancel()
            self._reset()
            if self._anchor_reset_token:
                self._anchor_active.reset(self._anchor_reset_token)
            await self._task_group.__aexit__(*args)

    @abstractmethod
    async def _initialize(self) -> None:
        """
        To be implemented by the concrete connection pool
        to perform any actions that should only be done on the first
        initialization
        """
        ...

    @abstractmethod
    def _reset(self) -> None:
        """
        To be implemented by the concrete connection pool
        to perform any finalization before the pool context is finally exited
        """
        ...

    @abstractmethod
    async def get_connection(self, **_: Any) -> ConnectionT:
        """
        Gets or create a connection from the pool.
        """
        ...

    @asynccontextmanager
    async def acquire(self, **_: Any) -> AsyncGenerator[ConnectionT]:
        """
        Yields a connection from the pool and releases it back.

        .. caution:: Do not explicitly release connections acquired
           using this context manager.
        """
        connection = await self.get_connection()
        yield connection
        self.release(connection)

    @abstractmethod
    def release(self, connection: ConnectionT) -> None:
        """
        Release a connection back  to the pool.
        """
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """
        Disconnect all active connections in the pool
        """
        ...

    @classmethod
    def _parse_url(
        cls: type[BaseConnectionPool[ConnectionT]],
        url: str,
        decode_components: bool,
        kwargs: BaseConnectionPoolParamsT,
        pool_param_class: type[BaseConnectionPoolParamsT],
    ) -> tuple[Location | None, BaseConnectionPoolParamsT]:
        parsed_url = urlparse(url)
        query_args = parse_qs(parsed_url.query)
        url_options = pool_param_class(
            **{
                name: cls.URL_QUERY_ARGUMENT_PARSERS.get(name, lambda value: value)(value[0])  # type: ignore
                for name, value in query_args.items()
                if name in pool_param_class.__annotations__ and value
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

        if username:
            url_options["username"] = username
        if password:
            url_options["password"] = password

        location: Location | None = None

        if parsed_url.scheme == "unix":
            location = UnixDomainSocketLocation(path=path)
        else:
            if hostname is not None:
                location = TCPLocation(host=hostname, port=int(parsed_url.port or 6379))
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
            if not url_options.get("db") and "db" in kwargs:
                url_options["db"] = kwargs["db"]
        kwargs.update(url_options)
        return location, kwargs
