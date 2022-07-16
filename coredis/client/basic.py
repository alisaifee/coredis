from __future__ import annotations

import asyncio
import contextlib
import contextvars
import warnings
from ssl import SSLContext
from typing import TYPE_CHECKING, Any, overload

from deprecated.sphinx import versionadded
from packaging.version import InvalidVersion, Version

from coredis._utils import nativestr
from coredis.cache import AbstractCache, SupportsClientTracking
from coredis.commands._key_spec import KeySpec
from coredis.commands.core import CoreCommands
from coredis.commands.function import Library
from coredis.commands.monitor import Monitor
from coredis.commands.pubsub import PubSub
from coredis.commands.script import Script
from coredis.commands.sentinel import SentinelCommands
from coredis.connection import (
    BaseConnection,
    RedisSSLContext,
    UnixDomainSocketConnection,
)
from coredis.exceptions import (
    ConnectionError,
    ReplicationError,
    SentinelConnectionError,
    TimeoutError,
    WatchError,
)
from coredis.pool import ConnectionPool
from coredis.response._callbacks import NoopCallback
from coredis.response.types import ScoredMember
from coredis.typing import (
    AnyStr,
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Coroutine,
    Generator,
    Generic,
    Iterator,
    KeyT,
    Literal,
    Optional,
    Parameters,
    ParamSpec,
    StringT,
    Tuple,
    Type,
    TypeVar,
    ValueT,
)

P = ParamSpec("P")
R = TypeVar("R")

if TYPE_CHECKING:
    import coredis.pipeline


ClientT = TypeVar("ClientT", bound="Client[Any]")
RedisT = TypeVar("RedisT", bound="Redis[Any]")
RedisStringT = TypeVar("RedisStringT", bound="Redis[str]")
RedisBytesT = TypeVar("RedisBytesT", bound="Redis[bytes]")


class Client(
    Generic[AnyStr],
    CoreCommands[AnyStr],
    SentinelCommands[AnyStr],
):
    cache: Optional[AbstractCache]
    connection_pool: ConnectionPool
    decode_responses: bool
    encoding: str
    protocol_version: Literal[2, 3]
    server_version: Optional[Version]

    def __init__(
        self,
        host: Optional[str] = "localhost",
        port: Optional[int] = 6379,
        db: int = 0,
        username: Optional[str] = None,
        password: Optional[str] = None,
        stream_timeout: Optional[int] = None,
        connect_timeout: Optional[int] = None,
        connection_pool: Optional[ConnectionPool] = None,
        connection_pool_cls: Type[ConnectionPool] = ConnectionPool,
        unix_socket_path: Optional[str] = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        ssl: bool = False,
        ssl_context: Optional[SSLContext] = None,
        ssl_keyfile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_cert_reqs: Optional[Literal["optional", "required", "none"]] = None,
        ssl_check_hostname: Optional[bool] = None,
        ssl_ca_certs: Optional[str] = None,
        max_connections: Optional[int] = None,
        retry_on_timeout: bool = False,
        max_idle_time: float = 0,
        idle_check_interval: float = 1,
        client_name: Optional[str] = None,
        protocol_version: Literal[2, 3] = 3,
        verify_version: bool = True,
        noreply: bool = False,
        **kwargs: Any,
    ):
        if not connection_pool:
            kwargs = {
                "db": db,
                "username": username,
                "password": password,
                "encoding": encoding,
                "stream_timeout": stream_timeout,
                "connect_timeout": connect_timeout,
                "max_connections": max_connections,
                "retry_on_timeout": retry_on_timeout,
                "decode_responses": decode_responses,
                "max_idle_time": max_idle_time,
                "idle_check_interval": idle_check_interval,
                "client_name": client_name,
                "protocol_version": protocol_version,
                "noreply": noreply,
            }
            # based on input, setup appropriate connection args

            if unix_socket_path is not None:
                kwargs.update(
                    {
                        "path": unix_socket_path,
                        "connection_class": UnixDomainSocketConnection,
                    }
                )
            else:
                # TCP specific options
                kwargs.update({"host": host, "port": port})

                if ssl_context is not None:
                    kwargs["ssl_context"] = ssl_context
                elif ssl:
                    ssl_context = RedisSSLContext(
                        ssl_keyfile,
                        ssl_certfile,
                        ssl_cert_reqs,
                        ssl_ca_certs,
                        ssl_check_hostname,
                    ).get()
                    kwargs["ssl_context"] = ssl_context
            connection_pool = connection_pool_cls(**kwargs)

        self.connection_pool = connection_pool
        self.encoding = str(connection_pool.connection_kwargs.get("encoding", encoding))
        self.decode_responses = bool(
            connection_pool.connection_kwargs.get("decode_responses", decode_responses)
        )
        connection_protocol_version = (
            connection_pool.connection_kwargs.get("protocol_version")
            or protocol_version
        )
        assert connection_protocol_version in {
            2,
            3,
        }, "Protocol version can only be one of {2,3}"
        self.protocol_version = connection_protocol_version
        self.server_version: Optional[Version] = None
        self.verify_version = verify_version
        self.__noreply = noreply
        self._noreplycontext: contextvars.ContextVar[
            Optional[bool]
        ] = contextvars.ContextVar("noreply", default=None)
        self._waitcontext: contextvars.ContextVar[
            Optional[Tuple[int, int]]
        ] = contextvars.ContextVar("wait", default=None)

    @property
    def noreply(self) -> bool:
        if not hasattr(self, "_noreplycontext"):
            return False
        if ctx := self._noreplycontext.get() is not None:
            return ctx
        return self.__noreply

    @noreply.setter
    def noreply(self, value: bool) -> None:
        if value != self.__noreply:
            self.__noreply = value
            self.connection_pool.disconnect()

    def _ensure_server_version(self, version: Optional[str]) -> None:
        if not self.verify_version:
            return
        if not version:
            return
        if not self.server_version and version:
            try:
                self.server_version = Version(nativestr(version))
            except InvalidVersion:
                warnings.warn(
                    (
                        f"Server reported an invalid version: {version}."
                        "If this is expected you can dismiss this warning by passing "
                        "verify_version=False to the client constructor"
                    ),
                    category=UserWarning,
                )
                self.verify_version = False
                self.server_version = None

    async def _ensure_wait(self, command: bytes, connection: BaseConnection) -> None:
        if wait := self._waitcontext.get():
            await connection.send_command(b"WAIT", *wait)
            if not await connection.read_response(decode=False) == wait[0]:
                raise ReplicationError(command, wait[0], wait[1])

    async def initialize(self: ClientT) -> ClientT:
        await self.connection_pool.initialize()
        return self

    def __await__(self: ClientT) -> Generator[Any, None, ClientT]:
        return self.initialize().__await__()

    def __repr__(self) -> str:
        return f"{type(self).__name__}<{repr(self.connection_pool)}>"

    async def scan_iter(
        self,
        match: Optional[StringT] = None,
        count: Optional[int] = None,
        type_: Optional[StringT] = None,
    ) -> AsyncIterator[AnyStr]:
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.
        """
        cursor = None

        while cursor != 0:
            cursor, data = await self.scan(
                cursor=cursor, match=match, count=count, type_=type_
            )

            for item in data:
                yield item

    async def sscan_iter(
        self,
        key: KeyT,
        match: Optional[StringT] = None,
        count: Optional[int] = None,
    ) -> AsyncIterator[AnyStr]:
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.
        """
        cursor = None

        while cursor != 0:
            cursor, data = await self.sscan(
                key, cursor=cursor, match=match, count=count
            )

            for item in data:
                yield item

    async def hscan_iter(
        self,
        key: KeyT,
        match: Optional[StringT] = None,
        count: Optional[int] = None,
    ) -> AsyncGenerator[Tuple[AnyStr, AnyStr], None]:
        """
        Make an iterator using the HSCAN command so that the client doesn't
        need to remember the cursor position.
        """
        cursor = None

        while cursor != 0:
            cursor, data = await self.hscan(
                key, cursor=cursor, match=match, count=count
            )

            for item in data.items():
                yield item

    async def zscan_iter(
        self,
        key: KeyT,
        match: Optional[StringT] = None,
        count: Optional[int] = None,
    ) -> AsyncIterator[ScoredMember]:
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.
        """
        cursor = None

        while cursor != 0:
            cursor, data = await self.zscan(
                key,
                cursor=cursor,
                match=match,
                count=count,
            )

            for item in data:
                yield item

    def register_script(self, script: ValueT) -> Script[AnyStr]:
        """
        Registers a Lua :paramref:`script`

        :return: A :class:`coredis.commands.script.Script` instance that is
         callable and hides the complexity of dealing with scripts, keys, and
         shas.
        """
        return Script[AnyStr](self, script)  # type: ignore

    @versionadded(version="3.1.0")
    async def register_library(
        self, name: StringT, code: StringT, replace: bool = False
    ) -> Library[AnyStr]:
        """
        Register a new library

        :param name: name of the library
        :param code: raw code for the library
        :param replace: Whether to replace the library when intializing. If ``False``
         an exception will be raised if the library was already loaded in the target
         redis instance.
        """
        return await Library[AnyStr](self, name=name, code=code, replace=replace)

    @versionadded(version="3.1.0")
    async def get_library(self, name: StringT) -> Library[AnyStr]:
        """
        Fetch a pre registered library

        :param name: name of the library
        """
        return await Library[AnyStr](self, name)

    @contextlib.contextmanager
    def ignore_replies(self: ClientT) -> Iterator[ClientT]:
        """
        Context manager to run commands without waiting for a reply.

        Example::

            client = coredis.Redis()
            with client.ignore_replies():
                assert None == await client.set("fubar", 1), "noreply"
            assert True == await client.set("fubar", 1), "reply"
        """
        self._noreplycontext.set(True)
        yield self
        self._noreplycontext.set(None)

    @contextlib.contextmanager
    def ensure_replication(
        self: ClientT, replicas: int = 1, timeout_ms: int = 100
    ) -> Iterator[ClientT]:
        """
        Context manager to ensure that commands executed within the context
        are replicated to :paramref:`replicas` within :paramref:`timeout_ms` milliseconds.

        Internally this uses `WAIT <https://redis.io/commands/wait>`_ after
        each command executed within the context

        :raises: :exc:`coredis.exceptions.ReplicationError`

        Example::

            client = coredis.RedisCluster(["localhost", 7000])
            with client.ensure_replication(1, 20):
                await client.set("fubar", 1)

        """
        self._waitcontext.set((replicas, timeout_ms))
        yield self
        self._waitcontext.set(None)


class Redis(Client[AnyStr]):
    connection_pool: ConnectionPool

    @overload
    def __init__(
        self: Redis[bytes],
        host: Optional[str] = ...,
        port: Optional[int] = ...,
        db: int = ...,
        *,
        username: Optional[str] = ...,
        password: Optional[str] = ...,
        stream_timeout: Optional[int] = ...,
        connect_timeout: Optional[int] = ...,
        connection_pool: Optional[ConnectionPool] = ...,
        unix_socket_path: Optional[str] = ...,
        encoding: str = ...,
        decode_responses: Literal[False] = ...,
        ssl: bool = ...,
        ssl_context: Optional[SSLContext] = ...,
        ssl_keyfile: Optional[str] = ...,
        ssl_certfile: Optional[str] = ...,
        ssl_cert_reqs: Optional[Literal["optional", "required", "none"]] = ...,
        ssl_check_hostname: Optional[bool] = ...,
        ssl_ca_certs: Optional[str] = ...,
        max_connections: Optional[int] = ...,
        retry_on_timeout: bool = ...,
        max_idle_time: float = ...,
        idle_check_interval: float = ...,
        client_name: Optional[str] = ...,
        protocol_version: Literal[2, 3] = ...,
        verify_version: bool = ...,
        cache: Optional[AbstractCache] = ...,
        noreply: bool = ...,
        **_: Any,
    ) -> None:
        ...

    @overload
    def __init__(
        self: Redis[str],
        host: Optional[str] = ...,
        port: Optional[int] = ...,
        db: int = ...,
        *,
        username: Optional[str] = ...,
        password: Optional[str] = ...,
        stream_timeout: Optional[int] = ...,
        connect_timeout: Optional[int] = ...,
        connection_pool: Optional[ConnectionPool] = ...,
        unix_socket_path: Optional[str] = ...,
        encoding: str = ...,
        decode_responses: Literal[True],
        ssl: bool = ...,
        ssl_context: Optional[SSLContext] = ...,
        ssl_keyfile: Optional[str] = ...,
        ssl_certfile: Optional[str] = ...,
        ssl_cert_reqs: Optional[Literal["optional", "required", "none"]] = ...,
        ssl_check_hostname: Optional[bool] = ...,
        ssl_ca_certs: Optional[str] = ...,
        max_connections: Optional[int] = ...,
        retry_on_timeout: bool = ...,
        max_idle_time: float = ...,
        idle_check_interval: float = ...,
        client_name: Optional[str] = ...,
        protocol_version: Literal[2, 3] = ...,
        verify_version: bool = ...,
        cache: Optional[AbstractCache] = ...,
        noreply: bool = ...,
        **_: Any,
    ) -> None:
        ...

    def __init__(
        self,
        host: Optional[str] = "localhost",
        port: Optional[int] = 6379,
        db: int = 0,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        stream_timeout: Optional[int] = None,
        connect_timeout: Optional[int] = None,
        connection_pool: Optional[ConnectionPool] = None,
        unix_socket_path: Optional[str] = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        ssl: bool = False,
        ssl_context: Optional[SSLContext] = None,
        ssl_keyfile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_cert_reqs: Optional[Literal["optional", "required", "none"]] = None,
        ssl_check_hostname: Optional[bool] = None,
        ssl_ca_certs: Optional[str] = None,
        max_connections: Optional[int] = None,
        retry_on_timeout: bool = False,
        max_idle_time: float = 0,
        idle_check_interval: float = 1,
        client_name: Optional[str] = None,
        protocol_version: Literal[2, 3] = 3,
        verify_version: bool = True,
        cache: Optional[AbstractCache] = None,
        noreply: bool = False,
        **_: Any,
    ) -> None:
        """
        Changes
          - .. versionchanged:: 4.0.0

            - :paramref:`non_atomic_cross_slot` defaults to ``True``
            - :paramref:`protocol_version`` defaults to ``3``
          - .. versionadded:: 3.11.0
             Added :paramref:`noreply`
          - .. versionadded:: 3.9.0
             If :paramref:`cache` is provided the client will check & populate
             the cache for read only commands and invalidate it for commands
             that could change the key(s) in the request.

          - .. versionchanged:: 3.5.0
             The :paramref:`verify_version` parameter now defaults to ``True``

          - .. versionadded:: 3.1.0
             The :paramref:`protocol_version` and :paramref:`verify_version`
             parameters were added

        :param host: The hostname of the redis server
        :param port: The port at which th redis server is listening on
        :param db: database number to switch to upon connection
        :param username: Username for authenticating with the redis server
        :param password: Password for authenticating with the redis server
        :param stream_timeout: Timeout when reading responses from the server
        :param connect_timeout: Timeout for establishing a connection to the server
        :param connection_pool: The connection pool instance to use. If not provided
         a new pool will be assigned to this client.
        :param unix_socket_path: Path to the UDS which the redis server
         is listening at
        :param encoding: The codec to use to encode strings transmitted to redis
         and decode responses with. (See :ref:`handbook/encoding:encoding/decoding`)
        :param decode_responses: If ``True`` string responses from the server
         will be decoded using :paramref:`encoding` before being returned.
         (See :ref:`handbook/encoding:encoding/decoding`)
        :param ssl: Whether to use an SSL connection
        :param ssl_context: If provided the :class:`ssl.SSLContext` will be used when
         establishing the connection. Otherwise either the default context (if no other
         ssl related parameters are provided) or a custom context based on the other
         ``ssl_*`` parameters will be used.
        :param ssl_keyfile: Path of the private key to use
        :param ssl_certfile: Path to the certificate corresponding to :paramref:`ssl_keyfile`
        :param ssl_cert_reqs: Whether to try to verify the server's certificates and
         how to behave if verification fails (See :attr:`ssl.SSLContext.verify_mode`).
        :param ssl_check_hostname: Whether to enable hostname checking when establishing
         an ssl connection.
        :param ssl_ca_certs: Path to a concatenated certificate authority file or a directory
         containing several CA certifcates to use  for validating the server's certificates
         when :paramref:`ssl_cert_reqs` is not ``"none"``
         (See :meth:`ssl.SSLContext.load_verify_locations`).
        :param max_connections: Maximum capacity of the connection pool (Ignored if
         :paramref:`connection_pool` is not ``None``.
        :param retry_on_timeout: Whether to retry a commmand once if a :exc:`TimeoutError`
         is encountered
        :param max_idle_time: Maximum number of a seconds an unused connection is cached
         before it is disconnected.
        :param idle_check_interval: Periodicity of idle checks to release idle connections.
        :param client_name: The client name to identifiy with the redis server
        :param protocol_version: Whether to use the RESP (``2``) or RESP3 (``3``)
         protocol for parsing responses from the server (Default ``3``).
         (See :ref:`handbook/response:redis response`)
        :param verify_version: Validate redis server version against the documented
         version introduced before executing a command and raises a
         :exc:`CommandNotSupportedError` error if the required version is higher than
         the reported server version
        :param cache: If provided the cache will be used to avoid requests for read only
         commands if the client has already requested the data and it hasn't been invalidated.
         The cache is responsible for any mutations to the keys that happen outside of this client
        :param noreply: If ``True`` the client will not request a response for any
         commands sent to the server.

        """
        super().__init__(
            host=host,
            port=port,
            db=db,
            username=username,
            password=password,
            stream_timeout=stream_timeout,
            connect_timeout=connect_timeout,
            connection_pool=connection_pool,
            connection_pool_cls=ConnectionPool,
            unix_socket_path=unix_socket_path,
            encoding=encoding,
            decode_responses=decode_responses,
            ssl=ssl,
            ssl_context=ssl_context,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_cert_reqs=ssl_cert_reqs,
            ssh_check_hostname=ssl_check_hostname,
            ssl_ca_certs=ssl_ca_certs,
            max_connections=max_connections,
            retry_on_timeout=retry_on_timeout,
            max_idle_time=max_idle_time,
            idle_check_interval=idle_check_interval,
            client_name=client_name,
            protocol_version=protocol_version,
            verify_version=verify_version,
            noreply=noreply,
        )
        self.cache = cache

    @classmethod
    @overload
    def from_url(
        cls: Type[RedisBytesT],
        url: str,
        db: Optional[int] = ...,
        *,
        decode_responses: Literal[False] = ...,
        protocol_version: Literal[2, 3] = ...,
        verify_version: bool = ...,
        noreply: bool = ...,
        **kwargs: Any,
    ) -> RedisBytesT:
        ...

    @classmethod
    @overload
    def from_url(
        cls: Type[RedisStringT],
        url: str,
        db: Optional[int] = ...,
        *,
        decode_responses: Literal[True],
        protocol_version: Literal[2, 3] = ...,
        verify_version: bool = ...,
        noreply: bool = ...,
        **kwargs: Any,
    ) -> RedisStringT:
        ...

    @classmethod
    def from_url(
        cls: Type[RedisT],
        url: str,
        db: Optional[int] = None,
        *,
        decode_responses: bool = False,
        protocol_version: Literal[2, 3] = 3,
        verify_version: bool = True,
        noreply: bool = False,
        **kwargs: Any,
    ) -> RedisT:
        """
        Return a Redis client object configured from the given URL, which must
        use either the `redis:// scheme
        <http://www.iana.org/assignments/uri-schemes/prov/redis>`_ for RESP
        connections or the ``unix://`` scheme for Unix domain sockets.

        For example:

        - ``redis://[:password]@localhost:6379/0``
        - ``rediss://[:password]@localhost:6379/0``
        - ``unix://[:password]@/path/to/socket.sock?db=0``

        There are several ways to specify a database number. The parse function
        will return the first specified option:

        1. A ``db`` querystring option, e.g. ``redis://localhost?db=0``
        2. If using the redis:// scheme, the path argument of the url, e.g.
           ``redis://localhost/0``
        3. The ``db`` argument to this function.

        If none of these options are specified, ``db=0`` is used.


        Any additional querystring arguments and keyword arguments will be
        passed along to the :class:`ConnectionPool` initializer. In the case
        of conflicting arguments, querystring arguments always win.
        """
        if decode_responses:

            return cls(
                decode_responses=True,
                protocol_version=protocol_version,
                verify_version=verify_version,
                noreply=noreply,
                connection_pool=ConnectionPool.from_url(
                    url,
                    db=db,
                    decode_responses=decode_responses,
                    protocol_version=protocol_version,
                    **kwargs,
                ),
            )
        else:
            return cls(
                decode_responses=False,
                protocol_version=protocol_version,
                verify_version=verify_version,
                noreply=noreply,
                connection_pool=ConnectionPool.from_url(
                    url,
                    db=db,
                    decode_responses=decode_responses,
                    protocol_version=protocol_version,
                    **kwargs,
                ),
            )

    async def initialize(self) -> Redis[AnyStr]:
        await super().initialize()
        if self.cache:
            self.cache = await self.cache.initialize(self)
        return self

    async def execute_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., R] = NoopCallback(),
        **options: Optional[ValueT],
    ) -> R:
        """Executes a command and returns a parsed response"""
        await self

        pool = self.connection_pool
        connection = await pool.get_connection(command, *args)
        if (
            self.cache
            and isinstance(self.cache, SupportsClientTracking)
            and connection.tracking_client_id != self.cache.get_client_id(connection)
        ):
            self.cache.reset()
            await connection.update_tracking_client(
                True, self.cache.get_client_id(connection)
            )
        try:
            if self.cache and command not in self.connection_pool.READONLY_COMMANDS:
                self.cache.invalidate(*KeySpec.extract_keys((command,) + args))
            await connection.send_command(command, *args)
            if self.noreply:
                return None  # type: ignore

            return callback(
                await connection.read_response(decode=options.get("decode")),
                version=self.protocol_version,
                **options,
            )
        except asyncio.CancelledError:
            # do not retry when coroutine is cancelled
            connection.disconnect()
            raise
        except (ConnectionError, TimeoutError) as e:
            connection.disconnect()
            if (
                not connection.retry_on_timeout and isinstance(e, TimeoutError)
            ) or isinstance(
                e, SentinelConnectionError
            ):  # do not retry explicit sentinel connection errors
                raise
            await connection.send_command(command, *args)

            if self.noreply:
                return None  # type: ignore

            return callback(
                await connection.read_response(decode=options.get("decode")),
                version=self.protocol_version,
                **options,
            )
        finally:
            await self._ensure_wait(command, connection)
            self._ensure_server_version(connection.server_version)
            pool.release(connection)

    def monitor(self) -> Monitor[AnyStr]:
        """
        Return an instance of a :class:`~coredis.commands.monitor.Monitor`

        The monitor can be used as an async iterator or individual commands
        can be fetched via :meth:`~coredis.commands.monitor.Monitor.get_command`.
        """
        return Monitor[AnyStr](self)

    def pubsub(
        self, ignore_subscribe_messages: bool = False, **kwargs: Any
    ) -> PubSub[AnyStr]:
        """
        Return a Pub/Sub instance that can be used to subscribe to channels
        and patterns and receive messages that get published to them.

        :param ignore_subscribe_messages: Whether to skip subscription
         acknowledgement messages
        """

        return PubSub[AnyStr](
            self.connection_pool,
            ignore_subscribe_messages=ignore_subscribe_messages,
            **kwargs,
        )

    async def pipeline(
        self,
        transaction: Optional[bool] = True,
        watches: Optional[Parameters[KeyT]] = None,
    ) -> "coredis.pipeline.Pipeline[AnyStr]":
        """
        Returns a new pipeline object that can queue multiple commands for
        later execution. :paramref:`transaction` indicates whether all commands
        should be executed atomically. Apart from making a group of operations
        atomic, pipelines are useful for reducing the back-and-forth overhead
        between the client and server.
        """
        from coredis.pipeline import Pipeline

        return Pipeline[AnyStr].proxy(self.connection_pool, transaction, watches)

    async def transaction(
        self,
        func: Callable[["coredis.pipeline.Pipeline[AnyStr]"], Coroutine[Any, Any, Any]],
        *watches: KeyT,
        **kwargs: Any,
    ) -> Optional[Any]:
        """
        Convenience method for executing the callable :paramref:`func` as a
        transaction while watching all keys specified in :paramref:`watches`.
        The :paramref:`func` callable should expect a single argument which is a
        :class:`coredis.pipeline.Pipeline` object retrieved by calling
        :meth:`~coredis.Redis.pipeline`.
        """
        value_from_callable = kwargs.pop("value_from_callable", False)
        watch_delay = kwargs.pop("watch_delay", None)
        async with await self.pipeline(True) as pipe:
            while True:
                try:
                    if watches:
                        await pipe.watch(*watches)
                    func_value = await func(pipe)
                    exec_value = await pipe.execute()
                    return func_value if value_from_callable else exec_value
                except WatchError:
                    if watch_delay is not None and watch_delay > 0:
                        await asyncio.sleep(watch_delay)
                    continue
