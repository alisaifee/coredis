from __future__ import annotations

import asyncio
import functools
import inspect
import textwrap
from abc import ABCMeta
from ssl import SSLContext
from typing import TYPE_CHECKING, Any, cast, overload

from deprecated.sphinx import versionadded
from packaging.version import Version

from coredis._utils import b, clusterdown_wrapper, first_key, nativestr
from coredis.commands._key_spec import KeySpec
from coredis.commands.constants import CommandName
from coredis.commands.core import CoreCommands
from coredis.commands.function import Library
from coredis.commands.monitor import Monitor
from coredis.commands.pubsub import ClusterPubSub, PubSub, ShardedPubSub
from coredis.commands.script import Script
from coredis.commands.sentinel import SentinelCommands
from coredis.connection import Connection, RedisSSLContext, UnixDomainSocketConnection
from coredis.exceptions import (
    AskError,
    BusyLoadingError,
    ClusterDownError,
    ClusterError,
    ClusterRoutingError,
    ConnectionError,
    MovedError,
    RedisClusterException,
    ResponseError,
    TimeoutError,
    TryAgainError,
    WatchError,
)
from coredis.lock import Lock, LuaLock
from coredis.nodemanager import Node, NodeFlag
from coredis.pool import ClusterConnectionPool, ConnectionPool
from coredis.response._callbacks import NoopCallback
from coredis.response.types import ScoredMember
from coredis.typing import (
    AnyStr,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    Generator,
    Generic,
    Iterable,
    Iterator,
    KeyT,
    Literal,
    Optional,
    Parameters,
    ParamSpec,
    ResponseType,
    StringT,
    Tuple,
    Type,
    TypeVar,
    ValueT,
    add_runtime_checks,
)

P = ParamSpec("P")
R = TypeVar("R")

if TYPE_CHECKING:
    import coredis.pipeline


class ClusterMeta(ABCMeta):
    ROUTING_FLAGS: Dict[bytes, NodeFlag]
    SPLIT_FLAGS: Dict[bytes, NodeFlag]
    RESULT_CALLBACKS: Dict[str, Callable[..., ResponseType]]
    NODE_FLAG_DOC_MAPPING = {
        NodeFlag.PRIMARIES: "all primaries",
        NodeFlag.REPLICAS: "all replicas",
        NodeFlag.RANDOM: "a random node",
        NodeFlag.ALL: "all nodes",
        NodeFlag.SLOT_ID: "a node selected by :paramref:`slot`",
    }

    def __new__(
        cls, name: str, bases: Tuple[type, ...], namespace: Dict[str, object]
    ) -> ClusterMeta:
        kls = super().__new__(cls, name, bases, namespace)
        methods = dict(k for k in inspect.getmembers(kls) if inspect.isfunction(k[1]))

        for name, method in methods.items():
            doc_addition = ""
            if cmd := getattr(method, "__coredis_command", None):
                if cmd.cluster.route:
                    kls.ROUTING_FLAGS[cmd.command] = cmd.cluster.route
                    aggregate_note = ""
                    if cmd.cluster.multi_node:
                        if cmd.cluster.combine:
                            aggregate_note = "and the results will be aggregated"
                        else:
                            aggregate_note = (
                                "and a mapping of nodes to results will be returned"
                            )
                    doc_addition = f"""
.. admonition:: Cluster note

   The command will be run on **{cls.NODE_FLAG_DOC_MAPPING[cmd.cluster.route]}** {aggregate_note}
                    """
                elif cmd.cluster.split and cmd.cluster.combine:
                    kls.SPLIT_FLAGS[cmd.command] = cmd.cluster.split
                    doc_addition = f"""
.. admonition:: Cluster note

   If :paramref:`RedisCluster.non_atomic_cross_slot` is set the
   command will be run on **{cls.NODE_FLAG_DOC_MAPPING[cmd.cluster.split]}**
   by distributing the keys to the appropriate nodes and the results aggregated
                """
                if cmd.cluster.multi_node:
                    kls.RESULT_CALLBACKS[cmd.command] = cmd.cluster.combine
                if cmd.readonly:
                    ConnectionPool.READONLY_COMMANDS.add(cmd.command)
                if (wrapped := add_runtime_checks(method)) != method:
                    setattr(kls, name, wrapped)
                    method = wrapped
            if doc_addition:

                def __w(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
                    @functools.wraps(func)
                    async def _w(*a: P.args, **k: P.kwargs) -> R:
                        return await func(*a, **k)

                    _w.__doc__ = f"""{textwrap.dedent(method.__doc__ or "")}
{doc_addition}
                """
                    return _w

                setattr(kls, name, __w(method))
        return kls


class RedisMeta(ABCMeta):
    def __new__(
        cls, name: str, bases: Tuple[type, ...], namespace: Dict[str, object]
    ) -> RedisMeta:
        kls = super().__new__(cls, name, bases, namespace)
        methods = dict(k for k in inspect.getmembers(kls) if inspect.isfunction(k[1]))

        for name, method in methods.items():
            if hasattr(method, "__coredis_command"):
                if (wrapped := add_runtime_checks(method)) != method:
                    setattr(kls, name, wrapped)
        return kls


class RedisConnection:
    encoding: str
    decode_responses: bool
    connection_pool: ConnectionPool
    protocol_version: Literal[2, 3]

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
        unix_socket_path: Optional[str] = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        ssl: bool = False,
        ssl_context: Optional[SSLContext] = None,
        ssl_keyfile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_cert_reqs: Optional[str] = None,
        ssl_ca_certs: Optional[str] = None,
        max_connections: Optional[int] = None,
        retry_on_timeout: bool = False,
        max_idle_time: float = 0,
        idle_check_interval: float = 1,
        client_name: Optional[str] = None,
        protocol_version: Literal[2, 3] = 2,
        verify_version: bool = True,
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
                        ssl_keyfile, ssl_certfile, ssl_cert_reqs, ssl_ca_certs
                    ).get()
                    kwargs["ssl_context"] = ssl_context
            connection_pool = ConnectionPool(**kwargs)

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

    async def parse_response(
        self,
        connection: Connection,
        *,
        decode: Optional[ValueT] = None,
        **_: Any,
    ) -> ResponseType:
        """
        Parses a response from the Redis server

        :meta private:
        """
        return await connection.read_response(
            decode=decode if decode is None else bool(decode)
        )

    async def initialize(self) -> RedisConnection:
        await self.connection_pool.initialize()
        return self

    def __await__(self) -> Generator[Any, None, RedisConnection]:
        return self.initialize().__await__()

    def __repr__(self) -> str:
        return f"{type(self).__name__}<{repr(self.connection_pool)}>"

    def ensure_server_version(self, version: Optional[str]) -> None:
        if not self.verify_version:
            return
        if not version:
            return
        if not self.server_version and version:
            self.server_version = Version(nativestr(version))
        elif str(self.server_version) != nativestr(version):
            raise Exception(
                f"Server version changed from {self.server_version} to {version}"
            )


class AbstractRedis(
    Generic[AnyStr],
    CoreCommands[AnyStr],
    SentinelCommands[AnyStr],
):
    """
    Async Redis client
    """

    server_version: Optional[Version]

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


RedisT = TypeVar("RedisT", bound="Redis[Any]")
RedisStringT = TypeVar("RedisStringT", bound="Redis[str]")
RedisBytesT = TypeVar("RedisBytesT", bound="Redis[bytes]")
RedisClusterT = TypeVar("RedisClusterT", bound="RedisCluster[Any]")
RedisClusterStringT = TypeVar("RedisClusterStringT", bound="RedisCluster[str]")
RedisClusterBytesT = TypeVar("RedisClusterBytesT", bound="RedisCluster[bytes]")


class Redis(
    AbstractRedis[AnyStr],
    Generic[AnyStr],
    RedisConnection,
    metaclass=RedisMeta,
):
    """
    Redis client
    """

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
        ssl_cert_reqs: Optional[str] = ...,
        ssl_ca_certs: Optional[str] = ...,
        max_connections: Optional[int] = ...,
        retry_on_timeout: bool = ...,
        max_idle_time: float = ...,
        idle_check_interval: float = ...,
        client_name: Optional[str] = ...,
        protocol_version: Literal[2, 3] = ...,
        verify_version: bool = ...,
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
        ssl_cert_reqs: Optional[str] = ...,
        ssl_ca_certs: Optional[str] = ...,
        max_connections: Optional[int] = ...,
        retry_on_timeout: bool = ...,
        max_idle_time: float = ...,
        idle_check_interval: float = ...,
        client_name: Optional[str] = ...,
        protocol_version: Literal[2, 3] = ...,
        verify_version: bool = ...,
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
        ssl_cert_reqs: Optional[str] = None,
        ssl_ca_certs: Optional[str] = None,
        max_connections: Optional[int] = None,
        retry_on_timeout: bool = False,
        max_idle_time: float = 0,
        idle_check_interval: float = 1,
        client_name: Optional[str] = None,
        protocol_version: Literal[2, 3] = 2,
        verify_version: bool = True,
        **_: Any,
    ) -> None:
        """
        .. versionchanged:: 3.5.0
           The :paramref:`verify_version` parameter now defaults to ``True``

        .. versionadded:: 3.1.0
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
        :param max_connections: Maximum capacity of the connection pool (Ignored if
         :paramref:`connection_pool` is not ``None``.
        :param retry_on_timeout: Whether to retry a commmand once if a :exc:`TimeoutError`
         is encountered
        :param max_idle_time: Maximum number of a seconds an unused connection is cached
         before it is disconnected.
        :param idle_check_interval: Periodicity of idle checks to release idle connections.
        :param client_name: The client name to identifiy with the redis server
        :param protocol_version: Whether to use the RESP (``2``) or RESP3 (``3``)
         protocol for parsing responses from the server (Default ``2``).
         (See :ref:`handbook/response:parsers`)
        :param verify_version: Validate redis server version against the documented
         version introduced before executing a command and raises a
         :exc:`CommandNotSupportedError` error if the required version is higher than
         the reported server version
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
            unix_socket_path=unix_socket_path,
            encoding=encoding,
            decode_responses=decode_responses,
            ssl=ssl,
            ssl_context=ssl_context,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_cert_reqs=ssl_cert_reqs,
            ssl_ca_certs=ssl_ca_certs,
            max_connections=max_connections,
            retry_on_timeout=retry_on_timeout,
            max_idle_time=max_idle_time,
            idle_check_interval=idle_check_interval,
            client_name=client_name,
            protocol_version=protocol_version,
            verify_version=verify_version,
        )
        self._use_lua_lock: Optional[bool] = None
        self.response_callbacks: Dict[bytes, Callable[..., Any]] = {}

    @classmethod
    @overload
    def from_url(
        cls: Type[RedisBytesT],
        url: str,
        db: Optional[int] = ...,
        *,
        decode_responses: Literal[False] = ...,
        protocol_version: Literal[2, 3] = ...,
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
        protocol_version: Literal[2, 3] = 2,
        **kwargs: Any,
    ) -> RedisT:
        """
        Return a Redis client object configured from the given URL, which must
        use either the `redis:// scheme
        <http://www.iana.org/assignments/uri-schemes/prov/redis>`_ for RESP
        connections or the ``unix://`` scheme for Unix domain sockets.

        For example:

        - ``redis://[:password]@localhost:6379/0``
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
                connection_pool=ConnectionPool.from_url(
                    url,
                    db=db,
                    decode_responses=decode_responses,
                    protocol_version=protocol_version,
                    **kwargs,
                ),
            )

    def set_response_callback(
        self, command: StringT, callback: Callable[..., Any]
    ) -> None:
        """
        Sets a custom Response Callback

        :meta private:
        """
        self.response_callbacks[b(command)] = callback

    async def execute_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., R] = NoopCallback(),
        **options: Optional[ValueT],
    ) -> R:
        """Executes a command and returns a parsed response"""
        pool = self.connection_pool
        connection = await pool.get_connection()

        try:
            await connection.send_command(command, *args)
            if custom_callback := self.response_callbacks.get(command):
                return custom_callback(  # type: ignore
                    await self.parse_response(connection, decode=options.get("decode")),
                    version=self.protocol_version,
                    **options,
                )

            return callback(
                await self.parse_response(connection, decode=options.get("decode")),
                version=self.protocol_version,
                **options,
            )
        except asyncio.CancelledError:
            # do not retry when coroutine is cancelled
            connection.disconnect()
            raise
        except (ConnectionError, TimeoutError) as e:
            connection.disconnect()

            if not connection.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            await connection.send_command(command, *args)

            return callback(
                await self.parse_response(connection, decode=options.get("decode")),
                version=self.protocol_version,
                **options,
            )
        finally:
            self.ensure_server_version(connection.server_version)
            pool.release(connection)

    def monitor(self) -> Monitor[AnyStr]:
        """
        Return an instance of a :class:`~coredis.commands.monitor.Monitor`

        The monitor can be used as an async iterator or individual commands
        can be fetched via :meth:`~coredis.commands.monitor.Monitor.get_command`.
        """
        return Monitor[AnyStr](self)

    def lock(
        self,
        name: StringT,
        timeout: Optional[float] = None,
        sleep: float = 0.1,
        blocking_timeout: Optional[bool] = None,
        lock_class: Optional[Type[Lock[AnyStr]]] = None,
        thread_local: bool = True,
    ) -> Lock[AnyStr]:
        """
        Return a new :class:`~coredis.lock.Lock` object using :paramref:`name` that mimics
        the behavior of :class:`threading.Lock`.

        See: :class:`~coredis.lock.LuaLock` (the default :paramref:`lock_class`)
        for more details.

        :raises: :exc:`~coredis.LockError`
        """

        if lock_class is None:
            if self._use_lua_lock is None:
                # the first time .lock() is called, determine if we can use
                # Lua by attempting to register the necessary scripts
                try:
                    LuaLock[AnyStr].register_scripts(self)
                    self._use_lua_lock = True
                except ResponseError:
                    self._use_lua_lock = False
            lock_class = self._use_lua_lock and LuaLock[AnyStr] or Lock[AnyStr]

        return lock_class(
            self,
            name,
            timeout=timeout,
            sleep=sleep,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
        )

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

        pipeline: Pipeline[AnyStr] = Pipeline[AnyStr].proxy(
            self.connection_pool, self.response_callbacks, transaction
        )
        await pipeline.reset()
        return pipeline

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


class RedisCluster(
    AbstractRedis[AnyStr],
    RedisConnection,
    metaclass=ClusterMeta,
):
    """
    Redis cluster client
    """

    RedisClusterRequestTTL = 16
    ROUTING_FLAGS: Dict[bytes, NodeFlag] = {}
    SPLIT_FLAGS: Dict[bytes, NodeFlag] = {}
    RESULT_CALLBACKS: Dict[bytes, Callable[..., Any]] = {}

    connection_pool: ClusterConnectionPool

    @overload
    def __init__(
        self: RedisCluster[bytes],
        host: Optional[str] = ...,
        port: Optional[int] = ...,
        *,
        startup_nodes: Optional[Iterable[Node]] = ...,
        max_connections: int = ...,
        max_connections_per_node: bool = ...,
        readonly: bool = ...,
        reinitialize_steps: Optional[int] = ...,
        skip_full_coverage_check: bool = ...,
        nodemanager_follow_cluster: bool = ...,
        decode_responses: Literal[False] = ...,
        connection_pool: Optional[ClusterConnectionPool] = ...,
        protocol_version: Literal[2, 3] = ...,
        verify_version: bool = ...,
        non_atomic_cross_slot: bool = ...,
        **kwargs: Any,
    ):
        ...

    @overload
    def __init__(
        self: RedisCluster[str],
        host: Optional[str] = ...,
        port: Optional[int] = ...,
        *,
        startup_nodes: Optional[Iterable[Node]] = ...,
        max_connections: int = ...,
        max_connections_per_node: bool = ...,
        readonly: bool = ...,
        reinitialize_steps: Optional[int] = ...,
        skip_full_coverage_check: bool = ...,
        nodemanager_follow_cluster: bool = ...,
        decode_responses: Literal[True],
        connection_pool: Optional[ClusterConnectionPool] = ...,
        protocol_version: Literal[2, 3] = ...,
        verify_version: bool = ...,
        non_atomic_cross_slot: bool = ...,
        **kwargs: Any,
    ):
        ...

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        *,
        startup_nodes: Optional[Iterable[Node]] = None,
        max_connections: int = 32,
        max_connections_per_node: bool = False,
        readonly: bool = False,
        reinitialize_steps: Optional[int] = None,
        skip_full_coverage_check: bool = False,
        nodemanager_follow_cluster: bool = False,
        decode_responses: bool = False,
        connection_pool: Optional[ClusterConnectionPool] = None,
        protocol_version: Literal[2, 3] = 2,
        verify_version: bool = True,
        non_atomic_cross_slot: bool = False,
        **kwargs: Any,
    ):
        """

        .. versionadded:: 3.6.0
           The :paramref:`non_atomic_cross_slot` parameter was added
        .. versionchanged:: 3.5.0
           The :paramref:`verify_version` parameter now defaults to ``True``

        .. versionadded:: 3.1.0
           The :paramref:`protocol_version` and :paramref:`verify_version`
           parameters were added

        :param host: Can be used to point to a startup node
        :param port: Can be used to point to a startup node
        :param startup_nodes: List of nodes that initial bootstrapping can be done
         from
        :param max_connections: Maximum number of connections that should be kept open at one time
        :param max_connections_per_node:
        :param readonly: enable READONLY mode. You can read possibly stale data from slave.
        :param reinitialize_steps:
        :param skip_full_coverage_check: Skips the check of cluster-require-full-coverage config,
         useful for clusters without the CONFIG command (like aws)
        :param nodemanager_follow_cluster: The node manager will during initialization try the
         last set of nodes that it was operating on. This will allow the client to drift along
         side the cluster if the cluster nodes move around alot.
        :param decode_responses: If ``True`` string responses from the server
         will be decoded using :paramref:`encoding` before being returned.
         (See :ref:`handbook/encoding:encoding/decoding`)
        :param connection_pool: The connection pool instance to use. If not provided
         a new pool will be assigned to this client.
        :param protocol_version: Whether to use the RESP (``2``) or RESP3 (``3``)
         protocol for parsing responses from the server (Default ``2``).
         (See :ref:`handbook/response:parsers`)
        :param verify_version: Validate redis server version against the documented
         version introduced before executing a command and raises a
         :exc:`CommandNotSupportedError` error if the required version is higher than
         the reported server version
        :param non_atomic_cross_slot: If ``True`` certain commands that can operate
         on multiple keys (cross slot) will be split across the relevant nodes by
         mapping the keys to the appropriate slot and the result merged before being
         returned.
        """

        if "db" in kwargs:  # noqa
            raise RedisClusterException(
                "Argument 'db' is not possible to use in cluster mode"
            )

        if connection_pool:
            pool = connection_pool
        else:
            startup_nodes = [] if startup_nodes is None else list(startup_nodes)

            # Support host/port as argument

            if host:
                startup_nodes.append(
                    Node(
                        host=host,
                        port=port if port else 7000,
                        name="",
                        server_type=None,
                        node_id=None,
                    )
                )
            pool = ClusterConnectionPool(
                startup_nodes=startup_nodes,
                max_connections=max_connections,
                reinitialize_steps=reinitialize_steps,
                max_connections_per_node=max_connections_per_node,
                skip_full_coverage_check=skip_full_coverage_check,
                nodemanager_follow_cluster=nodemanager_follow_cluster,
                readonly=readonly,
                decode_responses=decode_responses,
                protocol_version=protocol_version,
                **kwargs,
            )

        super().__init__(
            connection_pool=pool,
            decode_responses=decode_responses,
            verify_version=verify_version,
            protocol_version=protocol_version,
            **kwargs,
        )

        self.refresh_table_asap: bool = False
        self.route_flags: Dict[bytes, NodeFlag] = self.__class__.ROUTING_FLAGS.copy()
        self.split_flags: Dict[bytes, NodeFlag] = self.__class__.SPLIT_FLAGS.copy()
        self.response_callbacks: Dict[bytes, Callable[..., Any]] = {}
        self.result_callbacks: Dict[
            bytes, Callable[..., Any]
        ] = self.__class__.RESULT_CALLBACKS.copy()
        self.non_atomic_cross_slot = non_atomic_cross_slot

    @classmethod
    @overload
    def from_url(
        cls: Type[RedisClusterBytesT],
        url: str,
        *,
        db: Optional[int] = ...,
        skip_full_coverage_check: bool = ...,
        decode_responses: Literal[False] = ...,
        protocol_version: Literal[2, 3] = ...,
        **kwargs: Any,
    ) -> RedisClusterBytesT:
        ...

    @classmethod
    @overload
    def from_url(
        cls: Type[RedisClusterStringT],
        url: str,
        *,
        db: Optional[int] = ...,
        skip_full_coverage_check: bool = ...,
        decode_responses: Literal[True],
        protocol_version: Literal[2, 3] = ...,
        **kwargs: Any,
    ) -> RedisClusterStringT:
        ...

    @classmethod
    def from_url(
        cls: Type[RedisClusterT],
        url: str,
        *,
        db: Optional[int] = None,
        skip_full_coverage_check: bool = False,
        decode_responses: bool = False,
        protocol_version: Literal[2, 3] = 2,
        **kwargs: Any,
    ) -> RedisClusterT:
        """
        Return a Redis client object configured from the given URL, which must
        use either the ``redis://`` scheme
        `<http://www.iana.org/assignments/uri-schemes/prov/redis>`_ for RESP
        connections or the ``unix://`` scheme for Unix domain sockets.
        For example:

            - ``redis://[:password]@localhost:6379/0``
            - ``unix://[:password]@/path/to/socket.sock?db=0``

        There are several ways to specify a database number. The parse function
        will return the first specified option:

            #. A ``db`` querystring option, e.g. ``redis://localhost?db=0``
            #. If using the ``redis://`` scheme, the path argument of the url, e.g.
               ``redis://localhost/0``
            #. The ``db`` argument to this function.

        If none of these options are specified, db=0 is used.

        Any additional querystring arguments and keyword arguments will be
        passed along to the :class:`ConnectionPool` class's initializer. In the case
        of conflicting arguments, querystring arguments always win.
        """
        if decode_responses:
            return cls(
                decode_responses=True,
                protocol_version=protocol_version,
                connection_pool=ClusterConnectionPool.from_url(
                    url,
                    db=db,
                    skip_full_coverage_check=skip_full_coverage_check,
                    decode_responses=decode_responses,
                    protocol_version=protocol_version,
                    **kwargs,
                ),
            )
        else:
            return cls(
                decode_responses=False,
                protocol_version=protocol_version,
                connection_pool=ClusterConnectionPool.from_url(
                    url,
                    db=db,
                    skip_full_coverage_check=skip_full_coverage_check,
                    decode_responses=decode_responses,
                    protocol_version=protocol_version,
                    **kwargs,
                ),
            )

    def __repr__(self) -> str:
        servers = list(
            {
                "{}:{}".format(info["host"], info["port"])
                for info in self.connection_pool.nodes.startup_nodes
            }
        )
        servers.sort()

        return "{}<{}>".format(type(self).__name__, ", ".join(servers))

    @property
    def all_nodes(self) -> Iterator[Redis[AnyStr]]:
        """ """
        for node in self.connection_pool.nodes.all_nodes():
            yield cast(
                Redis[AnyStr],
                self.connection_pool.nodes.get_redis_link(node["host"], node["port"]),
            )

    @property
    def primaries(self) -> Iterator[Redis[AnyStr]]:
        """ """
        for master in self.connection_pool.nodes.all_primaries():
            yield cast(
                Redis[AnyStr],
                self.connection_pool.nodes.get_redis_link(
                    master["host"], master["port"]
                ),
            )

    @property
    def replicas(self) -> Iterator[Redis[AnyStr]]:
        """ """
        for replica in self.connection_pool.nodes.all_replicas():
            yield cast(
                Redis[AnyStr],
                self.connection_pool.nodes.get_redis_link(
                    replica["host"], replica["port"]
                ),
            )

    def set_result_callback(self, command: str, callback: Callable[..., Any]) -> None:
        "Sets a custom Result Callback"
        self.result_callbacks[command.encode("latin-1")] = callback

    def _determine_slot(self, command: bytes, *args: ValueT) -> Optional[int]:
        """Figures out what slot based on command and args"""

        if len(args) <= 0:
            raise RedisClusterException(
                f"No way to dispatch command:{str(command)} with args: {args}"
                " to Redis Cluster. Missing key."
            )
        keys: Tuple[ValueT, ...] = KeySpec.extract_keys(
            (command,) + args, self.connection_pool.readonly
        )
        if (
            command
            in {
                CommandName.EVAL,
                CommandName.EVALSHA,
                CommandName.FCALL,
                CommandName.FCALL_RO,
            }
            and not keys
        ):
            return None

        slots = {self.connection_pool.nodes.keyslot(key) for key in keys}
        if not slots:
            raise RedisClusterException(
                f"No way to dispatch command:{str(command)} with args: {args}"
                " to Redis Cluster. Missing key."
            )

        if len(slots) != 1:
            raise RedisClusterException(
                f"{str(command)} - all keys must map to the same key slot"
            )
        return slots.pop()

    def _merge_result(
        self,
        command: bytes,
        res: Dict[str, R],
        **kwargs: Optional[ValueT],
    ) -> R:
        if command in self.result_callbacks:
            # TODO: move cluster combine callbacks inline
            return cast(
                R,
                self.result_callbacks[command](
                    res, version=self.protocol_version, **kwargs
                ),
            )
        return first_key(res)

    def determine_node(
        self, command: bytes, **kwargs: Optional[ValueT]
    ) -> Optional[Iterable[Node]]:
        node_flag = self.route_flags.get(command)

        if command in self.split_flags and self.non_atomic_cross_slot:
            node_flag = self.split_flags[command]

        if node_flag == NodeFlag.RANDOM:
            return [self.connection_pool.nodes.random_node()]
        elif node_flag == NodeFlag.PRIMARIES:
            return self.connection_pool.nodes.all_primaries()
        elif node_flag == NodeFlag.REPLICAS:
            return self.connection_pool.nodes.all_replicas()
        elif node_flag == NodeFlag.ALL:
            return self.connection_pool.nodes.all_nodes()
        elif node_flag == NodeFlag.SLOT_ID:
            if (slot := kwargs.get("slot_id")) is None:
                raise RedisClusterException(
                    f"slot_id is needed to execute command {command.decode('latin-1')} {kwargs}"
                )
            if node_from_slot := self.connection_pool.nodes.node_from_slot(int(slot)):
                return [node_from_slot]
        return None

    @clusterdown_wrapper
    async def execute_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., R] = NoopCallback(),
        **kwargs: Optional[ValueT],
    ) -> R:
        """
        Sends a command to a node in the cluster
        """
        if not self.connection_pool.initialized:
            await self.connection_pool.initialize()
            self.refresh_table_asap = False

        if not command:
            raise RedisClusterException("Unable to determine command to use")

        nodes = self.determine_node(command, **kwargs)
        if nodes:
            try:
                return await self.execute_command_on_nodes(
                    nodes, command, *args, callback=callback, **kwargs
                )
            except ClusterDownError:
                self.connection_pool.disconnect()
                self.connection_pool.reset()
                self.refresh_table_asap = True
                raise

        if self.refresh_table_asap:
            await self.connection_pool.nodes.initialize()
            self.refresh_table_asap = False

        redirect_addr = None
        asking = False

        try_random_node = False
        try_random_type = NodeFlag.ALL
        slot = self._determine_slot(command, *args)
        if not slot:
            try_random_node = True
            try_random_type = NodeFlag.PRIMARIES

        ttl = int(self.RedisClusterRequestTTL)

        while ttl > 0:
            ttl -= 1

            if asking and redirect_addr:
                node = self.connection_pool.nodes.nodes[redirect_addr]
                r = self.connection_pool.get_connection_by_node(node)
            elif try_random_node:
                r = self.connection_pool.get_random_connection(
                    primary=try_random_type == NodeFlag.PRIMARIES
                )
                try_random_node = False
            elif slot is not None:
                if self.refresh_table_asap:
                    # MOVED
                    node = self.connection_pool.get_master_node_by_slot(slot)
                else:
                    node = self.connection_pool.get_node_by_slot(slot, command)
                r = self.connection_pool.get_connection_by_node(node)
            else:
                continue

            try:
                if asking:
                    await r.send_command(CommandName.ASKING)
                    await self.parse_response(r, decode=kwargs.get("decode"))
                    asking = False

                await r.send_command(command, *args)

                return callback(
                    await self.parse_response(r, decode=kwargs.get("decode")), **kwargs
                )
            except (RedisClusterException, BusyLoadingError, asyncio.CancelledError):
                raise
            except (ConnectionError, TimeoutError):
                try_random_node = True

                if ttl < self.RedisClusterRequestTTL / 2:
                    await asyncio.sleep(0.1)
            except ClusterDownError as e:
                self.connection_pool.disconnect()
                self.connection_pool.reset()
                self.refresh_table_asap = True

                raise e
            except MovedError as e:
                # Reinitialize on ever x number of MovedError.
                # This counter will increase faster when the same client object
                # is shared between multiple threads. To reduce the frequency you
                # can set the variable 'reinitialize_steps' in the constructor.
                self.refresh_table_asap = True
                await self.connection_pool.nodes.increment_reinitialize_counter()

                node = self.connection_pool.nodes.set_node(
                    e.host, e.port, server_type="master"
                )
                self.connection_pool.nodes.slots[e.slot_id][0] = node
            except TryAgainError:
                if ttl < self.RedisClusterRequestTTL / 2:
                    await asyncio.sleep(0.05)
            except AskError as e:
                redirect_addr, asking = f"{e.host}:{e.port}", True
            finally:
                self.ensure_server_version(r.server_version)
                self.connection_pool.release(r)

        raise ClusterError("TTL exhausted.")

    async def execute_command_on_nodes(
        self,
        nodes: Iterable[Node],
        command: bytes,
        *args: ValueT,
        callback: Callable[..., R],
        **options: Optional[ValueT],
    ) -> R:
        res: Dict[str, R] = {}
        node_arg_mapping: Dict[str, Tuple[ValueT, ...]] = {}
        if command in self.split_flags and self.non_atomic_cross_slot:
            keys = KeySpec.extract_keys((command,) + args)
            if keys:
                key_start: int = args.index(keys[0])
                key_end: int = args.index(keys[-1])
                if not args[key_start : 1 + key_end] == keys:
                    raise ClusterRoutingError(
                        f"Unable to map {command.decode('latin-1')} by keys {keys}"
                    )
                for node_name, node_keys in self.connection_pool.nodes.keys_to_nodes(
                    *keys
                ).items():
                    node_arg_mapping[node_name] = (
                        *args[:key_start],
                        *node_keys,  # type: ignore
                        *args[1 + key_end :],
                    )
        for node in nodes:
            connection = self.connection_pool.get_connection_by_node(node)
            # copy from redis-py
            try:
                if node["name"] in node_arg_mapping:
                    await connection.send_command(
                        command, *node_arg_mapping[node["name"]]
                    )
                else:
                    node_arg_mapping[node["name"]] = args
                    await connection.send_command(command, *args)
                res[node["name"]] = callback(
                    await self.parse_response(connection, decode=options.get("decode")),
                    version=self.protocol_version,
                    **options,
                )
            except asyncio.CancelledError:
                # do not retry when coroutine is cancelled
                connection.disconnect()
                raise
            except (ConnectionError, TimeoutError) as e:
                connection.disconnect()

                if not connection.retry_on_timeout and isinstance(e, TimeoutError):
                    raise
                try:
                    await connection.send_command(command, *args)
                except ConnectionError as err:
                    # if a retry attempt results in a connection error assume cluster error
                    raise ClusterDownError(str(err))
                res[node["name"]] = callback(
                    await self.parse_response(connection, decode=options.get("decode")),
                    version=self.protocol_version,
                    **options,
                )

            finally:
                self.ensure_server_version(connection.server_version)
                self.connection_pool.release(connection)

        return self._merge_result(command, res, **options)

    def lock(
        self,
        name: StringT,
        timeout: Optional[float] = None,
        sleep: float = 0.1,
        blocking_timeout: Optional[bool] = None,
        lock_class: Optional[Type[LuaLock[AnyStr]]] = None,
        thread_local: bool = True,
    ) -> Lock[AnyStr]:
        """
        Return a new :class:`~coredis.lock.Lock` object using :paramref:`name` that mimics
        the behavior of :class:`threading.Lock`.

        See: :class:`~coredis.lock.LuaLock` (the default :paramref:`lock_class`)
         for more details.

        :raises: :exc:`~coredis.LockError`
        """

        if lock_class is None:
            if self._use_lua_lock is None:  # type: ignore
                # the first time .lock() is called, determine if we can use
                # Lua by attempting to register the necessary scripts
                try:
                    LuaLock[AnyStr].register_scripts(self)
                    self._use_lua_lock = True
                except ResponseError:
                    self._use_lua_lock = False
            lock_class = LuaLock[AnyStr]

        return lock_class(
            self,
            name,
            timeout=timeout,
            sleep=sleep,
            blocking_timeout=blocking_timeout,
            thread_local=thread_local,
        )

    def pubsub(
        self, ignore_subscribe_messages: bool = False, **kwargs: Any
    ) -> ClusterPubSub[AnyStr]:
        """
        Return a Pub/Sub instance that can be used to subscribe to channels or
        patterns in a redis cluster and receive messages that get published to them.

        :param ignore_subscribe_messages: Whether to skip subscription
         acknowledgement messages
        """
        return ClusterPubSub[AnyStr](
            self.connection_pool,
            ignore_subscribe_messages=ignore_subscribe_messages,
            **kwargs,
        )

    @versionadded(version="3.6.0")
    def sharded_pubsub(
        self,
        ignore_subscribe_messages: bool = False,
        read_from_replicas: bool = False,
        **kwargs: Any,
    ) -> ShardedPubSub[AnyStr]:
        """
        Return a Pub/Sub instance that can be used to subscribe to channels
        in a redis cluster and receive messages that get published to them. The
        implementation returned differs from that returned by :meth:`pubsub`
        as it uses the Sharded Pub/Sub implementation which routes messages
        to cluster nodes using the same algorithm used to assign keys to slots.
        This effectively restricts the propagation of messages to be within the
        shard of a cluster hence affording horizontally scaling the use of Pub/Sub
        with the cluster itself.

        :param ignore_subscribe_messages: Whether to skip subscription
         acknowledgement messages
        :param read_from_replicas: Whether to read messages from replica nodes

        New in :redis-version:`7.0.0`
        """

        return ShardedPubSub[AnyStr](
            self.connection_pool,
            ignore_subscribe_messages=ignore_subscribe_messages,
            read_from_replicas=read_from_replicas,
            **kwargs,
        )

    async def pipeline(
        self,
        transaction: Optional[bool] = None,
        watches: Optional[Parameters[StringT]] = None,
    ) -> "coredis.pipeline.ClusterPipeline[AnyStr]":
        """
        Pipelines in cluster mode only provide a subset of the functionality
        of pipelines in standalone mode.

        Specifically:

        - Transactions are only supported if all commands in the pipeline
          only access keys which route to the same node.
        - Each command in the pipeline should only access keys on the same node
        - Transactions with :paramref:`watch` are not supported.
        """
        await self.connection_pool.initialize()

        from coredis.pipeline import ClusterPipeline

        return ClusterPipeline[AnyStr].proxy(
            connection_pool=self.connection_pool,
            startup_nodes=self.connection_pool.nodes.startup_nodes,
            result_callbacks=self.result_callbacks,
            response_callbacks=self.response_callbacks,
            transaction=transaction,
            watches=watches,
        )

    async def transaction(
        self,
        func: Callable[
            ["coredis.pipeline.ClusterPipeline[AnyStr]"],
            Coroutine[Any, Any, Any],
        ],
        *watches: StringT,
        **kwargs: Any,
    ) -> Any:
        """
        Convenience method for executing the callable :paramref:`func` as a
        transaction while watching all keys specified in :paramref:`watches`.
        The :paramref:`func` callable should expect a single argument which is a
        :class:`~coredis.pipeline.ClusterPipeline` instance retrieved
        by calling :meth:`~coredis.RedisCluster.pipeline`

        .. warning:: Cluster transactions can only be run with commands that
           route to the same node.
        """
        value_from_callable = kwargs.pop("value_from_callable", False)
        watch_delay = kwargs.pop("watch_delay", None)
        async with await self.pipeline(True, watches=watches) as pipe:
            while True:
                try:
                    func_value = await func(pipe)
                    exec_value = await pipe.execute()
                    return func_value if value_from_callable else exec_value
                except WatchError:
                    if watch_delay is not None and watch_delay > 0:
                        await asyncio.sleep(watch_delay)
                    continue

    async def scan_iter(
        self,
        match: Optional[StringT] = None,
        count: Optional[int] = None,
        type_: Optional[StringT] = None,
    ) -> AsyncIterator[AnyStr]:
        for node in self.primaries:
            cursor = None
            while cursor != 0:
                cursor, data = await node.scan(cursor or 0, match, count, type_)
                for item in data:
                    yield item
