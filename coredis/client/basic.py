from __future__ import annotations

import contextlib
import contextvars
import random
import warnings
from collections import defaultdict
from ssl import SSLContext
from typing import TYPE_CHECKING, Any, Coroutine, cast, overload

from anyio import AsyncContextManagerMixin, sleep
from deprecated.sphinx import versionadded
from exceptiongroup import catch
from packaging.version import InvalidVersion, Version
from typing_extensions import Self

from coredis._utils import logger, nativestr
from coredis.cache import AbstractCache
from coredis.commands import CommandRequest
from coredis.commands._key_spec import KeySpec
from coredis.commands._validators import (
    mutually_inclusive_parameters,
)
from coredis.commands.constants import CommandFlag, CommandName
from coredis.commands.core import CoreCommands
from coredis.commands.function import Library
from coredis.commands.pubsub import PubSub, SubscriptionCallback
from coredis.commands.script import Script
from coredis.commands.sentinel import SentinelCommands
from coredis.config import Config
from coredis.connection import (
    BaseConnection,
    RedisSSLContext,
    UnixDomainSocketConnection,
)
from coredis.credentials import AbstractCredentialProvider
from coredis.exceptions import (
    ConnectionError,
    PersistenceError,
    ReplicationError,
    WatchError,
)
from coredis.globals import CACHEABLE_COMMANDS, COMMAND_FLAGS, READONLY_COMMANDS
from coredis.modules import ModuleMixin
from coredis.pool import ConnectionPool
from coredis.response._callbacks import (
    AsyncPreProcessingCallback,
    NoopCallback,
    ResponseCallback,
)
from coredis.response.types import ScoredMember
from coredis.retry import ConstantRetryPolicy, NoRetryPolicy, RetryPolicy
from coredis.typing import (
    AnyStr,
    AsyncGenerator,
    AsyncIterator,
    Callable,
    ExecutionParameters,
    Generic,
    Iterator,
    KeyT,
    Literal,
    Mapping,
    Parameters,
    ParamSpec,
    RedisCommandP,
    RedisValueT,
    StringT,
    T_co,
    TypeAdapter,
    TypeVar,
    Unpack,
    ValueT,
)

P = ParamSpec("P")
R = TypeVar("R")

if TYPE_CHECKING:
    import coredis.pipeline
    from coredis.lock import Lock
    from coredis.stream import Consumer, GroupConsumer, StreamParameters

ClientT = TypeVar("ClientT", bound="Client[Any]")
RedisT = TypeVar("RedisT", bound="Redis[Any]")


class Client(
    AsyncContextManagerMixin,
    Generic[AnyStr],
    CoreCommands[AnyStr],
    ModuleMixin[AnyStr],
    SentinelCommands[AnyStr],
):
    connection_pool: ConnectionPool
    decode_responses: bool
    encoding: str
    server_version: Version | None
    callback_storage: dict[type[ResponseCallback[Any, Any]], dict[str, Any]]
    type_adapter: TypeAdapter

    def __init__(
        self,
        host: str | None = "localhost",
        port: int | None = 6379,
        db: int = 0,
        username: str | None = None,
        password: str | None = None,
        credential_provider: AbstractCredentialProvider | None = None,
        stream_timeout: float | None = None,
        connect_timeout: float | None = None,
        connection_pool: ConnectionPool | None = None,
        connection_pool_cls: type[ConnectionPool] = ConnectionPool,
        unix_socket_path: str | None = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        ssl: bool = False,
        ssl_context: SSLContext | None = None,
        ssl_keyfile: str | None = None,
        ssl_certfile: str | None = None,
        ssl_cert_reqs: Literal["optional", "required", "none"] | None = None,
        ssl_check_hostname: bool | None = None,
        ssl_ca_certs: str | None = None,
        max_connections: int | None = None,
        max_idle_time: int | None = None,
        client_name: str | None = None,
        verify_version: bool = True,
        noreply: bool = False,
        retry_policy: RetryPolicy = NoRetryPolicy(),
        noevict: bool = False,
        notouch: bool = False,
        type_adapter: TypeAdapter | None = None,
        cache: AbstractCache | None = None,
        **kwargs: Any,
    ):
        if not connection_pool:
            kwargs = {
                "db": db,
                "username": username,
                "password": password,
                "credential_provider": credential_provider,
                "encoding": encoding,
                "stream_timeout": stream_timeout,
                "connect_timeout": connect_timeout,
                "max_connections": max_connections,
                "decode_responses": decode_responses,
                "max_idle_time": max_idle_time,
                "client_name": client_name,
                "noreply": noreply,
                "noevict": noevict,
                "notouch": notouch,
                "_cache": cache,
            }

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
        self.encoding = connection_pool.encoding
        self.decode_responses = connection_pool.decode_responses
        self.server_version: Version | None = None
        self.verify_version = verify_version
        self.__noreply = noreply
        self._noreplycontext: contextvars.ContextVar[bool | None] = contextvars.ContextVar(
            "noreply", default=None
        )
        self._waitcontext: contextvars.ContextVar[tuple[int, int] | None] = contextvars.ContextVar(
            "wait", default=None
        )
        self._waitaof_context: contextvars.ContextVar[tuple[int, int, int] | None] = (
            contextvars.ContextVar("waitaof", default=None)
        )
        self.retry_policy = retry_policy
        self.callback_storage = defaultdict(dict)
        self.type_adapter = type_adapter or TypeAdapter()

    def create_request(
        self,
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., T_co],
        execution_parameters: ExecutionParameters | None = None,
    ) -> CommandRequest[T_co]:
        """
        Factory method to create a command request awaitable.
        Subclasses of :class:`coredis.client.Client` can override this method
        if custom behavior is required. See :class:`~coredis.commands.CommandRequest`
        for details.

        :param name: The name of the command
        :param arguments: all arguments sent to the command
        :param callback: a callback that takes the RESP response and converts it
         into a shape to be returned
        :return: An instance of a command request bound to this client.
        """
        return CommandRequest(
            self, name, *arguments, callback=callback, execution_parameters=execution_parameters
        )

    @property
    def noreply(self) -> bool:
        if not hasattr(self, "_noreplycontext"):
            return False
        ctx = self._noreplycontext.get()
        if ctx is not None:
            return ctx
        return self.__noreply

    @property
    def requires_wait(self) -> bool:
        if not hasattr(self, "_waitcontext") or not self._waitcontext.get():
            return False
        return True

    @property
    def requires_waitaof(self) -> bool:
        if not hasattr(self, "_waitaof_context") or not self._waitaof_context.get():
            return False
        return True

    def _ensure_server_version(self, version: str | None) -> None:
        if self.verify_version and not Config.optimized and not self.server_version and version:
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

    async def _ensure_wait_and_persist(
        self, command: RedisCommandP, connection: BaseConnection
    ) -> None:
        wait = self._waitcontext.get()
        waitaof = self._waitaof_context.get()
        wait_request = None
        aof_request = None
        if wait and wait[0] > 0:
            wait_request = connection.create_request(CommandName.WAIT, *wait, decode=False)
        if waitaof and waitaof[0] > 0:
            aof_request = connection.create_request(CommandName.WAITAOF, *waitaof, decode=False)
        if wait_request and wait:
            wait_result = await wait_request
            if not cast(int, wait_result) >= wait[0]:
                raise ReplicationError(command.name, wait[0], wait[1])
        if aof_request and waitaof:
            aof_result = cast(tuple[int, int], await aof_request)
            if not (aof_result[0] >= waitaof[0] and aof_result[1] >= waitaof[1]):
                raise PersistenceError(command.name, *waitaof)

    def __repr__(self) -> str:
        return f"{type(self).__name__}<{repr(self.connection_pool)}>"

    async def scan_iter(
        self,
        match: StringT | None = None,
        count: int | None = None,
        type_: StringT | None = None,
    ) -> AsyncIterator[AnyStr]:
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.
        """
        cursor = None

        while cursor != 0:
            cursor, data = await self.scan(cursor=cursor, match=match, count=count, type_=type_)

            for item in data:
                yield item

    async def sscan_iter(
        self,
        key: KeyT,
        match: StringT | None = None,
        count: int | None = None,
    ) -> AsyncIterator[AnyStr]:
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.
        """
        cursor = None

        while cursor != 0:
            cursor, data = await self.sscan(key, cursor=cursor, match=match, count=count)

            for item in data:
                yield item

    @overload
    def hscan_iter(
        self,
        key: KeyT,
        match: StringT | None = ...,
        count: int | None = ...,
    ) -> AsyncGenerator[tuple[AnyStr, AnyStr], None]: ...
    @overload
    def hscan_iter(
        self,
        key: KeyT,
        match: StringT | None = ...,
        count: int | None = ...,
        *,
        novalues: Literal[True],
    ) -> AsyncGenerator[AnyStr, None]: ...

    async def hscan_iter(
        self,
        key: KeyT,
        match: StringT | None = None,
        count: int | None = None,
        novalues: Literal[True] | None = None,
    ) -> AsyncGenerator[tuple[AnyStr, AnyStr], None] | AsyncGenerator[AnyStr, None]:
        """
        Make an iterator using the HSCAN command so that the client doesn't
        need to remember the cursor position.
        """
        cursor: int | None = None
        while cursor != 0:
            # TODO: find a better way to narrow the return type from hscan
            if novalues:
                cursor, fields = await self.hscan(
                    key, cursor=cursor, match=match, count=count, novalues=novalues
                )
                for item in fields:
                    yield item
            else:
                cursor, data = await self.hscan(key, cursor=cursor, match=match, count=count)

                for pair in data.items():
                    yield pair

    async def zscan_iter(
        self,
        key: KeyT,
        match: StringT | None = None,
        count: int | None = None,
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

    def register_script(self, script: RedisValueT) -> Script[AnyStr]:
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
        try:
            yield self
        finally:
            self._noreplycontext.set(None)

    @contextlib.contextmanager
    def ensure_replication(
        self: ClientT, replicas: int = 1, timeout_ms: int = 100
    ) -> Iterator[ClientT]:
        """
        Context manager to ensure that commands executed within the context
        are replicated to atleast :paramref:`replicas` within
        :paramref:`timeout_ms` milliseconds.

        Internally this uses `WAIT <https://redis.io/commands/wait>`_ after
        each command executed within the context

        :raises: :exc:`coredis.exceptions.ReplicationError`

        Example::

            client = coredis.RedisCluster("localhost", 7000)
            with client.ensure_replication(1, 20):
                await client.set("fubar", 1)

        """
        self._waitcontext.set((replicas, timeout_ms))
        try:
            yield self
        finally:
            self._waitcontext.set(None)

    @versionadded(version="4.12.0")
    @contextlib.contextmanager
    def ensure_persistence(
        self: ClientT,
        local: Literal[0, 1] = 0,
        replicas: int = 0,
        timeout_ms: int = 100,
    ) -> Iterator[ClientT]:
        """
        Context manager to ensure that commands executed within the context
        are synced to the AOF of a :paramref:`local` host and/or :paramref:`replicas`
        within :paramref:`timeout_ms` milliseconds.

        Internally this uses `WAITAOF <https://redis.io/commands/waitaof>`_ after
        each command executed within the context

        :raises: :exc:`coredis.exceptions.PersistenceError`

        Example for standalone client::

            client = coredis.Redis()
            with client.ensure_persistence(1, 0, 20):
                await client.set("fubar", 1)

        Example for cluster::

            client = coredis.RedisCluster("localhost", 7000)
            with client.ensure_persistence(1, 1, 20):
                await client.set("fubar", 1)

        """
        self._waitaof_context.set((local, replicas, timeout_ms))
        try:
            yield self
        finally:
            self._waitaof_context.set(None)

    def should_quick_release(self, command: RedisCommandP) -> bool:
        return CommandFlag.BLOCKING not in COMMAND_FLAGS[command.name]


class Redis(Client[AnyStr]):
    connection_pool: ConnectionPool

    @overload
    def __init__(
        self: Redis[bytes],
        host: str | None = ...,
        port: int | None = ...,
        db: int = ...,
        *,
        username: str | None = ...,
        password: str | None = ...,
        credential_provider: AbstractCredentialProvider | None = ...,
        stream_timeout: float | None = ...,
        connect_timeout: float | None = ...,
        connection_pool: ConnectionPool | None = ...,
        connection_pool_cls: type[ConnectionPool] = ...,
        unix_socket_path: str | None = ...,
        encoding: str = ...,
        decode_responses: Literal[False] = ...,
        ssl: bool = ...,
        ssl_context: SSLContext | None = ...,
        ssl_keyfile: str | None = ...,
        ssl_certfile: str | None = ...,
        ssl_cert_reqs: Literal["optional", "required", "none"] | None = ...,
        ssl_check_hostname: bool | None = ...,
        ssl_ca_certs: str | None = ...,
        max_connections: int | None = ...,
        max_idle_time: int | None = ...,
        client_name: str | None = ...,
        verify_version: bool = ...,
        cache: AbstractCache | None = ...,
        noreply: bool = ...,
        noevict: bool = ...,
        notouch: bool = ...,
        retry_policy: RetryPolicy = ...,
        type_adapter: TypeAdapter | None = ...,
        **kwargs: Any,
    ) -> None: ...

    @overload
    def __init__(
        self: Redis[str],
        host: str | None = ...,
        port: int | None = ...,
        db: int = ...,
        *,
        username: str | None = ...,
        password: str | None = ...,
        credential_provider: AbstractCredentialProvider | None = ...,
        stream_timeout: float | None = ...,
        connect_timeout: float | None = ...,
        connection_pool: ConnectionPool | None = ...,
        connection_pool_cls: type[ConnectionPool] = ...,
        unix_socket_path: str | None = ...,
        encoding: str = ...,
        decode_responses: Literal[True] = ...,
        ssl: bool = ...,
        ssl_context: SSLContext | None = ...,
        ssl_keyfile: str | None = ...,
        ssl_certfile: str | None = ...,
        ssl_cert_reqs: Literal["optional", "required", "none"] | None = ...,
        ssl_check_hostname: bool | None = ...,
        ssl_ca_certs: str | None = ...,
        max_connections: int | None = ...,
        max_idle_time: int | None = ...,
        client_name: str | None = ...,
        verify_version: bool = ...,
        cache: AbstractCache | None = ...,
        noreply: bool = ...,
        noevict: bool = ...,
        notouch: bool = ...,
        retry_policy: RetryPolicy = ...,
        type_adapter: TypeAdapter | None = ...,
        **kwargs: Any,
    ) -> None: ...

    def __init__(
        self,
        host: str | None = "localhost",
        port: int | None = 6379,
        db: int = 0,
        *,
        username: str | None = None,
        password: str | None = None,
        credential_provider: AbstractCredentialProvider | None = None,
        stream_timeout: float | None = None,
        connect_timeout: float | None = None,
        connection_pool: ConnectionPool | None = None,
        connection_pool_cls: type[ConnectionPool] = ConnectionPool,
        unix_socket_path: str | None = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        ssl: bool = False,
        ssl_context: SSLContext | None = None,
        ssl_keyfile: str | None = None,
        ssl_certfile: str | None = None,
        ssl_cert_reqs: Literal["optional", "required", "none"] | None = None,
        ssl_check_hostname: bool | None = None,
        ssl_ca_certs: str | None = None,
        max_connections: int | None = None,
        max_idle_time: int | None = None,
        client_name: str | None = None,
        verify_version: bool = True,
        cache: AbstractCache | None = None,
        noreply: bool = False,
        noevict: bool = False,
        notouch: bool = False,
        retry_policy: RetryPolicy = ConstantRetryPolicy(
            (ConnectionError, TimeoutError), retries=2, delay=0.01
        ),
        type_adapter: TypeAdapter | None = None,
        **kwargs: Any,
    ) -> None:
        """

        Changes
          - .. versionremoved:: 6.0.0
            - :paramref:`protocol_version` removed (and therefore support for RESP2)
          - .. versionadded:: 6.0.0
            -  TODO: Add stuff
          - .. versionadded:: 4.12.0

            - :paramref:`retry_policy`
            - :paramref:`noevict`
            - :paramref:`notouch`
            - :meth:`Redis.ensure_persistence` context manager
            - Redis Module support

              - RedisJSON: :attr:`Redis.json`
              - RedisBloom:

                - BloomFilter: :attr:`Redis.bf`
                - CuckooFilter: :attr:`Redis.cf`
                - CountMinSketch: :attr:`Redis.cms`
                - TopK: :attr:`Redis.topk`
                - TDigest: :attr:`Redis.tdigest`
              - RedisTimeSeries: :attr:`Redis.timeseries`
              - RedisGraph: :attr:`Redis.graph`
              - RediSearch:

                - Search & Aggregation: :attr:`Redis.search`
                - Autocomplete: Added :attr:`Redis.autocomplete`

          - .. versionchanged:: 4.12.0

            - Removed :paramref:`retry_on_timeout` constructor argument. Use
              :paramref:`retry_policy` instead.

          - .. versionadded:: 4.3.0

            - Added :paramref:`connection_pool_cls`

          - .. versionchanged:: 4.0.0

            - :paramref:`non_atomic_cross_slot` defaults to ``True``
            - :paramref:`protocol_version`` defaults to ``3``

          - .. versionadded:: 3.11.0

            - Added :paramref:`noreply`

          - .. versionadded:: 3.9.0

            - If :paramref:`cache` is provided the client will check & populate
              the cache for read only commands and invalidate it for commands
              that could change the key(s) in the request.

          - .. versionchanged:: 3.5.0

            - The :paramref:`verify_version` parameter now defaults to ``True``

          - .. versionadded:: 3.1.0

            - The :paramref:`protocol_version` and :paramref:`verify_version`
              :parameters were added


        :param host: The hostname of the redis server
        :param port: The port at which th redis server is listening on
        :param db: database number to switch to upon connection
        :param username: Username for authenticating with the redis server
        :param password: Password for authenticating with the redis server
        :param credential_provider: CredentialProvider to get authentication credentials
        :param stream_timeout: Timeout (seconds) when reading responses from the server
        :param connect_timeout: Timeout (seconds) for establishing a connection to the server
        :param connection_pool: The connection pool instance to use. If not provided
         a new pool will be assigned to this client.
        :param connection_pool_cls: The connection pool class to use when constructing
         a connection pool for this instance.
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
        :param max_idle_time: Maximum number of a seconds an unused connection is cached
         before it is disconnected.
        :param client_name: The client name to identifiy with the redis server
        :param verify_version: Validate redis server version against the documented
         version introduced before executing a command and raises a
         :exc:`CommandNotSupportedError` error if the required version is higher than
         the reported server version
        :param cache: If provided the cache will be used to avoid requests for read only
         commands if the client has already requested the data and it hasn't been invalidated.
         The cache is responsible for any mutations to the keys that happen outside of this client
        :param noreply: If ``True`` the client will not request a response for any
         commands sent to the server.
        :param noevict: Ensures that connections from the client will be excluded from the
         client eviction process even if we're above the configured client eviction threshold.
        :param notouch: Ensures that commands sent by the client will not alter the LRU/LFU of
         the keys they access.
        :param retry_policy: The retry policy to use when interacting with the redis server
        :param type_adapter: The adapter to use for serializing / deserializing customs types
         when interacting with redis commands.

        """
        if connection_pool and cache:
            raise RuntimeError("Parameters 'cache' and 'connection_pool' are mutually exclusive!")
        super().__init__(
            host=host,
            port=port,
            db=db,
            username=username,
            password=password,
            credential_provider=credential_provider,
            stream_timeout=stream_timeout,
            connect_timeout=connect_timeout,
            connection_pool=connection_pool,
            connection_pool_cls=connection_pool_cls,
            unix_socket_path=unix_socket_path,
            encoding=encoding,
            decode_responses=decode_responses,
            ssl=ssl,
            ssl_context=ssl_context,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_cert_reqs=ssl_cert_reqs,
            ssl_check_hostname=ssl_check_hostname,
            ssl_ca_certs=ssl_ca_certs,
            max_connections=max_connections,
            max_idle_time=max_idle_time,
            client_name=client_name,
            verify_version=verify_version,
            noreply=noreply,
            noevict=noevict,
            notouch=notouch,
            retry_policy=retry_policy,
            type_adapter=type_adapter,
            cache=cache,
            **kwargs,
        )
        self._decodecontext: contextvars.ContextVar[bool | None,] = contextvars.ContextVar(
            "decode", default=None
        )
        self._encodingcontext: contextvars.ContextVar[str | None,] = contextvars.ContextVar(
            "decode", default=None
        )

    @classmethod
    @overload
    def from_url(
        cls,
        url: str,
        db: int | None = ...,
        *,
        decode_responses: Literal[False] = ...,
        verify_version: bool = ...,
        noreply: bool = ...,
        noevict: bool = ...,
        notouch: bool = ...,
        retry_policy: RetryPolicy = ...,
        cache: AbstractCache | None = ...,
        **kwargs: Any,
    ) -> Redis[bytes]: ...

    @classmethod
    @overload
    def from_url(
        cls,
        url: str,
        db: int | None = ...,
        *,
        decode_responses: Literal[True] = ...,
        verify_version: bool = ...,
        noreply: bool = ...,
        noevict: bool = ...,
        notouch: bool = ...,
        retry_policy: RetryPolicy = ...,
        cache: AbstractCache | None = ...,
        **kwargs: Any,
    ) -> Redis[str]: ...

    @classmethod
    def from_url(
        cls: type[RedisT],
        url: str,
        db: int | None = None,
        *,
        decode_responses: bool = False,
        verify_version: bool = True,
        noreply: bool = False,
        noevict: bool = False,
        notouch: bool = False,
        retry_policy: RetryPolicy = ConstantRetryPolicy(
            (ConnectionError, TimeoutError), retries=2, delay=0.01
        ),
        type_adapter: TypeAdapter | None = None,
        cache: AbstractCache | None = None,
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

        :paramref:`url` and :paramref:`kwargs` are passed as is to
        the :func:`coredis.ConnectionPool.from_url`.
        """
        if decode_responses:
            return cls(
                decode_responses=True,
                verify_version=verify_version,
                noreply=noreply,
                retry_policy=retry_policy,
                type_adapter=type_adapter,
                connection_pool=ConnectionPool.from_url(
                    url,
                    db=db,
                    decode_responses=decode_responses,
                    noreply=noreply,
                    noevict=noevict,
                    notouch=notouch,
                    _cache=cache,
                    **kwargs,
                ),
            )
        else:
            return cls(
                decode_responses=False,
                verify_version=verify_version,
                noreply=noreply,
                retry_policy=retry_policy,
                type_adapter=type_adapter,
                connection_pool=ConnectionPool.from_url(
                    url,
                    db=db,
                    decode_responses=decode_responses,
                    noreply=noreply,
                    noevict=noevict,
                    notouch=notouch,
                    _cache=cache,
                    **kwargs,
                ),
            )

    @contextlib.asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        async with self.connection_pool:
            yield self

    async def execute_command(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **options: Unpack[ExecutionParameters],
    ) -> R:
        """
        Executes a command with configured retries and returns
        the parsed response
        """
        return await self.retry_policy.call_with_retries(
            lambda: self._execute_command(command, callback=callback, **options),
        )

    async def _execute_command(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **options: Unpack[ExecutionParameters],
    ) -> R:
        pool = self.connection_pool
        quick_release = self.should_quick_release(command)
        should_block = not quick_release or self.requires_wait or self.requires_waitaof
        keys = KeySpec.extract_keys(command.name, *command.arguments)
        cacheable = (
            command.name in CACHEABLE_COMMANDS
            and len(keys) == 1
            and not self.noreply
            and self._decodecontext.get() is None
        )
        cached_reply = None
        cache_hit = False
        use_cached = False
        reply = None
        released = False
        connection = await pool.get_connection()
        self._ensure_server_version(connection.server_version)
        try:
            if pool.cache and pool.cache.healthy:
                if connection.tracking_client_id != pool.cache.get_client_id(connection):
                    pool.cache.reset()
                    await connection.update_tracking_client(
                        True, pool.cache.get_client_id(connection)
                    )
                if command.name not in READONLY_COMMANDS:
                    pool.cache.invalidate(*keys)
                elif cacheable:
                    try:
                        cached_reply = cast(
                            R,
                            pool.cache.get(
                                command.name,
                                keys[0],
                                *command.arguments,
                            ),
                        )
                        use_cached = random.random() * 100.0 < min(100.0, pool.cache.confidence)
                        cache_hit = True
                    except KeyError:
                        pass
            if not (use_cached and cached_reply):
                request = connection.create_request(
                    command.name,
                    *command.arguments,
                    noreply=self.noreply,
                    decode=options.get("decode", self._decodecontext.get()),
                    encoding=self._encodingcontext.get(),
                    disconnect_on_cancellation=should_block,
                )
                # TODO: Fix this! using both the release & should_block
                #  flags to decide release logic is fragile. We should be
                #  releasing early even in the cached response flow.

                # if not blocking, no need to wait for reply
                if not should_block:
                    released = True
                    pool.release(connection)
                reply = await request
                await self._ensure_wait_and_persist(command, connection)
                if self.noreply:
                    return None  # type: ignore
                if isinstance(callback, AsyncPreProcessingCallback):
                    await callback.pre_process(self, reply)
            if pool.cache and cacheable:
                if cache_hit and not use_cached:
                    pool.cache.feedback(
                        command.name, keys[0], *command.arguments, match=cached_reply == reply
                    )
                if not cache_hit:
                    pool.cache.put(
                        command.name,
                        keys[0],
                        *command.arguments,
                        value=reply,
                    )
            return callback(cached_reply if cache_hit else reply)
        finally:
            if not released:
                pool.release(connection)

    @overload
    def decoding(
        self, mode: Literal[False], encoding: str | None = None
    ) -> contextlib.AbstractContextManager[Redis[bytes]]: ...

    @overload
    def decoding(
        self, mode: Literal[True], encoding: str | None = None
    ) -> contextlib.AbstractContextManager[Redis[str]]: ...

    @contextlib.contextmanager
    @versionadded(version="4.8.0")
    def decoding(self, mode: bool, encoding: str | None = None) -> Iterator[Redis[Any]]:
        """
        Context manager to temporarily change the decoding behavior
        of the client

        :param mode: Whether to decode or not
        :param encoding: Optional encoding to use if decoding. If not provided
         the :paramref:`~coredis.Redis.encoding` parameter provided to the client will
         be used.

        Example::

            client = coredis.Redis(decode_responses=True)
            await client.set("fubar", "baz")
            assert await client.get("fubar") == "baz"
            with client.decoding(False):
                assert await client.get("fubar") == b"baz"
                with client.decoding(True):
                    assert await client.get("fubar") == "baz"

        """
        prev_decode = self._decodecontext.get()
        prev_encoding = self._encodingcontext.get()
        self._decodecontext.set(mode)
        self._encodingcontext.set(encoding)
        try:
            yield self
        finally:
            self._decodecontext.set(prev_decode)
            self._encodingcontext.set(prev_encoding)

    def pubsub(
        self,
        *,
        channels: Parameters[StringT] | None = None,
        channel_handlers: Mapping[StringT, SubscriptionCallback] | None = None,
        patterns: Parameters[StringT] | None = None,
        pattern_handlers: Mapping[StringT, SubscriptionCallback] | None = None,
        ignore_subscribe_messages: bool = False,
        retry_policy: RetryPolicy | None = None,
        subscription_timeout: float = 1,
        **kwargs: Any,
    ) -> PubSub[AnyStr]:
        """
        Return a Pub/Sub instance that can be used to subscribe to channels
        and patterns and receive messages that get published to them.

        :param channels: channels that the constructed Pubsub instance should
         automatically subscribe to
        :param channel_handlers: Mapping of channels to automatically subscribe to
         and the associated handlers that will be invoked when a message is received
         on the specific channel.
        :param patterns: patterns that the constructed Pubsub instance should
         automatically subscribe to
        :param pattern_handlers: Mapping of patterns to automatically subscribe to
         and the associated handlers that will be invoked when a message is received
         on channel matching the pattern.
        :param ignore_subscribe_messages: Whether to skip subscription
         acknowledgement messages
        :param retry_policy: An explicit retry policy to use in the subscriber.
        :param subscription_timeout: Maximum amount of time in seconds to wait for
         acknowledgement of subscriptions.
        """

        return PubSub[AnyStr](
            self.connection_pool,
            ignore_subscribe_messages=ignore_subscribe_messages,
            retry_policy=retry_policy,
            channels=channels,
            channel_handlers=channel_handlers,
            patterns=patterns,
            pattern_handlers=pattern_handlers,
            subscription_timeout=subscription_timeout,
            **kwargs,
        )

    def pipeline(
        self,
        transaction: bool = True,
        *,
        raise_on_error: bool = True,
        timeout: float | None = None,
    ) -> coredis.pipeline.Pipeline[AnyStr]:
        """
        Returns a new pipeline object that can queue multiple commands for
        batch execution.

        :param transaction: indicates whether all commands should be executed atomically.
        :param raise_on_error: Whether to raise errors upon executing the pipeline.
         If set to `False` errors will be accumulated and retrievable from the individual
         commands that had errors.
        :param timeout: If specified this value will take precedence over
         :paramref:`Redis.stream_timeout`
        """
        from coredis.pipeline import Pipeline

        return Pipeline[AnyStr](self, transaction, raise_on_error, timeout)

    def lock(
        self,
        name: StringT,
        timeout: float | None = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: float | None = None,
    ) -> Lock[AnyStr]:
        """
        Return a lock instance which can be used to guard resource access across
        multiple clients.

        :param name: key for the lock
        :param timeout: indicates a maximum life for the lock.
         By default, it will remain locked until :meth:`release` is called.
         ``timeout`` can be specified as a float or integer, both representing
         the number of seconds to wait.

        :param sleep: indicates the amount of time to sleep per loop iteration
         when the lock is in blocking mode and another client is currently
         holding the lock.

        :param blocking: indicates whether calling :meth:`acquire` should block until
         the lock has been acquired or to fail immediately, causing :meth:`acquire`
         to return ``False`` and the lock not being acquired. Defaults to ``True``.

        :param blocking_timeout: indicates the maximum amount of time in seconds to
         spend trying to acquire the lock. A value of ``None`` indicates
         continue trying forever. ``blocking_timeout`` can be specified as a
         :class:`float` or :class:`int`, both representing the number of seconds to wait.
        """
        from coredis.lock import Lock

        return Lock(self, name, timeout, sleep, blocking, blocking_timeout)

    async def transaction(
        self,
        func: Callable[[coredis.pipeline.Pipeline[AnyStr]], Coroutine[Any, Any, R]],
        *watches: KeyT,
        watch_delay: float | None = None,
    ) -> R:
        """
        Convenience method for executing the callable :paramref:`func` as a
        transaction while watching all keys specified in :paramref:`watches`.

        :param func: callable should expect a single argument which is a
         :class:`coredis.pipeline.Pipeline` object retrieved by calling
         :meth:`~coredis.Redis.pipeline`.
        :param watches: The keys to watch during the transaction
        :param watch_delay: Time in seconds to wait after each watch error before retrying
        """
        msg = "Caught WatchError in transaction, retrying..."
        while True:
            with catch({WatchError: lambda _: logger.warning(msg)}):
                async with self.pipeline(transaction=False) as pipe:
                    async with pipe.watch(*watches):
                        return await func(pipe)
            if watch_delay:
                await sleep(watch_delay)

    @overload
    def xconsumer(
        self,
        streams: Parameters[KeyT],
        *,
        buffer_size: int = ...,
        timeout: int | None = ...,
        group: Literal[None] = ...,
        consumer: Literal[None] = ...,
        auto_create: bool = ...,
        auto_acknowledge: bool = ...,
        start_from_backlog: bool = ...,
        **stream_parameters: StreamParameters,
    ) -> Consumer[AnyStr]: ...
    @overload
    def xconsumer(
        self,
        streams: Parameters[KeyT],
        *,
        buffer_size: int = ...,
        timeout: int | None = ...,
        group: StringT = ...,
        consumer: StringT = ...,
        auto_create: bool = ...,
        auto_acknowledge: bool = ...,
        start_from_backlog: bool = ...,
        **stream_parameters: StreamParameters,
    ) -> GroupConsumer[AnyStr]: ...
    @versionadded(version="6.0.0")
    @mutually_inclusive_parameters("group", "consumer")
    def xconsumer(
        self,
        streams: Parameters[KeyT],
        *,
        buffer_size: int = 0,
        timeout: int | None = None,
        group: StringT | None = None,
        consumer: StringT | None = None,
        auto_create: bool = True,
        auto_acknowledge: bool = False,
        start_from_backlog: bool = False,
        **stream_parameters: StreamParameters,
    ) -> Consumer[AnyStr] | GroupConsumer[AnyStr]:
        """
        Create a stream consumer for one or more Redis streams.

        Depending on whether ``group`` and ``consumer`` are provided, this method
        creates either a standalone stream consumer or a member of a stream
        consumer group.

        If ``group`` and ``consumer`` are not provided, a standalone stream
        consumer is created that starts reading from the latest entry of each
        stream provided in :paramref:`streams`.

        The latest entry is determined by calling
        :meth:`coredis.Redis.xinfo_stream` and using the :data:`last-entry`
        attribute at the point of initializing the consumer instance or on first
        fetch (whichever comes first). If the stream(s) do not exist at the time
        of consumer creation, the consumer will simply start from the minimum
        identifier (``0-0``).

        If ``group`` and ``consumer`` are provided, a member of a stream consumer
        group is created. The consumer has an identical interface as
        :class:`coredis.stream.Consumer`.

        :param streams: the stream identifiers to consume from
        :param buffer_size: Size of buffer (per stream) to maintain. This
         translates to the maximum number of stream entries that are fetched
         on each request to redis.
        :param timeout: Maximum amount of time in milliseconds to block for new
         entries to appear on the streams the consumer is reading from.
        :param group: The name of the group this consumer is part of
        :param consumer: The unique name (within :paramref:`group`) of the consumer
        :param auto_create: If True the group will be created upon initialization
         or first fetch if it doesn't already exist.
        :param auto_acknowledge: If ``True`` the stream entries fetched will be fetched
         without needing to be acknowledged with :meth:`coredis.Redis.xack` to remove
         them from the pending entries list.
        :param start_from_backlog: If ``True`` the consumer will start by fetching any pending
         entries from the pending entry list before considering any new messages
         not seen by any other consumer in the :paramref:`group`
        :param stream_parameters: Mapping of optional parameters to use
         by stream for the streams provided in :paramref:`streams`.

         .. warning:: Providing an ``identifier`` in ``stream_parameters`` has a different
            meaning for a group consumer. If the value is any valid identifier other than ``>``
            the consumer will only access the history of pending messages. That is, the set of
            messages that were delivered to this consumer (identified by :paramref:`consumer`)
            and never acknowledged.
        """
        from coredis.stream import Consumer, GroupConsumer

        if group is not None and consumer is not None:
            return GroupConsumer(
                self,
                streams,
                group=group,
                consumer=consumer,
                buffer_size=buffer_size,
                auto_create=auto_create,
                auto_acknowledge=auto_acknowledge,
                start_from_backlog=start_from_backlog,
                timeout=timeout,
                **stream_parameters,
            )
        else:
            return Consumer(
                self, streams, buffer_size=buffer_size, timeout=timeout, **stream_parameters
            )
