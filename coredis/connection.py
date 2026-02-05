from __future__ import annotations

import dataclasses
import inspect
import math
import os
import socket
import ssl
from abc import ABC, abstractmethod
from collections import deque
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast
from weakref import ProxyType, proxy

from anyio import (
    TASK_STATUS_IGNORED,
    BrokenResourceError,
    CancelScope,
    CapacityLimiter,
    ClosedResourceError,
    EndOfStream,
    Event,
    connect_tcp,
    connect_unix,
    create_memory_object_stream,
    create_task_group,
    fail_after,
    get_cancelled_exc_class,
    move_on_after,
)
from anyio.abc import ByteStream, SocketAttribute, TaskGroup, TaskStatus
from anyio.lowlevel import checkpoint
from exceptiongroup import BaseExceptionGroup, catch

import coredis
from coredis._packer import Packer
from coredis._utils import logger, nativestr
from coredis.commands.constants import CommandName
from coredis.constants.resp import DataType
from coredis.credentials import (
    AbstractCredentialProvider,
    UserPass,
    UserPassCredentialProvider,
)
from coredis.exceptions import (
    AuthenticationRequiredError,
    ConnectionError,
    RedisError,
    UnknownCommandError,
)
from coredis.parser import NotEnoughData, Parser
from coredis.tokens import PureToken
from coredis.typing import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Generator,
    NotRequired,
    RedisValueT,
    ResponseType,
    TypedDict,
    TypeVar,
    Unpack,
)

CERT_REQS = {
    "none": ssl.CERT_NONE,
    "optional": ssl.CERT_OPTIONAL,
    "required": ssl.CERT_REQUIRED,
}
R = TypeVar("R")

if TYPE_CHECKING:
    from coredis.pool.nodemanager import ManagedNode


class BaseConnectionParams(TypedDict):
    """
    The common parameters accepted by :class:`coredis.connection.BaseConnection`
    """

    #: Maximum time to wait for receiving a response
    #: for requests created through this connection.
    stream_timeout: NotRequired[float | None]
    #: Maximum time to wait for establishing a connection
    connect_timeout: NotRequired[float | None]

    #: Default encoding for command responses.
    encoding: NotRequired[str]
    #: Whether to automatically decode responses.
    decode_responses: NotRequired[bool]

    #: Optional name to register with the server.
    client_name: NotRequired[str | None]
    #: If True, disables replies for all commands.
    noreply: NotRequired[bool]
    #: If True, sets CLIENT NO-EVICT on the connection.
    noevict: NotRequired[bool]
    #: If True, sets CLIENT NO-TOUCH on the connection.
    notouch: NotRequired[bool]
    #: Maximum idle time in seconds before the connection is closed.
    max_idle_time: NotRequired[int | None]

    #: Limiter to throttle CPU-bound processing.
    processing_budget: NotRequired[CapacityLimiter]

    #: The username to use for authenticating against the redis server
    username: NotRequired[str | None]
    #: The password to use for authenticating against the redis server
    password: NotRequired[str | None]
    #: If provided the connection handshake will include authentication using this provider.
    credential_provider: NotRequired[AbstractCredentialProvider]
    #: If provided the connection will immediately switch to this db as part of the handshake
    db: NotRequired[int | None]
    #: For TLS connections, the ssl context to use when performing the TLS handshake
    ssl_context: NotRequired[ssl.SSLContext]


@dataclasses.dataclass
class Request:
    connection: ProxyType[BaseConnection]
    command: bytes
    decode: bool
    encoding: str | None = None
    raise_exceptions: bool = True
    response_timeout: float | None = None
    disconnect_on_cancellation: bool = False
    _event: Event = dataclasses.field(default_factory=Event)
    _exc: BaseException | None = None
    _result: ResponseType | None = None

    def __await__(self) -> Generator[Any, None, ResponseType]:
        return self.get_result().__await__()

    def resolve(self, response: ResponseType) -> None:
        self._result = response
        self._event.set()

    def fail(self, error: BaseException) -> None:
        if not self._event.is_set():
            self._exc = error
            self._event.set()

    async def get_result(self) -> ResponseType:
        if not self._event.is_set():
            try:
                with move_on_after(self.response_timeout) as scope:
                    await self._event.wait()
                if scope.cancelled_caught and not self._event.is_set():
                    reason = (
                        f"{nativestr(self.command)} timed out after {self.response_timeout} seconds"
                    )
                    self._exc = TimeoutError(reason)
                    self._handle_response_cancellation(reason)
            except get_cancelled_exc_class():
                self._handle_response_cancellation(f"{nativestr(self.command)} was cancelled")
                raise
        return self._result_or_exc()

    def _handle_response_cancellation(self, reason: str) -> None:
        if self.connection and self.disconnect_on_cancellation:
            self.connection.terminate(reason)

    def _result_or_exc(self) -> ResponseType:
        if self._exc is not None:
            if self.raise_exceptions:
                raise self._exc
            return self._exc  # type: ignore
        return self._result


@dataclasses.dataclass
class CommandInvocation:
    command: bytes
    args: tuple[RedisValueT, ...]
    decode: bool | None
    encoding: str | None


class RedisSSLContext:
    context: ssl.SSLContext | None

    def __init__(
        self,
        keyfile: str | None,
        certfile: str | None,
        cert_reqs: str | ssl.VerifyMode | None = None,
        ca_certs: str | None = None,
        check_hostname: bool | None = None,
    ) -> None:
        self.keyfile = keyfile
        self.certfile = certfile
        self.check_hostname = check_hostname if check_hostname is not None else False
        if cert_reqs is None:
            self.cert_reqs = ssl.CERT_OPTIONAL
        elif isinstance(cert_reqs, str):
            self.cert_reqs = CERT_REQS[cert_reqs]
        else:
            self.cert_reqs = cert_reqs
        self.ca_certs = ca_certs
        self.context = None

    def get(self) -> ssl.SSLContext:
        if not self.context:
            self.context = ssl.create_default_context()
            if self.certfile and self.keyfile:
                self.context.load_cert_chain(certfile=self.certfile, keyfile=self.keyfile)
            if self.ca_certs:
                self.context.load_verify_locations(
                    **{("capath" if os.path.isdir(self.ca_certs) else "cafile"): self.ca_certs}
                )
            self.context.check_hostname = self.check_hostname
            self.context.verify_mode = self.cert_reqs
        return self.context


class BaseConnection(ABC):
    """
    Base class for Redis connections.

    Manages a low-level connection to a single Redis server:
    sending commands, queuing requests, receiving and parsing responses,
    handling RESP3 push messages, and connection lifecycle management.

    Subclasses must implement the :meth:`_connect` method to establish the underlying
    transport (TCP, UNIX socket, etc.).

    """

    Params = BaseConnectionParams
    """
    :meta private:
    """

    def __init__(
        self,
        *,
        stream_timeout: float | None = None,
        connect_timeout: float | None = None,
        max_idle_time: int | None = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        credential_provider: AbstractCredentialProvider | None = None,
        username: str | None = None,
        password: str | None = None,
        client_name: str | None = None,
        db: int | None = 0,
        noreply: bool = False,
        noevict: bool = False,
        notouch: bool = False,
        processing_budget: CapacityLimiter = CapacityLimiter(1),
        ssl_context: ssl.SSLContext | None = None,
    ):
        """
        :param stream_timeout: Maximum time to wait for receiving a response
         for requests created through this connection.
        :param connect_timeout: Maximum time to wait for establishing a connection
        :param max_idle_time: Maximum idle time in seconds before the connection is closed.
        :param encoding: Default encoding for command responses.
        :param decode_responses: Whether to automatically decode responses.
        :param credential_provider: If provided the connection handshake will include
         authentication using this provider.
        :param username: The username to use for authenticating against the redis server
        :param password: The password to use for authenticating against the redis server
        :param client_name: Optional name to register with the server.
        :param db: If provided the connection will immediately switch to this db as part
         of the handshake
        :param noreply: If True, disables replies for all commands.
        :param noevict: If True, sets CLIENT NO-EVICT on the connection.
        :param notouch: If True, sets CLIENT NO-TOUCH on the connection.
        :param processing_budget: limiter to throttle CPU-bound processing.
        :param ssl_context: For TLS connections, the ssl context to use when performing
         the TLS handshake.
        """
        self._stream_timeout = stream_timeout
        self._connect_timeout = connect_timeout
        # maximum time to wait for data from the server before
        # closing the connection
        self._max_idle_time = max_idle_time

        self._username = username
        self._password = password
        self._credential_provider = credential_provider

        self._db = db

        self._encoding = encoding
        self._decode_responses = decode_responses

        self._connect_callbacks: list[
            (Callable[[BaseConnection], Awaitable[None]] | Callable[[BaseConnection], None])
        ] = list()

        # server version as reported by the server
        self.server_version: str | None = None
        # name used to identify thie connection with the redis server
        self.client_name = client_name
        # id for this connection as returned by the redis server
        self.client_id: int | None = None
        # client id that the redis server should send any redirected notifications to
        self.tracking_client_id: int | None = None

        self._noreply = noreply
        self._noreply_set = False
        self._noevict = noevict
        self._notouch = notouch

        self._ssl_context = ssl_context

        self._task_group: TaskGroup | None = None
        # The actual connection to the server
        self._connection: ByteStream | None = None
        # buffer for push message types
        self._push_message_buffer_in, self._push_message_buffer_out = create_memory_object_stream[
            list[ResponseType]
        ](math.inf)
        # buffer for writes to the socket
        self._write_buffer_in, self._write_buffer_out = create_memory_object_stream[list[bytes]](
            math.inf
        )
        #  for writes to the socket
        self._write_buffer_in, self._write_buffer_out = create_memory_object_stream[list[bytes]](
            math.inf
        )
        self._parser = Parser()
        self._packer: Packer = Packer(self._encoding)

        self._requests: deque[Request] = deque()
        self._connection_cancel_scope: CancelScope | None = None

        # whether the `HELLO` handshake needs to be performed
        self._needs_handshake = True

        # Error flags
        self._last_error: BaseException | None = None
        self._connected = False
        self._transport_failed = False

        # To be used in the read task for cpu bound processing after data is received
        self._processing_budget = processing_budget

    def __repr__(self) -> str:
        return self.describe()

    @abstractmethod
    def describe(self) -> str: ...

    @property
    @abstractmethod
    def location(self) -> str: ...

    @property
    def connection(self) -> ByteStream:
        if not self._connection:
            raise ConnectionError("Connection not initialized correctly!") from self._last_error
        return self._connection

    @contextmanager
    def ensure_connection(self) -> Generator[ByteStream]:
        yield self.connection

    @property
    def is_connected(self) -> bool:
        """
        Whether the connection is established and initial handshakes were
        performed without error
        """
        return self._connected and self._connection is not None and not self._transport_failed

    def register_connect_callback(
        self,
        callback: (Callable[[BaseConnection], None] | Callable[[BaseConnection], Awaitable[None]]),
    ) -> None:
        self._connect_callbacks.append(callback)

    def clear_connect_callbacks(self) -> None:
        self._connect_callbacks = list()

    @abstractmethod
    async def _connect(self) -> ByteStream:
        """
        Establish and return the underlying transport connection to the Redis server.
        """
        ...

    async def run(self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED) -> None:
        """
        Establish a connection to the redis server and initiate any post connect callbacks.

        .. note:: This method can only be called once for an :class:`~coredis.connection.BaseConnection`
           instance. Once the connection closes either due to cancellation or errors it should be
           discarded.

        """
        if self._task_group:
            raise RuntimeError("Connection cannot be reused")

        try:
            self._connection = await self._connect()
        except Exception as connection_error:
            self._last_error = connection_error
            if isinstance(connection_error, RedisError):
                raise
            else:
                # Wrap any other errors with a ConnectionError so that upstreams (pools) can
                # handle them explicitly as being part of connection creation if they want.
                raise ConnectionError("Unable to establish a connection") from connection_error

        def handle_errors(error: BaseExceptionGroup) -> None:
            # TODO: change _last_error to use the whole exception group
            #  once python 3.10 support is dropped and the library
            #  consistently uses exception groups
            self._last_error = self._last_error or error.exceptions[-1]

        try:
            with catch({Exception: handle_errors}):
                async with (
                    self.connection,
                    self._write_buffer_in,
                    self._write_buffer_out,
                    self._push_message_buffer_in,
                    self._push_message_buffer_out,
                    create_task_group() as self._task_group,
                ):
                    self._task_group.start_soon(self._reader_task)
                    self._task_group.start_soon(self._writer_task)
                    # setup connection
                    await self.on_connect()
                    for callback in self._connect_callbacks:
                        task = callback(self)
                        if inspect.isawaitable(task):
                            await task
                    self._connected = True
                    task_status.started()
        finally:
            disconnect_exc = self._last_error or ConnectionError("Connection lost!")
            self._parser.on_disconnect()
            while self._requests:
                request = self._requests.popleft()
                request.fail(disconnect_exc)
            self._connection = None
            if not self._connected:
                # If a RedisError (raised ourselves) has resulted in an exception
                # before a connection was completely established raise that.
                # This is due to known handshake errors.
                if isinstance(self._last_error, RedisError):
                    raise self._last_error
                else:
                    logger.exception("Connection attempt failed unexpectedly!")
                    raise ConnectionError("Unable to establish a connection") from self._last_error
            else:
                # If a connection had successfully been established (including handshake)
                # errors should no longer be raised and it is the responsibility of the
                # downstream to ensure that `is_connected` is tested before using a connection
                if self._last_error:
                    logger.info("Connection closed unexpectedly!", exc_info=True)
                self._connected = False

    def terminate(self, reason: str | None = None) -> None:
        """
        Terminates the connection prematurely

        :meta private:
        """
        if self._task_group:
            self._task_group.cancel_scope.cancel(reason)

    async def _reader_task(self) -> None:
        """
        Listen on the socket and run the parser, completing pending requests in
        FIFO order.
        """
        while True:
            with fail_after(self._max_idle_time):
                try:
                    data = await self.connection.receive()
                except (EndOfStream, ClosedResourceError, BrokenResourceError) as err:
                    self._transport_failed = True
                    raise ConnectionError("Connection lost while receiving response") from err
                except Exception:
                    self._transport_failed = True
                    raise
                # If we're receiving data without any inflight requests
                # (push messages) tjhese need to be limited to avoid each
                # connection resulting in a hot read loop without any guaranteed
                # associated downstream consuming at the same rate.
                if not self._requests:
                    async with self._processing_budget:
                        self._data_received(data)
                else:
                    self._data_received(data)

    def _data_received(self, data: bytes) -> None:
        self._parser.feed(data)
        decode = self._requests[0].decode if self._requests else self._decode_responses
        response = self._parser.parse(
            decode=decode,
            encoding=self._requests[0].encoding if self._requests else self._encoding,
        )
        while not isinstance(response, NotEnoughData):
            if response[0] == DataType.PUSH:
                self._push_message_buffer_in.send_nowait(response[1])
            else:
                if self._requests:
                    request = self._requests.popleft()
                    if request.raise_exceptions and isinstance(response[1], RedisError):
                        request.fail(response[1])
                    else:
                        request.resolve(response[1])
            decode = self._requests[0].decode if self._requests else self._decode_responses
            response = self._parser.parse(
                decode=decode,
                encoding=self._requests[0].encoding if self._requests else self._encoding,
            )

    async def _writer_task(self) -> None:
        """
        Continually empty the buffer and send the data to the server.
        """
        while True:
            requests = await self._write_buffer_out.receive()
            while self._write_buffer_out.statistics().current_buffer_used > 0:
                requests.extend(self._write_buffer_out.receive_nowait())
                await checkpoint()
            data = b"".join(requests)
            try:
                await self.connection.send(data)
            except (ClosedResourceError, BrokenResourceError) as err:
                self._transport_failed = True
                raise ConnectionError("Connection lost while sending request") from err
            except Exception:
                self._transport_failed = True
                raise

    async def update_tracking_client(self, enabled: bool, client_id: int | None = None) -> bool:
        """
        Associate this connection to :paramref:`client_id` to
        relay any tracking notifications to.
        """
        try:
            params: list[RedisValueT] = (
                [b"ON", b"REDIRECT", client_id] if (enabled and client_id is not None) else [b"OFF"]
            )

            if await self.create_request(b"CLIENT TRACKING", *params, decode=False) != b"OK":
                raise ConnectionError("Unable to toggle client tracking")
            self.tracking_client_id = client_id
            return True
        except UnknownCommandError:  # noqa
            raise
        except Exception:  # noqa
            return False

    async def try_legacy_auth(self) -> None:
        if self._credential_provider:
            creds = await self._credential_provider.get_credentials()
            params = [creds.password]
            if isinstance(creds, UserPass):
                params.insert(0, creds.username)
        elif not self._password:
            return
        else:
            params = [self._password]
            if self._username:
                params.insert(0, self._username)
        await self.create_request(b"AUTH", *params, decode=False)

    async def perform_handshake(self) -> None:
        if not self._needs_handshake:
            return

        hello_command_args: list[int | str | bytes] = [3]
        if creds := (
            await self._credential_provider.get_credentials()
            if self._credential_provider
            else (
                await UserPassCredentialProvider(self._username, self._password).get_credentials()
                if (self._username or self._password)
                else None
            )
        ):
            hello_command_args.extend(
                [
                    "AUTH",
                    creds.username,
                    creds.password or b"",
                ]
            )
        try:
            hello_resp = await self.create_request(b"HELLO", *hello_command_args, decode=False)
            assert isinstance(hello_resp, (list, dict))
            resp3 = cast(dict[bytes, RedisValueT], hello_resp)
            assert resp3[b"proto"] == 3
            self.server_version = nativestr(resp3[b"version"])
            self.client_id = int(resp3[b"id"])
            if self.server_version >= "7.2":
                await self.create_request(
                    b"CLIENT SETINFO",
                    b"LIB-NAME",
                    b"coredis",
                )
                await self.create_request(
                    b"CLIENT SETINFO",
                    b"LIB-VER",
                    coredis.__version__,
                )
            self._needs_handshake = False
        except AuthenticationRequiredError:
            await self.try_legacy_auth()
            self.server_version = None
            self.client_id = None
        except UnknownCommandError:  # noqa
            raise ConnectionError(
                "Unable to use RESP3 due to missing `HELLO` implementation the server."
            )

    async def on_connect(self) -> None:
        await self.perform_handshake()

        if self._db:
            if await self.create_request(b"SELECT", self._db, decode=False) != b"OK":
                raise ConnectionError(f"Invalid Database {self._db}")

        if self.client_name is not None:
            if (
                await self.create_request(b"CLIENT SETNAME", self.client_name, decode=False)
                != b"OK"
            ):
                raise ConnectionError(f"Failed to set client name: {self.client_name}")

        if self._noevict:
            await self.create_request(b"CLIENT NO-EVICT", b"ON")

        if self._notouch:
            await self.create_request(b"CLIENT NO-TOUCH", b"ON")

        if self._noreply:
            await self.create_request(b"CLIENT REPLY", b"OFF", noreply=True)
            self._noreply_set = True

    @property
    async def push_messages(self) -> AsyncGenerator[list[ResponseType], None]:
        """
        Generator to retrieve RESP3 push type messages sent by the server.
        The generator will yield each message received in order until the
        connection is lost and will raise an :exc:`~coredis.exceptions.ConnectionError`
        """
        try:
            while True:
                yield await self._push_message_buffer_out.receive()
        except (EndOfStream, BrokenResourceError, ClosedResourceError) as err:
            raise ConnectionError("Connection lost while waiting for push messages") from err

    def send_command(
        self,
        command: bytes,
        *args: RedisValueT,
    ) -> None:
        """
        Queue a command to send to the server without
        associating a request to it. At the moment this is only useful in
        pubsub scenarios where commands such as ``SUBSCRIBE``, ``UNSUBSCRIBE``
        etc do not result in a response.
        """
        with self.ensure_connection():
            self._write_buffer_in.send_nowait(self._packer.pack_command(command, *args))

    def create_request(
        self,
        command: bytes,
        *args: RedisValueT,
        noreply: bool | None = None,
        decode: RedisValueT | None = None,
        encoding: str | None = None,
        raise_exceptions: bool = True,
        timeout: float | None = None,
        disconnect_on_cancellation: bool = False,
    ) -> Request:
        """
        Queue a command to send to the server and create an
        associated request that can awaited for the response.
        """
        with self.ensure_connection():
            cmd_list = []
            if noreply and not self._noreply:
                cmd_list = self._packer.pack_command(CommandName.CLIENT_REPLY, PureToken.SKIP)
            cmd_list.extend(self._packer.pack_command(command, *args))
            request_timeout: float | None = timeout or self._stream_timeout
            request = Request(
                proxy(self),
                command,
                bool(decode) if decode is not None else self._decode_responses,
                encoding or self._encoding,
                raise_exceptions,
                request_timeout,
                disconnect_on_cancellation,
            )
            # modifying buffer and requests should happen atomically
            if not (self._noreply_set or noreply):
                self._requests.append(request)
            else:
                request.resolve(None)
            self._write_buffer_in.send_nowait(cmd_list)
            return request

    def create_requests(
        self,
        commands: list[CommandInvocation],
        raise_exceptions: bool = True,
        timeout: float | None = None,
        disconnect_on_cancellation: bool = False,
    ) -> list[Request]:
        """
        Queue multiple commands to send to the server
        """
        with self.ensure_connection():
            request_timeout: float | None = timeout or self._stream_timeout
            requests = [
                Request(
                    proxy(self),
                    cmd.command,
                    bool(cmd.decode) if cmd.decode is not None else self._decode_responses,
                    cmd.encoding or self._encoding,
                    raise_exceptions,
                    request_timeout,
                    disconnect_on_cancellation,
                )
                for cmd in commands
            ]
            packed = self._packer.pack_commands([(cmd.command, *cmd.args) for cmd in commands])
            self._write_buffer_in.send_nowait(packed)
            self._requests.extend(requests)
            return requests


class Connection(BaseConnection):
    class Params(BaseConnectionParams):
        """
        :meta private:
        """

        host: NotRequired[str]
        port: NotRequired[int]

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        *,
        socket_keepalive: bool | None = None,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        **kwargs: Unpack[BaseConnectionParams],
    ):
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self._socket_keepalive = socket_keepalive
        self._socket_keepalive_options: dict[int, int | bytes] = socket_keepalive_options or {}

    async def _connect(self) -> ByteStream:
        with fail_after(self._connect_timeout):
            if self._ssl_context:
                connection: ByteStream = await connect_tcp(
                    self.host,
                    self.port,
                    tls=True,
                    ssl_context=self._ssl_context,
                    tls_standard_compatible=False,
                )
            else:
                connection = await connect_tcp(self.host, self.port)
            sock = connection.extra(SocketAttribute.raw_socket, default=None)
            if sock is not None:
                if self._socket_keepalive:  # TCP_KEEPALIVE
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    for k, v in self._socket_keepalive_options.items():
                        sock.setsockopt(socket.SOL_TCP, k, v)
            return connection

    def describe(self) -> str:
        return f"Connection<host={self.host},port={self.port},db={self._db}>"

    @property
    def location(self) -> str:
        return f"host={self.host},port={self.port}"


class UnixDomainSocketConnection(BaseConnection):
    class Params(BaseConnectionParams):
        """
        :meta private:
        """

        path: str

    def __init__(self, path: str = "", **kwargs: Unpack[BaseConnectionParams]) -> None:
        super().__init__(**kwargs)
        self.path = path

    async def _connect(self) -> ByteStream:
        with fail_after(self._connect_timeout):
            return await connect_unix(self.path)

    def describe(self) -> str:
        return f"UnixDomainSocketConnection<path={self.path},db={self._db}>"

    @property
    def location(self) -> str:
        return f"path={self.path}"


class ClusterConnection(Connection):
    "Manages TCP communication to and from a Redis server"

    node: ManagedNode

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        *,
        read_from_replicas: bool = False,
        **kwargs: Unpack[BaseConnectionParams],
    ) -> None:
        self.read_from_replicas = read_from_replicas
        super().__init__(
            host=host,
            port=port,
            **kwargs,
        )

        async def _on_connect(*_: Any) -> None:
            """
            Initialize the connection, authenticate and select a database and send
            `READONLY` if `read_from_replicas` is set during initialization.
            """

            if self.read_from_replicas:
                assert (await self.create_request(b"READONLY", decode=False)) == b"OK"

        self.register_connect_callback(_on_connect)

    def describe(self) -> str:
        return f"ClusterConnection<path={self.host},db={self.port}>"

    @property
    def location(self) -> str:
        return f"host={self.host},port={self.port}"
