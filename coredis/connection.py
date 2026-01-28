from __future__ import annotations

import dataclasses
import inspect
import math
import os
import socket
import ssl
from abc import abstractmethod
from collections import defaultdict, deque
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast
from weakref import ProxyType, proxy

from anyio import (
    TASK_STATUS_IGNORED,
    BrokenResourceError,
    CancelScope,
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
from typing_extensions import override

import coredis
from coredis._packer import Packer
from coredis._utils import logger, nativestr
from coredis.commands.constants import CommandName
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
    Awaitable,
    Callable,
    ClassVar,
    Generator,
    RedisValueT,
    ResponseType,
    TypeVar,
)

CERT_REQS = {
    "none": ssl.CERT_NONE,
    "optional": ssl.CERT_OPTIONAL,
    "required": ssl.CERT_REQUIRED,
}
R = TypeVar("R")

if TYPE_CHECKING:
    from coredis.pool.nodemanager import ManagedNode


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


class BaseConnection:
    """
    Base connection class which interacts with the underlying connection
    established with the redis server.
    """

    description: ClassVar[str] = "BaseConnection"
    locator: ClassVar[str] = ""

    def __init__(
        self,
        stream_timeout: float | None = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        *,
        client_name: str | None = None,
        noreply: bool = False,
        noevict: bool = False,
        notouch: bool = False,
        max_idle_time: int | None = None,
    ):
        self._stream_timeout = stream_timeout
        self.username: str | None = None
        self.password: str | None = ""
        self.credential_provider: AbstractCredentialProvider | None = None
        self.db: int | None = None
        self.pid: int = os.getpid()
        self._description_args: Callable[..., dict[str, str | int | None]] = lambda: dict()
        self._connect_callbacks: list[
            (Callable[[BaseConnection], Awaitable[None]] | Callable[[BaseConnection], None])
        ] = list()
        self.encoding = encoding
        self.decode_responses = decode_responses
        self.server_version: str | None = None
        self.client_name = client_name
        # id for this connection as returned by the redis server
        self.client_id: int | None = None
        # client id that the redis server should send any redirected notifications to
        self.tracking_client_id: int | None = None

        # maximum time to wait for data from the server before
        # closing the connection
        self.max_idle_time = max_idle_time

        self.noreply = noreply
        self.noreply_set = False
        self.noevict = noevict
        self.notouch = notouch

        self._task_group: TaskGroup | None = None
        # The actual connection to the server
        self._connection: ByteStream | None = None
        # Memory object stream that collects any unread push message types
        self._push_message_buffer_in, self._push_message_buffer_out = create_memory_object_stream[
            list[ResponseType]
        ](math.inf)
        # Memory object stream for buffering writes to the socket
        self._write_buffer_in, self._write_buffer_out = create_memory_object_stream[list[bytes]](
            math.inf
        )

        self._parser = Parser(self._push_message_buffer_in)
        self._packer: Packer = Packer(self.encoding)

        self._requests: deque[Request] = deque()
        self._connection_cancel_scope: CancelScope | None = None

        # whether the `HELLO` handshake needs to be performed
        self._needs_handshake = True

        # Error flags
        self._last_error: BaseException | None = None
        self._connected = False
        self._transport_failed = False

    def __repr__(self) -> str:
        return self.describe(self._description_args())

    @classmethod
    def describe(cls, description_args: dict[str, Any]) -> str:
        return cls.description.format_map(defaultdict(lambda: None, description_args))

    @property
    def location(self) -> str:
        return self.locator.format_map(defaultdict(lambda: None, self._description_args()))

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
    async def _connect(self) -> ByteStream: ...

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
                logger.info("Connection closed unexpectedly!")
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
            decode = self._requests[0].decode if self._requests else self.decode_responses
            # Try to parse a complete response from already-fed bytes
            response = self._parser.get_response(
                decode, self._requests[0].encoding if self._requests else self.encoding
            )
            if isinstance(response, NotEnoughData):
                # Need more bytes; read once, feed, and retry
                with fail_after(self.max_idle_time):
                    try:
                        data = await self.connection.receive()
                    except (EndOfStream, ClosedResourceError, BrokenResourceError) as err:
                        self._transport_failed = True
                        raise ConnectionError("Connection lost while receiving response") from err
                    except Exception:
                        self._transport_failed = True
                        raise
                    self._parser.feed(data)
                continue  # loop back and try parsing again

            # We have a full response for `head`; now pop and complete it
            if self._requests:
                request = self._requests.popleft()
                if request.raise_exceptions and isinstance(response, RedisError):
                    request.fail(response)
                else:
                    request.resolve(response)

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
        if self.credential_provider:
            creds = await self.credential_provider.get_credentials()
            params = [creds.password]
            if isinstance(creds, UserPass):
                params.insert(0, creds.username)
        elif not self.password:
            return
        else:
            params = [self.password]
            if self.username:
                params.insert(0, self.username)
        await self.create_request(b"AUTH", *params, decode=False)

    async def perform_handshake(self) -> None:
        if not self._needs_handshake:
            return

        hello_command_args: list[int | str | bytes] = [3]
        if creds := (
            await self.credential_provider.get_credentials()
            if self.credential_provider
            else (
                await UserPassCredentialProvider(self.username, self.password).get_credentials()
                if (self.username or self.password)
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
                self.create_request(
                    b"CLIENT SETINFO",
                    b"LIB-NAME",
                    b"coredis",
                )
                self.create_request(
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

        if self.db:
            if await self.create_request(b"SELECT", self.db, decode=False) != b"OK":
                raise ConnectionError(f"Invalid Database {self.db}")

        if self.client_name is not None:
            if (
                await self.create_request(b"CLIENT SETNAME", self.client_name, decode=False)
                != b"OK"
            ):
                raise ConnectionError(f"Failed to set client name: {self.client_name}")

        if self.noevict:
            await self.create_request(b"CLIENT NO-EVICT", b"ON")

        if self.notouch:
            await self.create_request(b"CLIENT NO-TOUCH", b"ON")

        if self.noreply:
            await self.create_request(b"CLIENT REPLY", b"OFF", noreply=True)
            self.noreply_set = True

    async def fetch_push_message(self, block: bool = False) -> list[ResponseType]:
        """
        Read the next pending response
        """
        if block:
            timeout = self._stream_timeout if not block else None
            with fail_after(timeout):
                try:
                    return await self._push_message_buffer_out.receive()
                except (EndOfStream, BrokenResourceError, ClosedResourceError) as err:
                    raise ConnectionError(
                        "Connection lost while waiting for push messages"
                    ) from err
        return self._push_message_buffer_out.receive_nowait()

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
        Queue a command to send to the server
        """
        with self.ensure_connection():
            cmd_list = []
            if noreply and not self.noreply:
                cmd_list = self._packer.pack_command(CommandName.CLIENT_REPLY, PureToken.SKIP)
            cmd_list.extend(self._packer.pack_command(command, *args))
            request_timeout: float | None = timeout or self._stream_timeout
            request = Request(
                proxy(self),
                command,
                bool(decode) if decode is not None else self.decode_responses,
                encoding or self.encoding,
                raise_exceptions,
                request_timeout,
                disconnect_on_cancellation,
            )
            # modifying buffer and requests should happen atomically
            if not (self.noreply_set or noreply):
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
                    bool(cmd.decode) if cmd.decode is not None else self.decode_responses,
                    cmd.encoding or self.encoding,
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
    description: ClassVar[str] = "Connection<host={host},port={port},db={db}>"
    locator: ClassVar[str] = "host={host},port={port}"

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        username: str | None = None,
        password: str | None = None,
        credential_provider: AbstractCredentialProvider | None = None,
        db: int | None = 0,
        stream_timeout: float | None = None,
        connect_timeout: float | None = None,
        ssl_context: ssl.SSLContext | None = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        socket_keepalive: bool | None = None,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        *,
        client_name: str | None = None,
        noreply: bool = False,
        noevict: bool = False,
        notouch: bool = False,
        max_idle_time: int | None = None,
    ):
        super().__init__(
            stream_timeout,
            encoding,
            decode_responses,
            client_name=client_name,
            noreply=noreply,
            noevict=noevict,
            notouch=notouch,
            max_idle_time=max_idle_time,
        )
        self.host = host
        self.port = port
        self.username: str | None = username
        self.password: str | None = password
        self.credential_provider: AbstractCredentialProvider | None = credential_provider
        self.db: int | None = db
        self.ssl_context = ssl_context
        self._connect_timeout = connect_timeout
        self._description_args: Callable[..., dict[str, str | int | None]] = lambda: {
            "host": self.host,
            "port": self.port,
            "db": self.db,
        }
        self.socket_keepalive = socket_keepalive
        self.socket_keepalive_options: dict[int, int | bytes] = socket_keepalive_options or {}

    @override
    async def _connect(self) -> ByteStream:
        with fail_after(self._connect_timeout):
            if self.ssl_context:
                connection: ByteStream = await connect_tcp(
                    self.host,
                    self.port,
                    tls=True,
                    ssl_context=self.ssl_context,
                    tls_standard_compatible=False,
                )
            else:
                connection = await connect_tcp(self.host, self.port)
            sock = connection.extra(SocketAttribute.raw_socket, default=None)
            if sock is not None:
                if self.socket_keepalive:  # TCP_KEEPALIVE
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    for k, v in self.socket_keepalive_options.items():
                        sock.setsockopt(socket.SOL_TCP, k, v)
            return connection


class UnixDomainSocketConnection(BaseConnection):
    description: ClassVar[str] = "UnixDomainSocketConnection<path={path},db={db}>"
    locator: ClassVar[str] = "path={path}"

    def __init__(
        self,
        path: str = "",
        username: str | None = None,
        password: str | None = None,
        credential_provider: AbstractCredentialProvider | None = None,
        db: int = 0,
        stream_timeout: float | None = None,
        connect_timeout: float | None = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        *,
        client_name: str | None = None,
        max_idle_time: int | None = None,
        **_: RedisValueT,
    ) -> None:
        super().__init__(
            stream_timeout,
            encoding,
            decode_responses,
            client_name=client_name,
            max_idle_time=max_idle_time,
        )
        self.path = path
        self.db = db
        self.username = username
        self.password = password
        self.credential_provider = credential_provider
        self._connect_timeout = connect_timeout
        self._description_args = lambda: {"path": self.path, "db": self.db}

    @override
    async def _connect(self) -> ByteStream:
        with fail_after(self._connect_timeout):
            return await connect_unix(self.path)


class ClusterConnection(Connection):
    "Manages TCP communication to and from a Redis server"

    description: ClassVar[str] = "ClusterConnection<host={host},port={port}>"
    locator: ClassVar[str] = "host={host},port={port}"
    node: ManagedNode

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        username: str | None = None,
        password: str | None = None,
        credential_provider: AbstractCredentialProvider | None = None,
        db: int | None = 0,
        stream_timeout: float | None = None,
        connect_timeout: float | None = None,
        ssl_context: ssl.SSLContext | None = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        socket_keepalive: bool | None = None,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        *,
        client_name: str | None = None,
        read_from_replicas: bool = False,
        noreply: bool = False,
        noevict: bool = False,
        notouch: bool = False,
        max_idle_time: int | None = None,
    ) -> None:
        self.read_from_replicas = read_from_replicas
        super().__init__(
            host=host,
            port=port,
            username=username,
            password=password,
            credential_provider=credential_provider,
            db=db,
            stream_timeout=stream_timeout,
            connect_timeout=connect_timeout,
            ssl_context=ssl_context,
            encoding=encoding,
            decode_responses=decode_responses,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            client_name=client_name,
            noreply=noreply,
            noevict=noevict,
            notouch=notouch,
            max_idle_time=max_idle_time,
        )

        async def _on_connect(*_: Any) -> None:
            """
            Initialize the connection, authenticate and select a database and send
            `READONLY` if `read_from_replicas` is set during initialization.
            """

            if self.read_from_replicas:
                assert (await self.create_request(b"READONLY", decode=False)) == b"OK"

        self.register_connect_callback(_on_connect)
