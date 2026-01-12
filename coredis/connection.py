from __future__ import annotations

import dataclasses
import inspect
import math
import os
import socket
import ssl
from abc import abstractmethod
from collections import defaultdict, deque
from typing import TYPE_CHECKING, Any, Generator, cast

from anyio import (
    TASK_STATUS_IGNORED,
    ClosedResourceError,
    Event,
    Lock,
    connect_tcp,
    connect_unix,
    create_memory_object_stream,
    create_task_group,
    fail_after,
    move_on_after,
)
from anyio.abc import ByteStream, SocketAttribute, TaskStatus
from anyio.streams.tls import TLSStream
from typing_extensions import override

import coredis
from coredis._packer import Packer
from coredis._utils import logger, nativestr
from coredis.credentials import (
    AbstractCredentialProvider,
    UserPass,
    UserPassCredentialProvider,
)
from coredis.exceptions import (
    AuthenticationRequiredError,
    ConnectionError,
    RedisError,
    TimeoutError,
    UnknownCommandError,
)
from coredis.parser import NotEnoughData, Parser
from coredis.tokens import PureToken
from coredis.typing import (
    Awaitable,
    Callable,
    ClassVar,
    RedisValueT,
    ResponseType,
    TypeVar,
)

R = TypeVar("R")

if TYPE_CHECKING:
    from coredis.pool.nodemanager import ManagedNode


@dataclasses.dataclass
class Request:
    command: bytes
    decode: bool
    encoding: str | None = None
    raise_exceptions: bool = True
    response_timeout: float | None = None
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
        # return now if response available
        if self._event.is_set():
            return self._result_or_exc()
        # add response timeout
        with move_on_after(self.response_timeout) as scope:
            await self._event.wait()
        if scope.cancelled_caught and not self._event.is_set():
            self._exc = TimeoutError(
                f"command {nativestr(self.command)} timed out after {self.response_timeout} seconds"
            )
        return self._result_or_exc()

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
            CERT_REQS = {
                "none": ssl.CERT_NONE,
                "optional": ssl.CERT_OPTIONAL,
                "required": ssl.CERT_REQUIRED,
            }

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
        #: id for this connection as returned by the redis server
        self.client_id: int | None = None
        #: client id that the redis server should send any redirected notifications to
        self.tracking_client_id: int | None = None

        self._connection: ByteStream | None = None
        #: Queue that collects any unread push message types
        push_messages, self._receive_messages = create_memory_object_stream[list[ResponseType]](
            math.inf
        )
        self._parser = Parser(push_messages)
        self.packer: Packer = Packer(self.encoding)
        self.max_idle_time = max_idle_time

        self.noreply = noreply
        self.noreply_set = False

        self.noevict = noevict
        self.notouch = notouch

        self.needs_handshake = True
        self._last_error: BaseException | None = None
        self._connected = False

        self._requests: deque[Request] = deque()
        self._write_lock = Lock()

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
            raise ConnectionError("Connection not initialized correctly!")
        return self._connection

    @property
    def is_connected(self) -> bool:
        """
        Whether the connection is established and initial handshakes were
        performed without error
        """
        return self._connected

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
        Establish a connnection to the redis server
        and initiate any post connect callbacks.
        """

        self._connection = await self._connect()
        try:
            async with self.connection, self._parser.push_messages, create_task_group() as tg:
                tg.start_soon(self.listen_for_responses)
                # setup connection
                await self.on_connect()
                # run any user callbacks. right now the only internal callback
                # is for pubsub channel/pattern resubscription
                for callback in self._connect_callbacks:
                    task = callback(self)
                    if inspect.isawaitable(task):
                        await task
                self._connected = True
                task_status.started()
        except Exception as e:
            logger.exception("Connection closed unexpectedly!")
            self._last_error = e
            # swallow the error unless connection hasn't been established;
            # it will usually be raised when accessing command results.
            # we want the connection to die, but we don't always want to
            # raise it and corrupt the connection pool.
            if not self._connected:
                raise
        finally:
            self._parser.on_disconnect()
            disconnect_exc = self._last_error or ConnectionError("Connection lost!")
            while self._requests:
                request = self._requests.popleft()
                request.fail(disconnect_exc)
            self._connection = None

    async def listen_for_responses(self) -> None:
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
                with move_on_after(self.max_idle_time) as scope:
                    data = await self.connection.receive()
                    self._parser.feed(data)
                if scope.cancelled_caught:  # this will cleanup the connection gracefully
                    break
                continue  # loop back and try parsing again

            # We have a full response for `head`; now pop and complete it
            if self._requests:
                request = self._requests.popleft()
                if request.raise_exceptions and isinstance(response, RedisError):
                    request.fail(response)
                else:
                    request.resolve(response)

    async def update_tracking_client(self, enabled: bool, client_id: int | None = None) -> bool:
        """
        Associate this connection to :paramref:`client_id` to
        relay any tracking notifications to.
        """
        try:
            params: list[RedisValueT] = (
                [b"ON", b"REDIRECT", client_id] if (enabled and client_id is not None) else [b"OFF"]
            )

            if (
                await (await self.create_request(b"CLIENT TRACKING", *params, decode=False))
                != b"OK"
            ):
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
        await (await self.create_request(b"AUTH", *params, decode=False))

    async def perform_handshake(self) -> None:
        if not self.needs_handshake:
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
            hello_resp = await (
                await self.create_request(b"HELLO", *hello_command_args, decode=False)
            )
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
            self.needs_handshake = False
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
            if await (await self.create_request(b"SELECT", self.db, decode=False)) != b"OK":
                raise ConnectionError(f"Invalid Database {self.db}")

        if self.client_name is not None:
            if (
                await (await self.create_request(b"CLIENT SETNAME", self.client_name, decode=False))
                != b"OK"
            ):
                raise ConnectionError(f"Failed to set client name: {self.client_name}")

        if self.noevict:
            await (await self.create_request(b"CLIENT NO-EVICT", b"ON"))

        if self.notouch:
            await (await self.create_request(b"CLIENT NO-TOUCH", b"ON"))

        if self.noreply:
            await (await self.create_request(b"CLIENT REPLY", b"OFF", noreply=True))
            self.noreply_set = True

    async def fetch_push_message(self, block: bool = False) -> list[ResponseType]:
        """
        Read the next pending response
        """
        if block:
            timeout = self._stream_timeout if not block else None
            with fail_after(timeout):
                return await self._receive_messages.receive()

        return self._receive_messages.receive_nowait()

    async def _send_packed_command(
        self, command: list[bytes], timeout: float | None = None
    ) -> None:
        """
        Sends an already packed command to the Redis server
        """
        with fail_after(timeout):
            data = b"".join(command)
            try:
                await self.connection.send(data)
            except ClosedResourceError as err:
                self._last_error = err
                self._connection = None
                raise ConnectionError(f"Failed to send data: {data.decode()}!") from err

    async def send_command(
        self,
        command: bytes,
        *args: RedisValueT,
    ) -> None:
        """
        Send a command to the redis server
        """
        async with self._write_lock:
            await self._send_packed_command(self.packer.pack_command(command, *args))

    async def create_request(
        self,
        command: bytes,
        *args: RedisValueT,
        noreply: bool | None = None,
        decode: RedisValueT | None = None,
        encoding: str | None = None,
        raise_exceptions: bool = True,
        timeout: float | None = None,
    ) -> Request:
        """
        Send a command to the redis server
        """
        from coredis.commands.constants import CommandName

        cmd_list = []
        if self.is_connected and noreply and not self.noreply:
            cmd_list = self.packer.pack_command(CommandName.CLIENT_REPLY, PureToken.SKIP)
        cmd_list.extend(self.packer.pack_command(command, *args))
        request_timeout: float | None = timeout or self._stream_timeout
        request = Request(
            command,
            bool(decode) if decode is not None else self.decode_responses,
            encoding or self.encoding,
            raise_exceptions,
            request_timeout,
        )
        async with self._write_lock:
            if not (self.noreply_set or noreply):
                self._requests.append(request)
            else:
                request.resolve(None)
            await self._send_packed_command(cmd_list, timeout=request_timeout)
        return request

    async def create_requests(
        self,
        commands: list[CommandInvocation],
        raise_exceptions: bool = True,
        timeout: float | None = None,
    ) -> list[Request]:
        """
        Send multiple commands to the redis server
        """
        request_timeout: float | None = timeout or self._stream_timeout
        requests = [
            Request(
                cmd.command,
                bool(cmd.decode) if cmd.decode is not None else self.decode_responses,
                cmd.encoding or self.encoding,
                raise_exceptions,
                request_timeout,
            )
            for cmd in commands
        ]
        packed = self.packer.pack_commands([(cmd.command, *cmd.args) for cmd in commands])
        async with self._write_lock:
            self._requests.extend(requests)
            await self._send_packed_command(packed, timeout=request_timeout)
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
            connection: ByteStream = await connect_tcp(self.host, self.port)
            if self.ssl_context:
                connection = await TLSStream.wrap(
                    connection,
                    ssl_context=self.ssl_context,
                    standard_compatible=False,
                    server_side=False,
                )
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

        async def _on_connect(*args: Any) -> None:
            """
            Initialize the connection, authenticate and select a database and send
            `READONLY` if `read_from_replicas` is set during initialization.
            """

            if self.read_from_replicas:
                assert (await (await self.create_request(b"READONLY", decode=False))) == b"OK"

        self.register_connect_callback(_on_connect)
