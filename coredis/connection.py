from __future__ import annotations

import asyncio
import dataclasses
import functools
import inspect
import itertools
import os
import socket
import ssl
import time
import warnings
import weakref
from collections import defaultdict, deque
from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

import async_timeout

import coredis
from coredis._packer import Packer
from coredis._utils import nativestr
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
    Literal,
    RedisValueT,
    ResponseType,
    TypeVar,
)

R = TypeVar("R")

if TYPE_CHECKING:
    from coredis.pool.nodemanager import ManagedNode


@dataclasses.dataclass
class Request:
    connection: weakref.ProxyType[Connection]
    command: bytes
    decode: bool
    encoding: str | None = None
    raise_exceptions: bool = True
    future: asyncio.Future[ResponseType] = dataclasses.field(
        default_factory=lambda: asyncio.get_running_loop().create_future()
    )
    created_at: float = dataclasses.field(default_factory=lambda: time.time())

    def __post_init__(self) -> None:
        self.future.add_done_callback(self.cleanup)

    def cleanup(self, future: asyncio.Future[ResponseType]) -> None:
        if future.cancelled() and self.connection and self.connection.is_connected:
            self.connection.disconnect()

    def enforce_deadline(self, timeout: float) -> None:
        if not self.future.done():
            self.future.set_exception(
                TimeoutError(f"command {nativestr(self.command)} timed out after {timeout} seconds")
            )


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


class BaseConnection(asyncio.BaseProtocol):
    """
    Base connection class which implements
    :class:`asyncio.BaseProtocol` to interact
    with the underlying connection established
    with the redis server.
    """

    #: id for this connection as returned by the redis server
    client_id: int | None
    #: Queue that collects any unread push message types
    push_messages: asyncio.Queue[ResponseType]
    #: client id that the redis server should send any redirected notifications to
    tracking_client_id: int | None
    #: Whether the connection should use RESP or RESP3
    protocol_version: Literal[2, 3]

    description: ClassVar[str] = "BaseConnection"
    locator: ClassVar[str] = ""

    #: average response time of requests made on this connection
    average_response_time: float

    def __init__(
        self,
        stream_timeout: float | None = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        *,
        client_name: str | None = None,
        protocol_version: Literal[2, 3] = 3,
        noreply: bool = False,
        noevict: bool = False,
        notouch: bool = False,
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
        self.protocol_version = protocol_version
        self.server_version: str | None = None
        self.client_name = client_name
        self.client_id = None
        self.tracking_client_id = None

        self.last_active_at: float = time.time()
        self.last_request_processed_at: float | None = None

        self._transport: asyncio.Transport | None = None
        self._parser = Parser()
        self._read_flag = asyncio.Event()
        self._read_waiters: set[asyncio.Task[bool]] = set()
        self.packer: Packer = Packer(self.encoding)
        self.push_messages: asyncio.Queue[ResponseType] = asyncio.Queue()

        self.noreply: bool = noreply
        self.noreply_set: bool = False

        self.noevict: bool = noevict
        self.notouch: bool = notouch

        self.needs_handshake: bool = True
        self._last_error: BaseException | None = None
        self._connection_error: BaseException | None = None

        self._requests: deque[Request] = deque()

        self.average_response_time: float = 0
        self.requests_processed: int = 0
        self._write_ready: asyncio.Event = asyncio.Event()
        self._transport_lock: asyncio.Lock = asyncio.Lock()

    def __repr__(self) -> str:
        return self.describe(self._description_args())

    @classmethod
    def describe(cls, description_args: dict[str, Any]) -> str:
        return cls.description.format_map(defaultdict(lambda: None, description_args))

    @property
    def location(self) -> str:
        return self.locator.format_map(defaultdict(lambda: None, self._description_args()))

    @property
    def estimated_time_to_idle(self) -> float:
        """
        Estimated time till the pending request queue of this connection
        has been cleared
        """
        return self.requests_pending * self.average_response_time

    def __del__(self) -> None:
        try:
            self.disconnect()
        except Exception:  # noqa
            pass

    @property
    def is_connected(self) -> bool:
        """
        Whether the connection is established and initial handshakes were
        performed without error
        """
        return self._transport is not None and self._connection_error is None

    @property
    def requests_pending(self) -> int:
        """
        Number of requests pending response on this connection
        """
        return len(self._requests)

    @property
    def lag(self) -> float:
        """
        Returns the amount of seconds since the last request was processed
        if there are still in flight requests pending on this connection
        """
        if not self._requests:
            return 0
        elif self.last_request_processed_at is None:
            return time.time()
        else:
            return time.time() - self.last_request_processed_at

    def register_connect_callback(
        self,
        callback: (Callable[[BaseConnection], None] | Callable[[BaseConnection], Awaitable[None]]),
    ) -> None:
        self._connect_callbacks.append(callback)

    def clear_connect_callbacks(self) -> None:
        self._connect_callbacks = list()

    async def can_read(self) -> bool:
        """Checks for data that can be read"""
        assert self._parser

        if not self.is_connected:
            await self.connect()

        return self._parser.can_read()

    async def connect(self) -> None:
        """
        Establish a connnection to the redis server
        and initiate any post connect callbacks
        """
        self._connection_error = None
        try:
            await self._connect()
        except (asyncio.CancelledError, RedisError) as err:
            self._connection_error = err
            raise
        except Exception as err:
            self._connection_error = err
            raise ConnectionError(str(err)) from err

        # run any user callbacks. right now the only internal callback
        # is for pubsub channel/pattern resubscription
        for callback in self._connect_callbacks:
            task = callback(self)
            if inspect.isawaitable(task):
                await task

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """
        :meta private:
        """
        self._transport = cast(asyncio.Transport, transport)
        self._write_ready.set()

    def connection_lost(self, exc: BaseException | None) -> None:
        """
        :meta private:
        """
        if exc:
            self._last_error = exc

        self.disconnect()

    def pause_writing(self) -> None:
        """
        :meta private:
        """
        self._write_ready.clear()

    def resume_writing(self) -> None:
        """
        :meta private:
        """
        self._write_ready.set()

    def data_received(self, data: bytes) -> None:
        """
        :meta private:
        """
        self._parser.feed(data)
        self._read_flag.set()
        if not self._requests:
            return

        request = self._requests.popleft()
        response = self._parser.get_response(request.decode, request.encoding)
        while not isinstance(
            response,
            NotEnoughData,
        ):
            if not (request.future.cancelled() or request.future.done()):
                if request.raise_exceptions and isinstance(response, RedisError):
                    request.future.set_exception(response)
                else:
                    request.future.set_result(response)

            self.last_request_processed_at = time.time()
            self.requests_processed += 1
            response_time = time.time() - request.created_at

            self.average_response_time = (
                (self.average_response_time * (self.requests_processed - 1)) + response_time
            ) / self.requests_processed

            try:
                request = self._requests.popleft()
            except IndexError:
                return

            response = self._parser.get_response(request.decode, request.encoding)

        # In case the first request pulled from the queue doesn't have enough data
        # to process, put it back to the start of the queue for the next iteration
        if request:
            self._requests.appendleft(request)

    def eof_received(self) -> None:
        """
        :meta private:
        """
        self.disconnect()

    async def _connect(self) -> None:
        raise NotImplementedError

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

        hello_command_args: list[int | str | bytes] = [self.protocol_version]
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
            if self.protocol_version == 3:
                resp3 = cast(dict[bytes, RedisValueT], hello_resp)
                assert resp3[b"proto"] == 3
                self.server_version = nativestr(resp3[b"version"])
                self.client_id = int(resp3[b"id"])
            else:
                resp = cast(list[RedisValueT], hello_resp)
                self.server_version = nativestr(resp[3])
                self.client_id = int(resp[7])
            if self.server_version >= "7.2":
                await asyncio.gather(
                    await self.create_request(
                        b"CLIENT SETINFO",
                        b"LIB-NAME",
                        b"coredis",
                    ),
                    await self.create_request(
                        b"CLIENT SETINFO",
                        b"LIB-VER",
                        coredis.__version__,
                    ),
                )
            self.needs_handshake = False
        except AuthenticationRequiredError:
            await self.try_legacy_auth()
            self.server_version = None
            self.client_id = None
        except UnknownCommandError:  # noqa
            # This should only happen for redis servers < 6 or forks of redis
            # that are not > 6 compliant.
            warning = (
                "The server responded with no support for the `HELLO` command"
                " and therefore a handshake could not be performed"
            )
            if self.protocol_version == 3:
                raise ConnectionError(
                    "Unable to use RESP3 due to missing `HELLO` implementation "
                    "the server. Use `protocol_version=2` when constructing the client."
                )
            else:
                warnings.warn(warning, category=UserWarning)
                await self.try_legacy_auth()
            self.needs_handshake = False

    async def on_connect(self) -> None:
        self._parser.on_connect(self)
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

        self.last_active_at = time.time()

    async def fetch_push_message(
        self,
        decode: RedisValueT | None = None,
        push_message_types: set[bytes] | None = None,
        block: bool | None = False,
    ) -> ResponseType:
        """
        Read the next pending response
        """
        if not self.is_connected:
            await self.connect()

        if len(self._requests) > 0:
            raise ConnectionError(
                f"Invalid request for push messages. {len(self._requests)} requests still pending"
            )

        message = self._parser.get_response(
            bool(decode) if decode is not None else self.decode_responses,
            self.encoding,
            push_message_types,
        )
        while isinstance(
            message,
            NotEnoughData,
        ):
            self._read_flag.clear()
            try:
                timeout = self._stream_timeout if not block else None
                read_ready_task = asyncio.create_task(self._read_flag.wait())
                read_ready_task.add_done_callback(
                    lambda _: self._read_waiters.discard(read_ready_task)
                )
                self._read_waiters.add(read_ready_task)
                await asyncio.wait_for(read_ready_task, timeout)
            except asyncio.TimeoutError:
                raise TimeoutError
            except asyncio.CancelledError:
                if not self.is_connected:
                    raise ConnectionError("Connection lost")
                raise
            message = self._parser.get_response(
                bool(decode) if decode is not None else self.decode_responses,
                self.encoding,
                push_message_types,
            )
        return message

    async def _send_packed_command(
        self, command: list[bytes], timeout: float | None = None
    ) -> None:
        """
        Sends an already packed command to the Redis server
        """

        assert self._transport
        try:
            async with async_timeout.timeout(timeout):
                await self._write_ready.wait()
        except asyncio.TimeoutError:
            if self._transport:
                self.disconnect()
            raise TimeoutError(f"Unable to write after waiting for socket for {timeout} seconds")
        self._transport.writelines(command)

    async def send_command(
        self,
        command: bytes,
        *args: RedisValueT,
    ) -> None:
        """
        Send a command to the redis server
        """

        if not self.is_connected:
            await self.connect()

        await self._send_packed_command(self.packer.pack_command(command, *args))

        self.last_active_at = time.time()

    async def create_request(
        self,
        command: bytes,
        *args: RedisValueT,
        noreply: bool | None = None,
        decode: RedisValueT | None = None,
        encoding: str | None = None,
        raise_exceptions: bool = True,
        timeout: float | None = None,
    ) -> asyncio.Future[ResponseType]:
        """
        Send a command to the redis server
        """
        from coredis.commands.constants import CommandName

        if not self.is_connected:
            await self.connect()

        cmd_list = []
        request_timeout: float | None = timeout or self._stream_timeout
        if self.is_connected and noreply and not self.noreply:
            cmd_list = self.packer.pack_command(CommandName.CLIENT_REPLY, PureToken.SKIP)
        cmd_list.extend(self.packer.pack_command(command, *args))
        await self._send_packed_command(cmd_list, timeout=request_timeout)

        self.last_active_at = time.time()

        if not (self.noreply_set or noreply):
            request = Request(
                weakref.proxy(self),
                command,
                bool(decode) if decode is not None else self.decode_responses,
                encoding or self.encoding,
                raise_exceptions,
            )
            self._requests.append(request)
            if request_timeout is not None:
                asyncio.get_running_loop().call_later(
                    request_timeout,
                    functools.partial(
                        request.enforce_deadline,
                        request_timeout,
                    ),
                )
            return request.future
        else:
            none: asyncio.Future[ResponseType] = asyncio.Future()
            none.set_result(None)
            return none

    async def create_requests(
        self,
        commands: list[CommandInvocation],
        raise_exceptions: bool = True,
        timeout: float | None = None,
    ) -> list[asyncio.Future[ResponseType]]:
        """
        Send multiple commands to the redis server
        """

        if not self.is_connected:
            await self.connect()

        request_timeout: float | None = timeout or self._stream_timeout

        await self._send_packed_command(
            self.packer.pack_commands(
                list(itertools.chain((cmd.command, *cmd.args) for cmd in commands))
            ),
            timeout=request_timeout,
        )

        self.last_active_at = time.time()
        requests: list[asyncio.Future[ResponseType]] = []
        for cmd in commands:
            request = Request(
                weakref.proxy(self),
                cmd.command,
                bool(cmd.decode) if cmd.decode is not None else self.decode_responses,
                cmd.encoding or self.encoding,
                raise_exceptions,
            )
            self._requests.append(request)
            if request_timeout is not None:
                asyncio.get_running_loop().call_later(
                    request_timeout,
                    functools.partial(request.enforce_deadline, request_timeout),
                )
            requests.append(request.future)
        return requests

    def disconnect(self) -> None:
        """
        Disconnect from the Redis server
        """
        self.needs_handshake = True
        self.noreply_set = False
        self._parser.on_disconnect()
        if self._transport:
            with suppress(RuntimeError):
                self._transport.close()

        disconnect_exc = self._last_error or ConnectionError("connection lost")
        while self._read_waiters:
            waiter = self._read_waiters.pop()
            if not waiter.done():
                with suppress(RuntimeError):
                    waiter.cancel()
        while True:
            try:
                request = self._requests.popleft()
                if not request.future.done():
                    request.future.set_exception(disconnect_exc)
            except IndexError:
                break
        self._transport = None


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
        protocol_version: Literal[2, 3] = 3,
        noreply: bool = False,
        noevict: bool = False,
        notouch: bool = False,
    ):
        super().__init__(
            stream_timeout,
            encoding,
            decode_responses,
            client_name=client_name,
            protocol_version=protocol_version,
            noreply=noreply,
            noevict=noevict,
            notouch=notouch,
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

    async def _connect(self) -> None:
        async with self._transport_lock:
            if self._transport:
                return
            if self.ssl_context:
                connection = asyncio.get_running_loop().create_connection(
                    lambda: self, host=self.host, port=self.port, ssl=self.ssl_context
                )
            else:
                connection = asyncio.get_running_loop().create_connection(
                    lambda: self, host=self.host, port=self.port
                )

            try:
                async with async_timeout.timeout(self._connect_timeout):
                    transport, _ = await connection
            except asyncio.TimeoutError:
                raise ConnectionError(
                    f"Unable to establish a connection within {self._connect_timeout} seconds"
                )
            sock = transport.get_extra_info("socket")
            if sock is not None:
                try:
                    # TCP_KEEPALIVE
                    if self.socket_keepalive:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

                        for k, v in self.socket_keepalive_options.items():
                            sock.setsockopt(socket.SOL_TCP, k, v)
                except (OSError, TypeError):
                    # `socket_keepalive_options` might contain invalid options
                    # causing an error
                    transport.close()
                    raise
            await self.on_connect()


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
        protocol_version: Literal[2, 3] = 3,
        **_: RedisValueT,
    ) -> None:
        super().__init__(
            stream_timeout,
            encoding,
            decode_responses,
            client_name=client_name,
            protocol_version=protocol_version,
        )
        self.path = path
        self.db = db
        self.username = username
        self.password = password
        self.credential_provider = credential_provider
        self._connect_timeout = connect_timeout
        self._description_args = lambda: {"path": self.path, "db": self.db}

    async def _connect(self) -> None:
        async with async_timeout.timeout(self._connect_timeout):
            await asyncio.get_running_loop().create_unix_connection(lambda: self, path=self.path)

        await self.on_connect()


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
        protocol_version: Literal[2, 3] = 3,
        read_from_replicas: bool = False,
        noreply: bool = False,
        noevict: bool = False,
        notouch: bool = False,
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
            protocol_version=protocol_version,
            noreply=noreply,
            noevict=noevict,
            notouch=notouch,
        )

    async def on_connect(self) -> None:
        """
        Initialize the connection, authenticate and select a database and send
        `READONLY` if `read_from_replicas` is set during initialization.

        :meta private:
        """

        await super().on_connect()
        if self.read_from_replicas:
            assert (await (await self.create_request(b"READONLY", decode=False))) == b"OK"
