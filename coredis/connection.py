from __future__ import annotations

import asyncio
import dataclasses
import inspect
import os
import socket
import ssl
import time
import warnings
from collections import defaultdict, deque
from typing import TYPE_CHECKING, Any, cast

import async_timeout

from coredis._packer import Packer
from coredis._unpacker import NotEnoughData
from coredis._utils import nativestr
from coredis.exceptions import (
    AuthenticationRequiredError,
    ConnectionError,
    RedisError,
    TimeoutError,
    UnknownCommandError,
)
from coredis.parser import Parser
from coredis.tokens import PureToken
from coredis.typing import (
    Awaitable,
    Callable,
    ClassVar,
    Deque,
    Dict,
    List,
    Literal,
    Optional,
    ResponseType,
    Set,
    TypeVar,
    Union,
    ValueT,
)

R = TypeVar("R")

if TYPE_CHECKING:
    from coredis.pool.nodemanager import ManagedNode


@dataclasses.dataclass
class Request:
    command: bytes
    decode: Optional[bool] = None
    raise_exceptions: bool = True
    future: asyncio.Future[ResponseType] = dataclasses.field(
        default_factory=lambda: asyncio.get_running_loop().create_future()
    )

    def enforce_deadline(self) -> None:
        if not self.future.done():
            self.future.set_exception(TimeoutError())


class RedisSSLContext:
    context: Optional[ssl.SSLContext]

    def __init__(
        self,
        keyfile: Optional[str],
        certfile: Optional[str],
        cert_reqs: Optional[Union[str, ssl.VerifyMode]] = None,
        ca_certs: Optional[str] = None,
        check_hostname: Optional[bool] = None,
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
        self.ca_certs = ca_certs
        self.context = None

    def get(self) -> ssl.SSLContext:
        if self.keyfile is None:
            self.context = ssl.create_default_context(cafile=self.ca_certs)
        else:
            self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            self.context.verify_mode = self.cert_reqs
            self.context.check_hostname = self.check_hostname
            self.context.load_cert_chain(
                certfile=self.certfile, keyfile=self.keyfile  # type: ignore
            )
            if self.ca_certs:
                self.context.load_verify_locations(
                    **{
                        "capath"
                        if os.path.isdir(self.ca_certs)
                        else "cafile": self.ca_certs
                    }
                )
        assert self.context
        return self.context


class BaseConnection(asyncio.BaseProtocol):
    """
    Base connection class which implements
    :class:`asyncio.BaseProtocol` to interact
    with the underlying connection established
    with the redis server.
    """

    #: id for this connection as returned by the redis server
    client_id: Optional[int]
    #: Queue that collects any unread push message types
    push_messages: asyncio.Queue[ResponseType]
    #: client id that the redis server should send any redirected notifications to
    tracking_client_id: Optional[int]
    #: Whether the connection should use RESP or RESP3
    protocol_version: Literal[2, 3]

    description: ClassVar[str] = "BaseConnection"
    locator: ClassVar[str] = ""

    def __init__(
        self,
        retry_on_timeout: bool = False,
        stream_timeout: Optional[float] = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        *,
        client_name: Optional[str] = None,
        protocol_version: Literal[2, 3] = 3,
        noreply: bool = False,
    ):
        self._stream_timeout = stream_timeout
        self.username: Optional[str] = None
        self.password: Optional[str] = ""
        self.db: Optional[int] = None
        self.pid = os.getpid()
        self.retry_on_timeout = retry_on_timeout
        self._description_args: Callable[
            ..., Dict[str, Optional[Union[str, int]]]
        ] = lambda: dict()
        self._connect_callbacks: List[
            Union[
                Callable[[BaseConnection], Awaitable[None]],
                Callable[[BaseConnection], None],
            ]
        ] = list()
        self.encoding = encoding
        self.decode_responses = decode_responses
        self.protocol_version = protocol_version
        self.server_version: Optional[str] = None
        self.client_name = client_name
        self.client_id = None
        # flag to show if a connection is waiting for response
        self.last_active_at: float = time.time()
        self.last_request_processed: Optional[float] = None
        self.packer = Packer(self.encoding)
        self.push_messages: asyncio.Queue[ResponseType] = asyncio.Queue()
        self.tracking_client_id = None
        self.noreply = noreply
        self.noreply_set = False
        self.needs_handshake = True
        self._transport: Optional[asyncio.Transport] = None
        self._read_flag = asyncio.Event()
        self._last_error: Optional[BaseException] = None
        self._parser = Parser(encoding, decode_responses)
        self._requests: Deque[Request] = deque()

    def __repr__(self) -> str:
        return self.describe(self._description_args())

    @classmethod
    def describe(cls, description_args: Dict[str, Any]) -> str:
        return cls.description.format_map(defaultdict(lambda: None, description_args))

    @property
    def location(self) -> str:
        return self.locator.format_map(
            defaultdict(lambda: None, self._description_args())
        )

    def __del__(self) -> None:
        try:
            self.disconnect()
        except Exception:  # noqa
            pass

    @property
    def is_connected(self) -> bool:
        return self._transport is not None

    @property
    def requests_pending(self) -> int:
        return len(self._requests)

    @property
    def lag(self) -> float:
        if not self._requests:
            return 0
        elif self.last_request_processed is None:
            return time.time()
        else:
            return time.time() - self.last_request_processed

    def register_connect_callback(
        self,
        callback: Union[
            Callable[[BaseConnection], None],
            Callable[[BaseConnection], Awaitable[None]],
        ],
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
        try:
            await self._connect()
        except (asyncio.CancelledError, RedisError):
            raise
        except Exception as err:
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

    def connection_lost(self, exc: Optional[BaseException]) -> None:
        """
        :meta private:
        """
        if exc:
            self._last_error = exc

        self.disconnect()

    def pause_writing(self) -> None:  # noqa
        """
        :meta private:
        """

    def resume_writing(self) -> None:  # noqa
        """
        :meta private:
        """

    def data_received(self, data: bytes) -> None:
        """
        :meta private:
        """
        self._parser.unpacker.feed(data)
        self._read_flag.set()
        if not self._requests:
            return

        request = self._requests.popleft()
        response = self._parser.get_response(request.decode)
        while not isinstance(
            response,
            NotEnoughData,
        ):
            if request.raise_exceptions and isinstance(response, RedisError):
                request.future.set_exception(response)
            else:
                request.future.set_result(response)

            self.last_request_processed = time.time()

            try:
                request = self._requests.popleft()
            except IndexError:
                return

            response = self._parser.get_response(request.decode)

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

    async def update_tracking_client(
        self, enabled: bool, client_id: Optional[int] = None
    ) -> bool:
        """
        Associate this connection to :paramref:`client_id` to
        relay any tracking notifications to.
        """
        try:
            params: List[ValueT] = (
                [b"ON", b"REDIRECT", client_id]
                if (enabled and client_id is not None)
                else [b"OFF"]
            )

            if (
                await (
                    await self.create_request(b"CLIENT TRACKING", *params, decode=False)
                )
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
        if not self.password:
            return
        params = [self.password]
        if self.username:
            params.insert(0, self.username)
        await (await self.create_request(b"AUTH", *params, decode=False))

    async def perform_handshake(self) -> None:
        if not self.needs_handshake:
            return

        hello_command_args: List[Union[int, str, bytes]] = [self.protocol_version]
        if self.username or self.password:
            hello_command_args.extend(
                ["AUTH", self.username or b"default", self.password or b""]
            )
        try:
            hello_resp = await (
                await self.create_request(b"HELLO", *hello_command_args, decode=False)
            )
            assert isinstance(hello_resp, (list, dict))
            if self.protocol_version == 3:
                resp3 = cast(Dict[bytes, ValueT], hello_resp)
                assert resp3[b"proto"] == 3
                self.server_version = nativestr(resp3[b"version"])
                self.client_id = int(resp3[b"id"])
            else:
                resp = cast(List[ValueT], hello_resp)
                self.server_version = nativestr(resp[3])
                self.client_id = int(resp[7])
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
            if (
                await (await self.create_request(b"SELECT", self.db, decode=False))
                != b"OK"
            ):
                raise ConnectionError(f"Invalid Database {self.db}")

        if self.client_name is not None:
            if (
                await (
                    await self.create_request(
                        b"CLIENT SETNAME", self.client_name, decode=False
                    )
                )
                != b"OK"
            ):
                raise ConnectionError(f"Failed to set client name: {self.client_name}")
        if self.noreply:
            await (await self.create_request(b"CLIENT REPLY", b"OFF", noreply=True))
            self.noreply_set = True

        self.last_active_at = time.time()

    async def fetch_push_message(
        self,
        decode: Optional[ValueT] = None,
        push_message_types: Optional[Set[bytes]] = None,
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
            bool(decode) if decode is not None else None, push_message_types
        )
        while isinstance(
            message,
            NotEnoughData,
        ):
            self._read_flag.clear()
            try:
                async with async_timeout.timeout(self._stream_timeout):
                    await self._read_flag.wait()
            except asyncio.TimeoutError:
                raise TimeoutError
            message = self._parser.get_response(
                bool(decode) if decode is not None else None, push_message_types
            )
        self.last_request_processed = time.time()
        return message

    def _send_packed_command(self, command: List[bytes]) -> None:
        """
        Sends an already packed command to the Redis server
        """

        assert self._transport
        self._transport.writelines(command)

    async def send_command(
        self,
        command: bytes,
        *args: ValueT,
    ) -> None:
        """
        Send a command to the redis server
        """

        if not self.is_connected:
            await self.connect()

        self._send_packed_command(self.packer.pack_command(command, *args))

        self.last_active_at = time.time()

    async def create_request(
        self,
        command: bytes,
        *args: ValueT,
        noreply: Optional[bool] = None,
        decode: Optional[ValueT] = None,
        raise_exceptions: bool = True,
    ) -> asyncio.Future[ResponseType]:
        """
        Send a command to the redis server
        """
        from coredis.commands.constants import CommandName

        if not self.is_connected:
            await self.connect()

        cmd_list = []
        if self.is_connected and noreply and not self.noreply:
            cmd_list = self.packer.pack_command(
                CommandName.CLIENT_REPLY, PureToken.SKIP
            )
        cmd_list.extend(self.packer.pack_command(command, *args))
        self._send_packed_command(cmd_list)

        self.last_active_at = time.time()

        if not (self.noreply_set or noreply):
            request = Request(
                command,
                bool(decode) if decode is not None else None,
                raise_exceptions,
            )
            self._requests.append(request)
            if self._stream_timeout is not None:
                asyncio.get_running_loop().call_later(
                    self._stream_timeout, request.enforce_deadline
                )
            return request.future
        else:
            none: asyncio.Future[ResponseType] = asyncio.Future()
            none.set_result(None)
            return none

    def disconnect(self) -> None:
        """
        Disconnect from the Redis server
        """
        self.needs_handshake = True
        self.noreply_set = False
        self._parser.on_disconnect()
        if self._transport:
            try:
                self._transport.close()
            # Raised if event loop is already closed.
            except RuntimeError:  # noqa
                pass

        while True:
            try:
                request = self._requests.popleft()
                if not request.future.cancelled():
                    request.future.set_exception(
                        self._last_error or ConnectionError("Connection lost")
                    )
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
        username: Optional[str] = None,
        password: Optional[str] = None,
        db: Optional[int] = 0,
        retry_on_timeout: bool = False,
        stream_timeout: Optional[float] = None,
        connect_timeout: Optional[float] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        socket_keepalive: Optional[bool] = None,
        socket_keepalive_options: Optional[Dict[int, Union[int, bytes]]] = None,
        *,
        client_name: Optional[str] = None,
        protocol_version: Literal[2, 3] = 3,
        noreply: bool = False,
    ):
        super().__init__(
            retry_on_timeout,
            stream_timeout,
            encoding,
            decode_responses,
            client_name=client_name,
            protocol_version=protocol_version,
            noreply=noreply,
        )
        self.host = host
        self.port = port
        self.username: Optional[str] = username
        self.password: Optional[str] = password
        self.db: Optional[int] = db
        self.ssl_context = ssl_context
        self._connect_timeout = connect_timeout
        self._description_args: Callable[
            ..., Dict[str, Optional[Union[str, int]]]
        ] = lambda: {
            "host": self.host,
            "port": self.port,
            "db": self.db,
        }
        self.socket_keepalive = socket_keepalive
        self.socket_keepalive_options: Dict[int, Union[int, bytes]] = (
            socket_keepalive_options or {}
        )

    async def _connect(self) -> None:
        if self.ssl_context:
            connection = asyncio.get_running_loop().create_connection(
                lambda: self, host=self.host, port=self.port, ssl=self.ssl_context
            )
        else:
            connection = asyncio.get_running_loop().create_connection(
                lambda: self, host=self.host, port=self.port
            )

        async with async_timeout.timeout(self._connect_timeout):
            transport, _ = await connection

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
        username: Optional[str] = None,
        password: Optional[str] = None,
        db: int = 0,
        retry_on_timeout: bool = False,
        stream_timeout: Optional[float] = None,
        connect_timeout: Optional[float] = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        *,
        client_name: Optional[str] = None,
        protocol_version: Literal[2, 3] = 3,
        **_: ValueT,
    ) -> None:
        super().__init__(
            retry_on_timeout,
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
        self._connect_timeout = connect_timeout
        self._description_args = lambda: {"path": self.path, "db": self.db}

    async def _connect(self) -> None:
        async with async_timeout.timeout(self._connect_timeout):
            await asyncio.get_running_loop().create_unix_connection(
                lambda: self, path=self.path
            )

        await self.on_connect()


class ClusterConnection(Connection):
    "Manages TCP communication to and from a Redis server"
    description: ClassVar[str] = "ClusterConnection<host={host},port={port}>"
    locator: ClassVar[str] = "host={host},port={port}"
    node: "ManagedNode"

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        username: Optional[str] = None,
        password: Optional[str] = None,
        db: Optional[int] = 0,
        retry_on_timeout: bool = False,
        stream_timeout: Optional[float] = None,
        connect_timeout: Optional[float] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        socket_keepalive: Optional[bool] = None,
        socket_keepalive_options: Optional[Dict[int, Union[int, bytes]]] = None,
        *,
        client_name: Optional[str] = None,
        protocol_version: Literal[2, 3] = 3,
        read_from_replicas: bool = False,
        noreply: bool = False,
    ) -> None:
        self.read_from_replicas = read_from_replicas
        super().__init__(
            host=host,
            port=port,
            username=username,
            password=password,
            db=db,
            retry_on_timeout=retry_on_timeout,
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
        )

    async def on_connect(self) -> None:
        """
        Initialize the connection, authenticate and select a database and send
        `READONLY` if `read_from_replicas` is set during initialization.

        :meta private:
        """

        await super().on_connect()
        if self.read_from_replicas:
            if await (await self.create_request(b"READONLY", decode=False)) != b"OK":
                raise ConnectionError("READONLY command failed")
