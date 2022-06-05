from __future__ import annotations

import asyncio
import inspect
import os
import socket
import ssl
import sys
import time
import warnings
from asyncio import AbstractEventLoop, StreamReader, StreamWriter
from typing import cast

from coredis._packer import Packer
from coredis._utils import nativestr
from coredis.exceptions import (
    AuthenticationRequiredError,
    ConnectionError,
    RedisError,
    TimeoutError,
    UnknownCommandError,
)
from coredis.nodemanager import Node
from coredis.parsers import BaseParser, DefaultParser
from coredis.typing import (
    Awaitable,
    Callable,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    ResponseType,
    Set,
    Type,
    TypeVar,
    Union,
    ValueT,
)

R = TypeVar("R")


async def exec_with_timeout(
    coroutine: Awaitable[R],
    timeout: Optional[float] = None,
) -> R:
    try:
        return await asyncio.wait_for(coroutine, timeout)
    except asyncio.TimeoutError as exc:
        raise TimeoutError(exc)


class RedisSSLContext:
    context: Optional[ssl.SSLContext]

    def __init__(
        self,
        keyfile: Optional[str],
        certfile: Optional[str],
        cert_reqs: Optional[Union[str, ssl.VerifyMode]] = None,
        ca_certs: Optional[str] = None,
    ) -> None:
        self.keyfile = keyfile
        self.certfile = certfile

        if cert_reqs is None:
            self.cert_reqs = ssl.CERT_NONE
        elif isinstance(cert_reqs, str):
            CERT_REQS = {
                "none": ssl.CERT_NONE,
                "optional": ssl.CERT_OPTIONAL,
                "required": ssl.CERT_REQUIRED,
            }

            if cert_reqs not in CERT_REQS:
                raise RedisError(
                    "Invalid SSL Certificate Requirements Flag: %s" % cert_reqs
                )
            self.cert_reqs = CERT_REQS[cert_reqs]
        self.ca_certs = ca_certs
        self.context = None

    def get(self) -> ssl.SSLContext:
        if self.keyfile is None:
            self.context = ssl.create_default_context(cafile=self.ca_certs)
        else:
            self.context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            self.context.verify_mode = self.cert_reqs
            self.context.load_cert_chain(
                certfile=self.certfile, keyfile=self.keyfile  # type: ignore
            )
            self.context.load_verify_locations(self.ca_certs)
        assert self.context
        return self.context


class BaseConnection:
    client_id: Optional[str]
    description: ClassVar[str] = "BaseConnection"
    _reader: Optional[StreamReader]
    _writer: Optional[StreamWriter]
    protocol_version: Literal[2, 3]
    push_messages: asyncio.Queue[ResponseType]

    def __init__(
        self,
        retry_on_timeout: bool = False,
        stream_timeout: Optional[float] = None,
        parser_class: Type[BaseParser] = DefaultParser,
        reader_read_size: int = 65535,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        *,
        client_name: Optional[str] = None,
        loop: Optional[asyncio.events.AbstractEventLoop] = None,
        protocol_version: Literal[2, 3] = 2,
    ):
        self._parser: BaseParser = parser_class(reader_read_size)
        self._stream_timeout = stream_timeout
        self._reader = None
        self._writer = None
        self.username: Optional[str] = None
        self.password: Optional[str] = ""
        self.db: Optional[int] = None
        self.pid = os.getpid()
        self.retry_on_timeout = retry_on_timeout
        self._description_args: Dict[str, Optional[Union[str, int]]] = dict()
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
        self.loop = loop
        self.client_name = client_name
        self.client_id = None
        # flag to show if a connection is waiting for response
        self.awaiting_response = False
        self.last_active_at = time.time()
        self.packer = Packer(self.encoding)
        self.push_messages: asyncio.Queue[ResponseType] = asyncio.Queue()

    def __repr__(self) -> str:
        return self.description.format(**self._description_args)

    def __del__(self) -> None:
        try:
            self.disconnect()
        except Exception:
            pass

    @property
    def is_connected(self) -> bool:
        return bool(self._reader and self._writer)

    @property
    def reader(self) -> StreamReader:
        assert self._reader
        return self._reader

    @property
    def writer(self) -> StreamWriter:
        assert self._writer
        return self._writer

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
        try:
            await self._connect()
        except asyncio.CancelledError:
            raise
        except RedisError:
            raise
        except Exception:
            raise ConnectionError()
        # run any user callbacks. right now the only internal callback
        # is for pubsub channel/pattern resubscription

        for callback in self._connect_callbacks:
            task = callback(self)
            if inspect.isawaitable(task):
                await task

    async def _connect(self) -> None:
        raise NotImplementedError

    async def check_auth_response(self) -> None:
        response = await self.read_response()

        if nativestr(response) != "OK":
            raise ConnectionError(
                f"Failed to authenticate: username={self.username} & password={self.password}"
            )

    async def on_connect(self) -> None:
        self._parser.on_connect(self)
        hello_command_args: List[Union[int, str, bytes]] = [self.protocol_version]
        auth_attempted = False

        if self.username or self.password:
            hello_command_args.extend(
                ["AUTH", self.username or b"default", self.password or b""]
            )
            auth_attempted = True
        try:
            await self.send_command(b"HELLO", *hello_command_args)
            hello_resp = await self.read_response(decode=False)
            if isinstance(hello_resp, (list, dict)):
                if self.protocol_version == 3:
                    resp3 = cast(Dict[bytes, ValueT], hello_resp)
                    if not resp3[b"proto"] == 3:
                        raise ConnectionError(
                            f"Unexpected response when negotiating protocol: [{resp3}]"
                        )
                    self.server_version = nativestr(resp3[b"version"])
                    self.client_id = nativestr(resp3[b"id"])
                else:
                    resp = cast(List[ValueT], hello_resp)
                    self.server_version = nativestr(resp[3])
                    self.client_id = nativestr(resp[7])
            else:
                warnings.warn(
                    (
                        f"Unexpected response for `HELLO` command: {hello_resp!r}."
                        " Unable to determine server version and set protocol version."
                        " Assuming RESP v2"
                    ),
                    category=UserWarning,
                )
                self.server_version = None
                self.client_id = None
                if self.protocol_version == 3:
                    raise ConnectionError("Unable to set RESP3 protocol version")
        except (UnknownCommandError, AuthenticationRequiredError):
            if self.protocol_version == 3:
                raise ConnectionError("Unable to set RESP3 protocol version")
            self.server_version = None
            self.client_id = None
            auth_attempted = False

        if not auth_attempted and (self.username or self.password):
            if self.username and self.password:
                await self.send_command(b"AUTH", self.username, self.password)
                await self.check_auth_response()
            elif self.password:
                await self.send_command(b"AUTH", self.password)
                await self.check_auth_response()

        if self.db:
            await self.send_command(b"SELECT", self.db)

            if await self.read_response(decode=False) != b"OK":
                raise ConnectionError("Invalid Database")

        if self.client_name is not None:
            await self.send_command(b"CLIENT SETNAME", self.client_name)

            if await self.read_response(decode=False) != b"OK":
                raise ConnectionError(f"Failed to set client name: {self.client_name}")

        self.last_active_at = time.time()

    async def read_response(
        self,
        decode: Optional[bool] = None,
        push_message_types: Optional[Set[bytes]] = None,
    ) -> ResponseType:
        try:
            response = await exec_with_timeout(
                self._parser.read_response(
                    decode=decode, push_message_types=push_message_types
                ),
                self._stream_timeout,
            )
            self.last_active_at = time.time()
        except TimeoutError:
            self.disconnect()
            raise

        if isinstance(response, RedisError):
            raise response
        self.awaiting_response = False

        return response

    async def send_packed_command(self, command: List[bytes]) -> None:
        """Sends an already packed command to the Redis server"""

        if not self._writer:
            await self.connect()
            assert self._writer
        try:
            self._writer.writelines(command)
        except TimeoutError:
            self.disconnect()
            raise TimeoutError("Timeout writing to socket")
        except Exception:
            e = sys.exc_info()[1]
            self.disconnect()
            if e:
                if len(e.args) == 1:
                    errno, errmsg = "UNKNOWN", e.args[0]
                else:
                    errno = e.args[0]
                    errmsg = e.args[1]
                raise ConnectionError(
                    f"Error {errno} while writing to socket. {errmsg}."
                )
            else:
                raise

    async def send_command(self, command: bytes, *args: ValueT) -> None:
        if not self.is_connected:
            await self.connect()
        await self.send_packed_command(self.packer.pack_command(command, *args))
        self.awaiting_response = True
        self.last_active_at = time.time()

    def disconnect(self) -> None:
        """Disconnects from the Redis server"""
        self._parser.on_disconnect()
        try:
            if self._writer:
                self._writer.close()
        except Exception:
            pass
        self._reader = None
        self._writer = None


class Connection(BaseConnection):
    description: ClassVar[str] = "Connection<host={host},port={port},db={db}>"

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
        ssl_context: Optional[RedisSSLContext] = None,
        parser_class: Type[BaseParser] = DefaultParser,
        reader_read_size: int = 65535,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        socket_keepalive: Optional[bool] = None,
        socket_keepalive_options: Optional[Dict[int, Union[int, bytes]]] = None,
        *,
        client_name: Optional[str] = None,
        loop: Optional[AbstractEventLoop] = None,
        protocol_version: Literal[2, 3] = 2,
    ):
        super().__init__(
            retry_on_timeout,
            stream_timeout,
            parser_class,
            reader_read_size,
            encoding,
            decode_responses,
            client_name=client_name,
            loop=loop,
            protocol_version=protocol_version,
        )
        self.host = host
        self.port = port
        self.username: Optional[str] = username
        self.password: Optional[str] = password
        self.db: Optional[int] = db
        self.ssl_context = ssl_context
        self._connect_timeout = connect_timeout
        self._description_args: Dict[str, Optional[Union[str, int]]] = {
            "host": self.host,
            "port": self.port,
            "db": self.db,
        }
        self.socket_keepalive = socket_keepalive
        self.socket_keepalive_options: Dict[int, Union[int, bytes]] = (
            socket_keepalive_options or {}
        )

    async def _connect(self) -> None:
        connection = asyncio.open_connection(
            host=self.host, port=self.port, ssl=self.ssl_context
        )
        reader, writer = await exec_with_timeout(
            connection,
            self._connect_timeout,
        )
        self._reader = reader
        self._writer = writer
        sock = writer.transport.get_extra_info("socket")

        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            try:
                # TCP_KEEPALIVE

                if self.socket_keepalive:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

                    for k, v in self.socket_keepalive_options.items():
                        sock.setsockopt(socket.SOL_TCP, k, v)
            except (OSError, TypeError):
                # `socket_keepalive_options` might contain invalid options
                # causing an error. Do not leavo999e the connection open.
                writer.close()
                raise
        await self.on_connect()


class UnixDomainSocketConnection(BaseConnection):
    description: ClassVar[str] = "UnixDomainSocketConnection<path={path},db={db}>"

    def __init__(
        self,
        path: str = "",
        username: Optional[str] = None,
        password: Optional[str] = None,
        db: int = 0,
        retry_on_timeout: bool = False,
        stream_timeout: Optional[float] = None,
        connect_timeout: Optional[float] = None,
        ssl_context: Optional[RedisSSLContext] = None,
        parser_class: Type[BaseParser] = DefaultParser,
        reader_read_size: int = 65535,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        *,
        client_name: Optional[str] = None,
        loop: Optional[AbstractEventLoop] = None,
        protocol_version: Literal[2, 3] = 2,
    ) -> None:
        super().__init__(
            retry_on_timeout,
            stream_timeout,
            parser_class,
            reader_read_size,
            encoding,
            decode_responses,
            client_name=client_name,
            loop=loop,
            protocol_version=protocol_version,
        )
        self.path = path
        self.db = db
        self.username = username
        self.password = password
        self.ssl_context = ssl_context
        self._connect_timeout = connect_timeout
        self._description_args = {"path": self.path, "db": self.db}

    async def _connect(self) -> None:
        connection = asyncio.open_unix_connection(path=self.path, ssl=self.ssl_context)
        reader, writer = await exec_with_timeout(
            connection,
            self._connect_timeout,
        )
        self._reader = reader
        self._writer = writer
        await self.on_connect()


class ClusterConnection(Connection):
    "Manages TCP communication to and from a Redis server"
    description: ClassVar[str] = "ClusterConnection<host={host},port={port}>"
    node: Node

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
        ssl_context: Optional[RedisSSLContext] = None,
        parser_class: Type[BaseParser] = DefaultParser,
        reader_read_size: int = 65535,
        encoding: str = "utf-8",
        decode_responses: bool = False,
        socket_keepalive: Optional[bool] = None,
        socket_keepalive_options: Optional[Dict[int, Union[int, bytes]]] = None,
        *,
        client_name: Optional[str] = None,
        loop: Optional[AbstractEventLoop] = None,
        protocol_version: Literal[2, 3] = 2,
        readonly: bool = False,
    ) -> None:
        self.readonly = readonly
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
            parser_class=parser_class,
            reader_read_size=reader_read_size,
            encoding=encoding,
            decode_responses=decode_responses,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            client_name=client_name,
            loop=loop,
            protocol_version=protocol_version,
        )

    async def on_connect(self) -> None:
        """
        Initialize the connection, authenticate and select a database and send READONLY if it is
        set during object initialization.

        :meta private:
        """

        if self.db:
            warnings.warn("SELECT DB is not allowed in cluster mode")
            self.db = None
        await super().on_connect()

        if self.readonly:
            await self.send_command(b"READONLY")

            if await self.read_response(decode=False) != b"OK":
                raise ConnectionError("READONLY command failed")
