from __future__ import annotations

import asyncio
import inspect
import os
import socket
import ssl
import sys
import time
import warnings
from typing import Callable, Dict, List, Literal, Optional, Type, Union

from coredis.constants import SYM_CRLF, SYM_DOLLAR, SYM_EMPTY, SYM_STAR
from coredis.exceptions import (
    AuthenticationRequiredError,
    ConnectionError,
    RedisError,
    TimeoutError,
    UnknownCommandError,
)
from coredis.parsers import (  # noqa: For backward compatibility
    BaseParser,
    DefaultParser,
    HiredisParser,
    PythonParser,
)
from coredis.typing import ValueT
from coredis.utils import b, nativestr


async def exec_with_timeout(coroutine, timeout, *, loop=None):
    try:
        return await asyncio.wait_for(coroutine, timeout)
    except asyncio.TimeoutError as exc:
        raise TimeoutError(exc)


class RedisSSLContext:
    def __init__(self, keyfile=None, certfile=None, cert_reqs=None, ca_certs=None):
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

    def get(self):
        if not self.keyfile:
            self.context = ssl.create_default_context(cafile=self.ca_certs)
        else:
            self.context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            self.context.verify_mode = self.cert_reqs
            self.context.load_cert_chain(certfile=self.certfile, keyfile=self.keyfile)
            self.context.load_verify_locations(self.ca_certs)

        return self.context


class BaseConnection:
    description = "BaseConnection"

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
        self.username = None
        self.password = ""
        self.db = ""
        self.pid = os.getpid()
        self.retry_on_timeout = retry_on_timeout
        self._description_args: Dict[str, Union[str, bytes, int, float]] = dict()
        self._connect_callbacks: List[Callable] = list()
        self.encoding = encoding
        self.decode_responses = decode_responses
        self.protocol_version = protocol_version
        self.server_version: Optional[str] = None
        self.loop = loop
        self.client_name = client_name
        # flag to show if a connection is waiting for response
        self.awaiting_response = False
        self.last_active_at = time.time()

    def __repr__(self):
        return self.description.format(**self._description_args)

    def __del__(self):
        try:
            self.disconnect()
        except Exception:
            pass

    @property
    def is_connected(self):
        return bool(self._reader and self._writer)

    def register_connect_callback(self, callback):
        self._connect_callbacks.append(callback)

    def clear_connect_callbacks(self):
        self._connect_callbacks = list()

    async def can_read(self):
        """Checks for data that can be read"""
        assert self._parser

        if not self.is_connected:
            await self.connect()

        return self._parser.can_read()

    async def connect(self):
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

    async def _connect(self):
        raise NotImplementedError

    async def check_auth_response(self):
        response = await self.read_response()

        if nativestr(response) != "OK":
            raise ConnectionError(
                f"Failed to authenticate: username={self.username} & password={self.password}"
            )

    async def on_connect(self):
        self._parser.on_connect(self)
        hello_command_args: List[Union[int, str, bytes]] = [self.protocol_version]
        auth_attempted = False
        if self.username or self.password:
            hello_command_args.extend(
                ["AUTH", self.username or b"default", self.password]
            )
            auth_attempted = True
        await self.send_command(b"HELLO", *hello_command_args)
        try:
            resp = await self.read_response(decode=False)
            if self.protocol_version == 3:
                if not resp[b"proto"] == 3:
                    raise ConnectionError(
                        f"Unexpected response when negotiating protocol: [{resp}]"
                    )
                self.server_version = nativestr(resp[b"version"])
            else:
                self.server_version = nativestr(resp[3])
        except (UnknownCommandError, AuthenticationRequiredError):
            self.version = None
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

    async def read_response(self, decode: Optional[bool] = None):
        try:
            response = await exec_with_timeout(
                self._parser.read_response(decode=decode),
                self._stream_timeout,
                loop=self.loop,
            )
            self.last_active_at = time.time()
        except TimeoutError:
            self.disconnect()
            raise

        if isinstance(response, RedisError):
            raise response
        self.awaiting_response = False

        return response

    async def send_packed_command(self, command):
        """Sends an already packed command to the Redis server"""

        if not self._writer:
            await self.connect()
            assert self._writer
        try:
            if isinstance(command, str):
                command = [command]
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

    async def send_command(self, *args: ValueT):
        if not self.is_connected:
            await self.connect()
        await self.send_packed_command(self.pack_command(*args))
        self.awaiting_response = True
        self.last_active_at = time.time()

    def encode(self, value):
        """Returns a bytestring representation of the value"""
        if isinstance(value, bytes):
            return value
        elif isinstance(value, int):
            return b"%d" % value
        elif isinstance(value, float):
            return b"%.15g" % value
        elif not isinstance(value, str):
            value = f"{value}"
        if isinstance(value, str):
            value = value.encode(self.encoding)

        return value

    def disconnect(self):
        """Disconnects from the Redis server"""
        self._parser.on_disconnect()
        try:
            if self._writer:
                self._writer.close()
        except Exception:
            pass
        self._reader = None
        self._writer = None

    def pack_command(self, *args):
        "Pack a series of arguments into the Redis protocol"
        output = []
        # the client might have included 1 or more literal arguments in
        # the command name, e.g., 'CONFIG GET'. The Redis server expects these
        # arguments to be sent separately, so split the first argument
        # manually. All of these arguements get wrapped in the Token class
        # to prevent them from being encoded.
        command = b(args[0])
        if b" " in command:
            args = tuple(s for s in command.split()) + args[1:]
        else:
            args = (command,) + args[1:]

        buff = SYM_EMPTY.join((SYM_STAR, b"%d" % len(args), SYM_CRLF))

        for arg in map(self.encode, args):
            # to avoid large string mallocs, chunk the command into the
            # output list if we're sending large values

            if len(buff) > 6000 or len(arg) > 6000:
                buff = SYM_EMPTY.join((buff, SYM_DOLLAR, b"%d" % len(arg), SYM_CRLF))
                output.append(buff)
                output.append(arg)
                buff = SYM_CRLF
            else:
                buff = SYM_EMPTY.join(
                    (buff, SYM_DOLLAR, b"%d" % len(arg), SYM_CRLF, arg, SYM_CRLF)
                )
        output.append(buff)
        return output

    def pack_commands(self, commands):
        "Pack multiple commands into the Redis protocol"
        output = []
        pieces = []
        buffer_length = 0

        for cmd in commands:
            for chunk in self.pack_command(*cmd):
                pieces.append(chunk)
                buffer_length += len(chunk)

            if buffer_length > 6000:
                output.append(SYM_EMPTY.join(pieces))
                buffer_length = 0
                pieces = []

        if pieces:
            output.append(SYM_EMPTY.join(pieces))

        return output


class Connection(BaseConnection):
    description = "Connection<host={host},port={port},db={db}>"

    def __init__(
        self,
        host="127.0.0.1",
        port=6379,
        username=None,
        password=None,
        db=0,
        retry_on_timeout=False,
        stream_timeout=None,
        connect_timeout=None,
        ssl_context=None,
        parser_class=DefaultParser,
        reader_read_size=65535,
        encoding="utf-8",
        decode_responses=False,
        socket_keepalive=None,
        socket_keepalive_options=None,
        *,
        client_name=None,
        loop=None,
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
        self.username = username
        self.password = password
        self.db = db
        self.ssl_context = ssl_context
        self._connect_timeout = connect_timeout
        self._description_args = {"host": self.host, "port": self.port, "db": self.db}
        self.socket_keepalive = socket_keepalive
        self.socket_keepalive_options = socket_keepalive_options or {}

    async def _connect(self):
        connection = asyncio.open_connection(
            host=self.host, port=self.port, ssl=self.ssl_context
        )
        reader, writer = await exec_with_timeout(
            connection, self._connect_timeout, loop=self.loop
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
                # causing an error. Do not leave the connection open.
                writer.close()
                raise
        await self.on_connect()


class UnixDomainSocketConnection(BaseConnection):
    description = "UnixDomainSocketConnection<path={path},db={db}>"

    def __init__(
        self,
        path="",
        username=None,
        password=None,
        db=0,
        retry_on_timeout=False,
        stream_timeout=None,
        connect_timeout=None,
        ssl_context=None,
        parser_class=DefaultParser,
        reader_read_size=65535,
        encoding="utf-8",
        decode_responses=False,
        *,
        client_name=None,
        loop=None,
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
        self.path = path
        self.db = db
        self.username = username
        self.password = password
        self.ssl_context = ssl_context
        self._connect_timeout = connect_timeout
        self._description_args = {"path": self.path, "db": self.db}

    async def _connect(self):
        connection = asyncio.open_unix_connection(path=self.path, ssl=self.ssl_context)
        reader, writer = await exec_with_timeout(
            connection, self._connect_timeout, loop=self.loop
        )
        self._reader = reader
        self._writer = writer
        await self.on_connect()


class ClusterConnection(Connection):
    "Manages TCP communication to and from a Redis server"
    description = "ClusterConnection<host={host},port={port}>"
    node: Dict

    def __init__(self, *args, **kwargs):
        self.readonly = kwargs.pop("readonly", False)
        super().__init__(*args, **kwargs)

    async def on_connect(self):
        """
        Initialize the connection, authenticate and select a database and send READONLY if it is
        set during object initialization.

        :meta private:
        """

        if self.db:
            warnings.warn("SELECT DB is not allowed in cluster mode")
            self.db = ""
        await super().on_connect()

        if self.readonly:
            await self.send_command(b"READONLY")

            if await self.read_response(decode=False) != b"OK":
                raise ConnectionError("READONLY command failed")
