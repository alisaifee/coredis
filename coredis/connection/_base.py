from __future__ import annotations

import dataclasses
import functools
import inspect
import math
import os
import ssl
from abc import ABC, abstractmethod
from collections import deque
from typing import cast

from anyio import (
    TASK_STATUS_IGNORED,
    BrokenResourceError,
    CancelScope,
    CapacityLimiter,
    ClosedResourceError,
    EndOfStream,
    create_memory_object_stream,
    create_task_group,
    fail_after,
)
from anyio.abc import ByteStream, TaskGroup, TaskStatus
from anyio.lowlevel import checkpoint
from exceptiongroup import BaseExceptionGroup, catch

import coredis
from coredis._packer import Packer
from coredis._utils import logger, nativestr
from coredis.commands.constants import CommandName
from coredis.constants.resp import DataType
from coredis.credentials import AbstractCredentialProvider, UserPassCredentialProvider
from coredis.exceptions import ConnectionError, UnknownCommandError
from coredis.parser import NotEnoughData, Parser
from coredis.tokens import PureToken
from coredis.typing import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Concatenate,
    NotRequired,
    P,
    R,
    RedisError,
    RedisValueT,
    ResponseType,
    Self,
    TypedDict,
    TypeVar,
)

from ._request import Request

CERT_REQS = {
    "none": ssl.CERT_NONE,
    "optional": ssl.CERT_OPTIONAL,
    "required": ssl.CERT_REQUIRED,
}

ConnectionT = TypeVar("ConnectionT", bound="BaseConnection")


@dataclasses.dataclass(unsafe_hash=True)
class Location:
    """
    Abstract location
    """

    ...


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
    credential_provider: NotRequired[AbstractCredentialProvider | None]
    #: If provided the connection will immediately switch to this db as part of the handshake
    db: NotRequired[int | None]
    #: For TLS connections, the ssl context to use when performing the TLS handshake
    ssl_context: NotRequired[ssl.SSLContext | None]


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


@dataclasses.dataclass
class CommandInvocation:
    command: bytes
    args: tuple[RedisValueT, ...]
    decode: bool | None
    encoding: str | None


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

    @staticmethod
    def _ensure_usable(
        function: Callable[Concatenate[ConnectionT, P], R],
    ) -> Callable[Concatenate[ConnectionT, P], R]:
        @functools.wraps(function)
        def _connection_ensured(slf: ConnectionT, /, *args: P.args, **kwargs: P.kwargs) -> R:
            if (not slf._handshake_attempted and slf.transport_healthy) or slf.usable:
                return function(slf, *args, **kwargs)
            raise ConnectionError("Connection not usable") from slf._last_error

        _connection_ensured.__doc__ = f"""{_connection_ensured.__doc__}
:raises: :exc:`coredis.exceptions.ConnectionError` if the connection is
 not usable.
"""
        return _connection_ensured

    def __init__(
        self,
        location: Location,
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
        ssl_context: ssl.SSLContext | None = None,
        processing_budget: CapacityLimiter | None = None,
    ):
        """
        :param location: The location of the server this connection is connecting to
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
        :param ssl_context: For TLS connections, the ssl context to use when performing
         the TLS handshake.
        :param processing_budget: limiter to throttle CPU-bound processing.
        """
        self.location = location

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
            (Callable[[Self], Awaitable[None]] | Callable[[Self], None])
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
        self.stream: ByteStream | None = None
        # buffer for push message types
        self._push_message_buffer_in, self._push_message_buffer_out = create_memory_object_stream[
            list[ResponseType]
        ](math.inf)
        # buffer for streaming messages
        self._streamed_message_buffer_in, self._streamed_message_buffer_out = (
            create_memory_object_stream[ResponseType](math.inf)
        )
        # RESP parser/packer
        #  for writes to the socket
        self._write_buffer_in, self._write_buffer_out = create_memory_object_stream[list[Request]](
            math.inf
        )
        self._parser = Parser()
        self._packer: Packer = Packer(self._encoding)

        self._requests: deque[Request] = deque()
        self._connection_cancel_scope: CancelScope | None = None

        # Error & State flags
        self._last_error: BaseException | None = None
        self._ready = False
        self._handshake_attempted = False
        self._transport_failed = False
        self._terminated = False

        # To be used in the read task for cpu bound processing after data is received
        self._processing_budget = processing_budget or CapacityLimiter(1)

    def __repr__(self) -> str:
        return self.describe()

    @abstractmethod
    def describe(self) -> str: ...

    @property
    def transport_healthy(self) -> bool:
        """
        Whether the underlying transport stream is healthy
        """
        return self.stream is not None and not self._transport_failed and not self._terminated

    @property
    def usable(self) -> bool:
        """
        Whether the connection is established and initial handshakes were
        performed without error
        """
        return self.transport_healthy and self._ready

    def register_connect_callback(
        self,
        callback: (Callable[[Self], None] | Callable[[Self], Awaitable[None]]),
    ) -> None:
        """
        Registers a callback that will be executed after the initial handshake
        is performed and before the connection is marked usable for regular
        commands.

        .. caution:: Any exception raised by a connect callback will not be
           handled and will result in an unusable connection.
        """
        self._connect_callbacks.append(callback)

    async def _trigger_connect_callbacks(self: Self) -> None:
        for callback in self._connect_callbacks:
            task = callback(self)
            if inspect.isawaitable(task):
                await task

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

        def handle_errors(error: BaseExceptionGroup) -> None:
            # TODO: change _last_error to use the whole exception group
            #  once python 3.10 support is dropped and the library
            #  consistently uses exception groups
            self._last_error = self._last_error or error.exceptions[-1]

        try:
            self.stream = await self._connect()
            try:
                with catch({Exception: handle_errors}):
                    async with (
                        self.stream,
                        self._write_buffer_in,
                        self._write_buffer_out,
                        self._push_message_buffer_in,
                        self._push_message_buffer_out,
                        self._streamed_message_buffer_in,
                        self._streamed_message_buffer_out,
                        create_task_group() as self._task_group,
                    ):
                        self._task_group.start_soon(self._reader_task)
                        self._task_group.start_soon(self._writer_task)
                        # setup connection
                        await self.__perform_handshake()
                        # *CAUTION* (and also maybe *FIXME*).
                        # There should be no code executed that could raise an
                        # exception after this line and before the task is marked
                        # as started.
                        task_status.started()
            finally:
                disconnect_exc = self._last_error or ConnectionError("Connection lost!")
                self._parser.on_disconnect()
                while self._requests:
                    request = self._requests.popleft()
                    request.fail(disconnect_exc)
                self.stream = None
                if self._ready:
                    # If a connection had successfully been established (including handshake)
                    # errors should no longer be raised and it is the responsibility of the
                    # downstream to ensure that `is_connected` is tested before using a connection
                    if self._last_error:
                        logger.info("Connection closed unexpectedly!", exc_info=True)
                    self._ready = False
                else:
                    # If a RedisError (raised ourselves) has resulted in an exception
                    # before a connection was completely established raise that.
                    # This is due to known handshake errors.
                    if isinstance(self._last_error, RedisError):
                        raise self._last_error
                    else:
                        logger.exception("Connection attempt failed unexpectedly!")
                        raise ConnectionError(
                            "Unable to establish a connection"
                        ) from self._last_error
        except Exception as connection_error:
            self._last_error = connection_error
            if isinstance(connection_error, RedisError):
                raise
            else:
                # Wrap any other errors with a ConnectionError so that upstreams (pools) can
                # handle them explicitly as being part of connection creation if they want.
                raise ConnectionError("Unable to establish a connection") from connection_error

    def terminate(self, reason: str | None = None) -> None:
        """
        Terminates the connection prematurely due to internal
        reasons (basically a request pending on the connection has been
        cancelled or timed out leaving the connection unusable)

        :meta private:
        """
        self._terminated = True
        if self._task_group:
            self._task_group.cancel_scope.cancel(reason)

    async def _reader_task(self) -> None:
        """
        Listen on the socket and run the parser, completing pending requests in
        FIFO order.
        """
        while self.stream is not None:
            with fail_after(self._max_idle_time):
                try:
                    data = await self.stream.receive()
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
                else:
                    self._streamed_message_buffer_in.send_nowait(response[1])
            decode = self._requests[0].decode if self._requests else self._decode_responses
            response = self._parser.parse(
                decode=decode,
                encoding=self._requests[0].encoding if self._requests else self._encoding,
            )

    async def _writer_task(self) -> None:
        """
        Continually empty the buffer and send the data to the server.
        """
        while self.stream is not None:
            requests = await self._write_buffer_out.receive()
            while self._write_buffer_out.statistics().current_buffer_used > 0:
                requests.extend(self._write_buffer_out.receive_nowait())
                await checkpoint()
            data = b"".join(
                self._packer.pack_commands(
                    [(request.command, *request.args) for request in requests]
                )
            )
            try:
                await self.stream.send(data)
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

    async def __perform_handshake(self) -> None:
        try:
            hello_command_args: list[int | str | bytes] = [3]
            if creds := (
                await self._credential_provider.get_credentials()
                if self._credential_provider
                else (
                    await UserPassCredentialProvider(
                        self._username, self._password
                    ).get_credentials()
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
            await self._trigger_connect_callbacks()
            self._ready = True
        finally:
            self._handshake_attempted = True

    @property
    @_ensure_usable
    async def push_messages(self) -> AsyncGenerator[list[ResponseType], None]:
        """
        Generator to retrieve RESP3 push type messages sent by the server.
        The generator will yield each message received in order until the
        connection is lost and will raise an :exc:`~coredis.exceptions.ConnectionError`
        to signal that the generator has ended.
        """
        try:
            while True:
                yield await self._push_message_buffer_out.receive()
        except (EndOfStream, BrokenResourceError, ClosedResourceError) as err:
            raise ConnectionError("Connection lost while waiting for push messages") from err

    @property
    @_ensure_usable
    async def streamed_messages(self) -> AsyncGenerator[ResponseType, None]:
        """
        Generator to retrieve messages received from the server that were not
        sent as a response to a request made through this connection.
        The generator will yield each message received in order until the
        connection is lost and will raise an :exc:`~coredis.exceptions.ConnectionError`
        to signal that the generator has ended.
        """
        try:
            while True:
                yield await self._streamed_message_buffer_out.receive()
        except (EndOfStream, BrokenResourceError, ClosedResourceError) as err:
            raise ConnectionError("Connection lost") from err

    @_ensure_usable
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
        self._write_buffer_in.send_nowait(
            [
                Request(
                    connection=self,
                    command=command,
                    args=args,
                    decode=False,
                    expects_response=False,
                )
            ]
        )

    @_ensure_usable
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
        requests = []
        request_timeout: float | None = timeout or self._stream_timeout
        expects_response = not (self._noreply_set or noreply)
        if noreply and not self._noreply:
            requests.append(
                Request(
                    connection=self,
                    command=CommandName.CLIENT_REPLY,
                    args=(PureToken.SKIP,),
                    decode=bool(decode) if decode is not None else self._decode_responses,
                    encoding=encoding or self._encoding,
                    raise_exceptions=raise_exceptions,
                    expects_response=False,
                    disconnect_on_cancellation=disconnect_on_cancellation,
                )
            )
        request = Request(
            connection=self,
            command=command,
            args=args,
            decode=bool(decode) if decode is not None else self._decode_responses,
            encoding=encoding or self._encoding,
            raise_exceptions=raise_exceptions,
            response_timeout=request_timeout,
            expects_response=expects_response,
            disconnect_on_cancellation=disconnect_on_cancellation,
        )
        requests.append(request)
        self._write_buffer_in.send_nowait(requests)
        if expects_response:
            self._requests.append(request)
        return request

    @_ensure_usable
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
        request_timeout: float | None = timeout or self._stream_timeout
        requests = [
            Request(
                connection=self,
                command=cmd.command,
                args=cmd.args,
                decode=bool(cmd.decode) if cmd.decode is not None else self._decode_responses,
                encoding=cmd.encoding or self._encoding,
                raise_exceptions=raise_exceptions,
                response_timeout=request_timeout,
                disconnect_on_cancellation=disconnect_on_cancellation,
            )
            for cmd in commands
        ]
        self._write_buffer_in.send_nowait(requests)
        self._requests.extend(requests)
        return requests
