from __future__ import annotations

import asyncio
import sys
from abc import ABC, abstractmethod
from asyncio import StreamReader
from io import BytesIO

from coredis.constants import SYM_CRLF, RESPDataType
from coredis.exceptions import (
    AskError,
    AuthenticationFailureError,
    AuthenticationRequiredError,
    AuthorizationError,
    BusyLoadingError,
    ClusterCrossSlotError,
    ClusterDownError,
    ConnectionError,
    ExecAbortError,
    InvalidResponse,
    MovedError,
    NoKeyError,
    NoScriptError,
    ProtocolError,
    ReadOnlyError,
    RedisError,
    ResponseError,
    StreamConsumerGroupError,
    StreamDuplicateConsumerGroupError,
    TryAgainError,
    UnknownCommandError,
    WrongTypeError,
)
from coredis.protocols import ConnectionP
from coredis.typing import (
    ClassVar,
    Dict,
    List,
    MutableSet,
    Optional,
    ResponsePrimitive,
    ResponseType,
    Set,
    Tuple,
    Type,
    Union,
)

try:
    import hiredis

    _hiredis_available = True
except ImportError:
    _hiredis_available = False

HIREDIS_AVAILABLE = _hiredis_available


class RESPNode:
    __slots__ = ("container", "depth", "key")

    def __init__(
        self,
        container: Union[
            List[ResponseType],
            MutableSet[Union[ResponsePrimitive, Tuple[ResponsePrimitive, ...]]],
            Dict[ResponsePrimitive, ResponseType],
        ],
        depth: int,
        key: ResponsePrimitive,
    ):
        self.container = container
        self.depth = depth
        self.key = key


class NotEnoughDataError(Exception):
    pass


class SocketBuffer:
    def __init__(self, stream_reader: StreamReader, read_size: int) -> None:
        self._stream = stream_reader
        self.read_size = read_size
        self._buffer: Optional[BytesIO] = BytesIO()
        self._sock = None
        # number of bytes written to the buffer from the socket
        self.bytes_written = 0
        # number of bytes read from the buffer
        self.bytes_read = 0

    @property
    def length(self) -> int:
        return self.bytes_written - self.bytes_read

    async def read_from_socket(self, length: Optional[int] = None) -> None:
        buf = self._buffer
        assert buf
        buf.seek(self.bytes_written)
        marker = 0

        try:
            while True:
                data = await self._stream.read(self.read_size)
                # an empty string indicates the server shutdown the socket

                if len(data) == 0:
                    raise ConnectionError("Socket closed on remote end")
                buf.write(data)
                data_length = len(data)
                self.bytes_written += data_length
                marker += data_length

                if length is not None and length > marker:
                    continue

                break
        except OSError:  # noqa
            e = sys.exc_info()[1]
            if e:
                raise ConnectionError(f"Error while reading from socket: {e.args}")
            else:
                raise

    def get(self, length: int) -> bytes:
        assert self._buffer

        length = length + 2  # make sure to read the \r\n terminator
        # make sure we've read enough data from the socket

        if length > self.length:
            raise NotEnoughDataError()

        self._buffer.seek(self.bytes_read)
        data = self._buffer.read(length)
        self.bytes_read += len(data)

        # purge the buffer when we've consumed it all so it doesn't
        # grow forever

        if self.bytes_read == self.bytes_written:
            self.purge()

        return data[:-2]

    async def read(self, length: int) -> bytes:
        assert self._buffer

        length = length + 2  # make sure to read the \r\n terminator
        # make sure we've read enough data from the socket

        if length > self.length:
            await self.read_from_socket(length - self.length)

        self._buffer.seek(self.bytes_read)
        data = self._buffer.read(length)
        self.bytes_read += len(data)

        # purge the buffer when we've consumed it all so it doesn't
        # grow forever

        if self.bytes_read == self.bytes_written:
            self.purge()

        return data[:-2]

    def readline(self) -> bytes:
        buf = self._buffer
        assert buf
        buf.seek(self.bytes_read)
        data = buf.readline()

        if not data.endswith(SYM_CRLF):
            buf.seek(self.bytes_read)
            raise NotEnoughDataError()

        self.bytes_read += len(data)
        # purge the buffer when we've consumed it all so it doesn't
        # grow forever
        if self.bytes_read == self.bytes_written:
            self.purge()

        return data[:-2]

    def purge(self) -> None:
        assert self._buffer
        self._buffer.seek(0)
        self._buffer.truncate()
        self.bytes_written = 0
        self.bytes_read = 0

    def close(self) -> None:
        self.purge()
        if self._buffer:
            self._buffer.close()
        self._buffer = None
        self._sock = None


class BaseParser(ABC):
    """
    Base class for parsers
    """

    EXCEPTION_CLASSES: Dict[
        str, Union[Type[RedisError], Dict[str, Type[RedisError]]]
    ] = {
        "ASK": AskError,
        "BUSYGROUP": StreamDuplicateConsumerGroupError,
        "CLUSTERDOWN": ClusterDownError,
        "CROSSSLOT": ClusterCrossSlotError,
        "ERR": {
            "max number of clients reached": ConnectionError,
            "unknown command": UnknownCommandError,
        },
        "EXECABORT": ExecAbortError,
        "LOADING": BusyLoadingError,
        "NOSCRIPT": NoScriptError,
        "MOVED": MovedError,
        "NOAUTH": AuthenticationRequiredError,
        "NOGROUP": StreamConsumerGroupError,
        "NOKEY": NoKeyError,
        "NOPERM": AuthorizationError,
        "NOPROTO": ProtocolError,
        "READONLY": ReadOnlyError,
        "TRYAGAIN": TryAgainError,
        "WRONGPASS": AuthenticationFailureError,
        "WRONGTYPE": WrongTypeError,
    }

    def __init__(self, read_size: int) -> None:
        self._read_size = read_size

    def parse_error(self, response: str) -> RedisError:
        """
        Parse an error response

        :meta private:
        """
        error_code = response.split(" ")[0]
        if error_code in self.EXCEPTION_CLASSES:
            response = response[len(error_code) + 1 :]
            exception_class = self.EXCEPTION_CLASSES[error_code]

            if isinstance(exception_class, dict):
                options = exception_class.items()
                exception_class = ResponseError
                for err, exc in options:
                    if response.startswith(err):
                        exception_class = exc
                        break
            return exception_class(response)
        return ResponseError(response)

    @abstractmethod
    async def read_response(self, decode: Optional[bool] = None) -> ResponseType:
        pass

    @abstractmethod
    def can_read(self) -> bool:
        pass

    @abstractmethod
    def on_connect(self, connection: ConnectionP) -> None:
        """Called when the stream connects"""

    @abstractmethod
    def on_disconnect(self) -> None:
        """Called when the stream disconnects"""


class PythonParser(BaseParser):
    """
    Built in python parser that requires no additional
    dependencies.
    """

    #: Supported response data types
    ALLOWED_TYPES: ClassVar[Set[int]] = {
        RESPDataType.NONE,
        RESPDataType.SIMPLE_STRING,
        RESPDataType.BULK_STRING,
        RESPDataType.VERBATIM,
        RESPDataType.BOOLEAN,
        RESPDataType.INT,
        RESPDataType.DOUBLE,
        RESPDataType.ARRAY,
        RESPDataType.PUSH,
        RESPDataType.MAP,
        RESPDataType.SET,
        RESPDataType.ERROR,
    }

    def __init__(self, read_size: int) -> None:
        self._stream: Optional[StreamReader] = None
        self._buffer: Optional[SocketBuffer] = None
        self.encoding: Optional[str] = None
        super().__init__(read_size)

    def __del__(self) -> None:
        self.on_disconnect()

    def on_connect(self, connection: ConnectionP) -> None:
        """Called when the stream connects"""
        self._stream = connection.reader
        self._buffer = SocketBuffer(self._stream, self._read_size)

        if connection.decode_responses:
            self.encoding = connection.encoding

    def on_disconnect(self) -> None:
        """Called when the stream disconnects"""

        if self._stream is not None:
            self._stream = None

        if self._buffer is not None:
            self._buffer.close()
            self._buffer = None
        self.encoding = None

    def can_read(self) -> bool:
        return bool(self._buffer.length) if self._buffer else False

    async def read_response(self, decode: Optional[bool] = None) -> ResponseType:
        if not self._buffer:
            raise ConnectionError("Socket closed on remote end")

        need_decode = self.encoding is not None
        decode_bytes = False
        if decode is not None:
            need_decode = decode
        if need_decode and self.encoding:
            decode_bytes = True

        nodes: List[RESPNode] = []

        while True:
            try:
                chunk = self._buffer.readline()
            except NotEnoughDataError:
                await self._buffer.read_from_socket()
                continue

            marker, chunk = chunk[0], chunk[1:]
            response: ResponsePrimitive | RedisError = None
            if marker not in PythonParser.ALLOWED_TYPES:  # noqa
                raise InvalidResponse(
                    f"Protocol Error: {chr(marker)}, {bytes(chunk)!r}"
                )
            if marker == RESPDataType.ERROR:
                error = self.parse_error(bytes(chunk).decode())
                if isinstance(error, ConnectionError):
                    raise error
                response = error
            elif marker == RESPDataType.SIMPLE_STRING:
                response = bytes(chunk)
            elif marker == RESPDataType.INT:
                response = int(chunk)
            elif marker == RESPDataType.DOUBLE:
                response = float(chunk)
            elif marker == RESPDataType.NONE:
                response = None
            elif marker == RESPDataType.BOOLEAN:
                response = chunk[0] == ord(b"t")
            elif marker == RESPDataType.BULK_STRING:
                length = int(chunk)
                if length == -1:
                    response = None
                else:
                    if self._buffer.length > length + 2:
                        response = self._buffer.get(length)
                    else:
                        response = await self._buffer.read(length)
            elif marker == RESPDataType.VERBATIM:
                length = int(chunk)
                if length == -1:
                    response = None
                else:
                    if self._buffer.length > length + 2:
                        response = self._buffer.get(length)
                    else:
                        response = await self._buffer.read(length)
                    if response[:3] != b"txt":
                        raise InvalidResponse(
                            f"Unexpected verbatim string of type {response[:3]!r}"
                        )
                    response = response[4:]
            elif marker in {
                RESPDataType.ARRAY,
                RESPDataType.PUSH,
                RESPDataType.MAP,
                RESPDataType.SET,
            }:
                if marker in {RESPDataType.ARRAY, RESPDataType.PUSH}:
                    length = int(chunk)
                    if length >= 0:
                        nodes.append(
                            RESPNode(
                                [],
                                length,
                                None,
                            )
                        )
                elif marker == RESPDataType.MAP:
                    length = 2 * int(chunk)
                    if length >= 0:
                        nodes.append(RESPNode({}, length, None))
                else:
                    length = int(chunk)
                    if length >= 0:
                        nodes.append(RESPNode(set(), length, None))
                if length > 0:
                    continue
            if decode_bytes and self.encoding and isinstance(response, bytes):
                response = response.decode(self.encoding)
            if nodes:
                if nodes[-1].depth > 0:
                    nodes[-1].depth -= 1
                    if isinstance(nodes[-1].container, list):
                        nodes[-1].container.append(response)
                    elif isinstance(nodes[-1].container, dict):
                        if nodes[-1].key is not None:
                            nodes[-1].container[nodes[-1].key] = response
                            nodes[-1].key = None
                        else:
                            if isinstance(response, (bool, int, float, bytes, str)):
                                nodes[-1].key = response
                            else:
                                raise TypeError(
                                    f"unhashable type {response} as dictionary key"
                                )
                    else:
                        if isinstance(response, (bool, int, float, bytes, str)):
                            nodes[-1].container.add(response)
                        else:
                            raise TypeError(f"unhashable type {response} as set member")

                if len(nodes) > 1:
                    while len(nodes) > 1 and nodes[-1].depth == 0:
                        nodes[-2].depth -= 1
                        if isinstance(nodes[-2].container, list):
                            nodes[-2].container.append(nodes[-1].container)
                        elif isinstance(nodes[-2].container, dict):
                            if nodes[-2].key is not None:
                                nodes[-2].container[nodes[-2].key] = nodes[-1].container
                                nodes[-2].key = None
                            else:
                                raise TypeError(
                                    f"unhashable type {nodes[-1].container} as dictionary key"
                                )
                        else:
                            raise TypeError(
                                f"unhashable type {nodes[-1].container} as set member"
                            )
                        nodes.pop()

            if len(nodes) == 1 and nodes[-1].depth == 0:
                return nodes.pop().container
            if not nodes:
                return response
        return None


HIREDIS_SENTINEL = False


class HiredisParser(BaseParser):
    """
    Parser class for connections using Hiredis
    """

    def __init__(self, read_size: int) -> None:
        if not HIREDIS_AVAILABLE:
            raise RuntimeError()
        self._stream: Optional[StreamReader] = None
        self._reader: Optional[hiredis.Reader] = None
        self._raw_reader: Optional[hiredis.Reader] = None
        self._next_response: Optional[
            Union[
                str,
                bytes,
                bool,  # to allow for HIREDIS_SENTINEL
            ]
        ] = HIREDIS_SENTINEL
        super().__init__(read_size)

    def __del__(self) -> None:
        self.on_disconnect()

    def can_read(self) -> bool:
        if not self._reader:  # noqa
            raise ConnectionError("Socket closed on remote end")

        if self._next_response is not HIREDIS_SENTINEL:
            self._next_response = self._reader.gets()

        return self._next_response is not HIREDIS_SENTINEL

    def on_connect(self, connection: ConnectionP) -> None:
        self._stream = connection.reader
        kwargs: Dict[str, Union[Type[Exception], str]] = {
            "protocolError": InvalidResponse,
            "replyError": ResponseError,
            # TODO: uncomment this and change HIREDIS_SENTINEL to something else
            #  so that bool responses with RESP3 can be handled.
            # "notEnoughData": HIREDIS_SENTINEL,
        }

        self._raw_reader = hiredis.Reader(**kwargs)  # type: ignore
        if connection.decode_responses:
            kwargs["encoding"] = connection.encoding
        self._reader = hiredis.Reader(**kwargs)  # type: ignore
        self._next_response = HIREDIS_SENTINEL

    def on_disconnect(self) -> None:
        if self._stream is not None:
            self._stream = None
        self._reader = None
        self._next_response = HIREDIS_SENTINEL

    async def read_response(self, decode: Optional[bool] = None) -> ResponseType:
        """
        Parse a response if available on the wire

        :param decode: Only valuable if set to False to override any decode
         configuration set on the parent connection
        :return: a parsed response structure from the server
        """
        response: ResponseType
        if not self._stream:
            raise ConnectionError("Socket closed on remote end")

        # _next_response might be cached from a can_read() call
        if self._next_response is not HIREDIS_SENTINEL:
            response = self._next_response
            self._next_response = HIREDIS_SENTINEL
            return response

        cur_reader = self._reader if decode is not False else self._raw_reader
        assert cur_reader

        response = cur_reader.gets()
        while response is HIREDIS_SENTINEL:
            try:
                buffer = await self._stream.read(self._read_size)
            # CancelledError will be caught by client so that command won't be retried again
            # For more detailed discussion please see https://github.com/alisaifee/coredis/issues/56
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa
                e = sys.exc_info()[1]
                if e:
                    raise ConnectionError(
                        f"Error {type(e)} while reading from stream: {e.args}"
                    )
                raise

            if not buffer:
                raise ConnectionError("Socket closed on remote end")
            cur_reader.feed(buffer)
            response = cur_reader.gets()

        if isinstance(response, ResponseError):
            return self.parse_error(response.args[0])
        return response


DefaultParser: Type[BaseParser]

if HIREDIS_AVAILABLE:
    DefaultParser = HiredisParser
else:
    DefaultParser = PythonParser
