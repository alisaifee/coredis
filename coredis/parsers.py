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
        self._sock = None

    async def read_from_socket(self, buf: BytesIO, length: Optional[int] = None) -> int:
        marker = 0

        try:
            while True:
                data = await self._stream.read(self.read_size)
                # an empty string indicates the server shutdown the socket
                if len(data) == 0:
                    raise ConnectionError("Socket closed on remote end")
                buf.write(data)
                data_length = len(data)
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
        return marker

    def close(self) -> None:
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
        self._localbuffer: Optional[BytesIO] = None
        self._bytes_read = 0
        self._bytes_written = 0
        self.encoding: Optional[str] = None
        super().__init__(read_size)

    def __del__(self) -> None:
        self.on_disconnect()

    def on_connect(self, connection: ConnectionP) -> None:
        """Called when the stream connects"""
        self._stream = connection.reader
        self._buffer = SocketBuffer(self._stream, self._read_size)
        self._localbuffer = BytesIO()
        if connection.decode_responses:
            self.encoding = connection.encoding

    def on_disconnect(self) -> None:
        """Called when the stream disconnects"""

        if self._stream is not None:
            self._stream = None

        if self._buffer is not None:
            self._buffer.close()
            self._buffer = None

        if self._localbuffer is not None:
            self._localbuffer = None
            self._bytes_read = 0
            self._bytes_written = 0

        self.encoding = None

    def can_read(self) -> bool:
        return (
            (self._bytes_written - self._bytes_read) > 0 if self._localbuffer else False
        )

    async def read_response(self, decode: Optional[bool] = None) -> ResponseType:
        if not self._buffer:
            raise ConnectionError("Socket closed on remote end")

        need_decode = self.encoding is not None
        decode_bytes = False
        if decode is not None:
            need_decode = decode
        if need_decode and self.encoding:
            decode_bytes = True

        parsed: ResponseType = None
        nodes: List[RESPNode] = []

        assert self._localbuffer
        buffer = self._localbuffer
        bytes_written, bytes_read = self._bytes_written, self._bytes_read

        while True:
            data = buffer.readline()
            if not data[-2::] == SYM_CRLF:
                buffer.seek(bytes_written)
                num_written = await self._buffer.read_from_socket(buffer)
                bytes_written += num_written
                buffer.seek(bytes_read)
                continue
            bytes_read += len(data)
            chunk = data[:-2]

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
                if decode_bytes and self.encoding:
                    response = response.decode(self.encoding)
            elif marker == RESPDataType.INT:
                response = int(chunk)
            elif marker == RESPDataType.DOUBLE:
                response = float(chunk)
            elif marker == RESPDataType.NONE:
                response = None
            elif marker == RESPDataType.BOOLEAN:
                response = chunk[0] == ord(b"t")
            elif marker in {RESPDataType.BULK_STRING, RESPDataType.VERBATIM}:
                length = int(chunk)
                if length == -1:
                    response = None
                else:
                    if (available := bytes_written - bytes_read) < length + 2:
                        buffer.seek(bytes_written)
                        num_written = await self._buffer.read_from_socket(
                            buffer, length + 2 - available
                        )
                        bytes_written += num_written
                        buffer.seek(bytes_read)
                    data = buffer.read(length + 2)
                    bytes_read += len(data)
                    response = data[:-2]
                    if marker == RESPDataType.VERBATIM:
                        if response[:3] != b"txt":
                            raise InvalidResponse(
                                f"Unexpected verbatim string of type {response[:3]!r}"
                            )
                        response = response[4:]
                    if decode_bytes and self.encoding:
                        response = response.decode(self.encoding)
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
                parsed = nodes.pop().container
                break
            if not nodes:
                parsed = response
                break

        if bytes_read == bytes_written:
            self._localbuffer.seek(0)
            self._localbuffer.truncate()
            self._bytes_read = self._bytes_written = 0
        else:
            self._bytes_read = bytes_read
            self._bytes_written = bytes_written

        return parsed


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
