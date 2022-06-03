from __future__ import annotations

import asyncio
import sys
from abc import ABC, abstractmethod
from asyncio import StreamReader

from coredis._unpacker import NotEnoughData, Unpacker
from coredis._utils import b
from coredis.constants import RESPDataType
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
from coredis.typing import Dict, Optional, ResponseType, Set, Type, Union

try:
    import hiredis

    _hiredis_available = True
except ImportError:
    _hiredis_available = False

HIREDIS_AVAILABLE = _hiredis_available


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
    async def read_response(
        self,
        decode: Optional[bool] = None,
        push_message_types: Optional[Set[bytes]] = None,
    ) -> ResponseType:
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
    def __init__(self, read_size: int) -> None:
        self._stream: Optional[StreamReader] = None
        self.encoding: Optional[str] = None
        self.unpacker: Optional[Unpacker] = None
        self.push_messages: Optional[asyncio.Queue[ResponseType]] = None

        super().__init__(read_size)

    def __del__(self) -> None:
        self.on_disconnect()

    def on_connect(self, connection: ConnectionP) -> None:
        """Called when the stream connects"""
        self._stream = connection.reader
        self.unpacker = Unpacker(connection.encoding, self.parse_error)
        self.push_messages = connection.push_messages
        if connection.decode_responses:
            self.encoding = connection.encoding

    def on_disconnect(self) -> None:
        """Called when the stream disconnects"""

        if self._stream is not None:
            self._stream = None

        if self.unpacker is not None:
            self.unpacker = None
        self.push_messages = None
        self.encoding = None

    def can_read(self) -> bool:
        return (
            (self.unpacker.bytes_written - self.unpacker.bytes_read) > 0
            if self.unpacker
            else False
        )

    async def read_response(
        self,
        decode: Optional[bool] = None,
        push_message_types: Optional[Set[bytes]] = None,
    ) -> ResponseType:
        if not self._stream:
            raise ConnectionError("Socket closed on remote end")

        need_decode = self.encoding is not None
        decode_bytes = False
        if decode is not None:
            need_decode = decode
        if need_decode and self.encoding:
            decode_bytes = True

        assert self.unpacker

        while True:
            response = self.unpacker.parse(decode_bytes)
            if isinstance(response, NotEnoughData):
                try:
                    buffer = await self._stream.read(self._read_size)
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
                self.unpacker.feed(buffer)
            else:
                if response and response.response_type == RESPDataType.PUSH:
                    assert isinstance(response.response, list)
                    assert self.push_messages
                    if (
                        not push_message_types
                        or b(response.response[0]) not in push_message_types
                    ):
                        self.push_messages.put_nowait(response.response)
                        continue
                break
        return response.response if response else None


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

    async def read_response(
        self,
        decode: Optional[bool] = None,
        push_message_types: Optional[Set[bytes]] = None,
    ) -> ResponseType:
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
