from __future__ import annotations

import asyncio
from io import BytesIO
from typing import Hashable, Type, cast

from coredis._protocols import ConnectionP
from coredis._utils import b
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
    NotBusyError,
    ProtocolError,
    ReadOnlyError,
    RedisError,
    ResponseError,
    StreamConsumerGroupError,
    StreamDuplicateConsumerGroupError,
    TryAgainError,
    UnblockedError,
    UnknownCommandError,
    WrongTypeError,
)
from coredis.typing import (
    Dict,
    Final,
    FrozenSet,
    List,
    MutableSet,
    NamedTuple,
    Optional,
    ResponsePrimitive,
    ResponseType,
    Set,
    Tuple,
    Union,
)


class NotEnoughData:
    pass


NOT_ENOUGH_DATA: Final[NotEnoughData] = NotEnoughData()


class RESPNode:
    __slots__ = ("depth", "key", "node_type")
    depth: int
    key: Union[
        ResponsePrimitive, Tuple[ResponsePrimitive, ...], FrozenSet[ResponsePrimitive]
    ]
    node_type: int

    def __init__(
        self,
        depth: int,
        node_type: int,
        key: Union[
            ResponsePrimitive,
            Tuple[ResponsePrimitive, ...],
            FrozenSet[ResponsePrimitive],
        ],
    ):
        self.depth = depth
        self.node_type = node_type
        self.key = key

    def append(self, item: ResponseType) -> None:
        raise NotImplementedError()

    def ensure_hashable(self, item: ResponseType) -> Hashable:
        if isinstance(item, (int, float, bool, str, bytes)):
            return item
        elif isinstance(item, set):
            return frozenset(self.ensure_hashable(cast(ResponseType, i)) for i in item)
        elif isinstance(item, list):
            return tuple(self.ensure_hashable(i) for i in item)
        elif isinstance(item, dict):
            return tuple(
                (cast(ResponsePrimitive, k), self.ensure_hashable(v))
                for k, v in item.items()
            )
        return item  # noqa


class ListNode(RESPNode):
    __slots__ = ("container",)

    def __init__(self, depth: int, node_type: int) -> None:
        self.container: List[ResponseType] = []
        super().__init__(depth, node_type, None)

    def append(self, item: ResponseType) -> None:
        self.depth -= 1
        self.container.append(item)


class DictNode(RESPNode):
    __slots__ = ("container",)

    def __init__(self, depth: int) -> None:
        self.container: Dict[
            Union[
                ResponsePrimitive,
                Tuple[ResponsePrimitive, ...],
                FrozenSet[ResponsePrimitive],
            ],
            ResponseType,
        ] = {}
        super().__init__(depth * 2, RESPDataType.MAP, None)

    def append(self, item: ResponseType) -> None:
        self.depth -= 1
        if not self.key:
            self.key = cast(
                Union[
                    ResponsePrimitive,
                    Tuple[ResponsePrimitive, ...],
                    FrozenSet[ResponsePrimitive],
                ],
                self.ensure_hashable(item),
            )
        else:
            self.container[self.key] = item
            self.key = None


class SetNode(RESPNode):
    __slots__ = ("container",)

    def __init__(self, depth: int) -> None:
        self.container: MutableSet[
            Union[
                ResponsePrimitive,
                Tuple[ResponsePrimitive, ...],
                FrozenSet[ResponsePrimitive],
            ]
        ] = set()
        super().__init__(depth, RESPDataType.SET, None)

    def append(
        self,
        item: ResponseType,
    ) -> None:
        self.depth -= 1
        self.container.add(
            cast(
                Union[
                    ResponsePrimitive,
                    Tuple[ResponsePrimitive, ...],
                    FrozenSet[ResponsePrimitive],
                ],
                self.ensure_hashable(item),
            )
        )


class UnpackedResponse(NamedTuple):
    response_type: int
    response: ResponseType


class Parser:
    """
    Interface between a connection and Unpacker
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
            "unknown subcommand": UnknownCommandError,
            "sub command not supported": UnknownCommandError,
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
        "NOTBUSY": NotBusyError,
        "READONLY": ReadOnlyError,
        "TRYAGAIN": TryAgainError,
        "UNBLOCKED": UnblockedError,
        "WRONGPASS": AuthenticationFailureError,
        "WRONGTYPE": WrongTypeError,
    }

    def __init__(self) -> None:
        self.push_messages: Optional[asyncio.Queue[ResponseType]] = None
        self.localbuffer: BytesIO = BytesIO(b"")
        self.bytes_read: int = 0
        self.bytes_written: int = 0
        self.nodes: List[Union[ListNode, SetNode, DictNode]] = []

    def feed(self, data: bytes) -> None:
        self.localbuffer.seek(self.bytes_written)
        self.bytes_written += self.localbuffer.write(data)
        self.localbuffer.seek(self.bytes_read)

    def on_connect(self, connection: ConnectionP) -> None:
        """Called when the stream connects"""
        self.push_messages = connection.push_messages

    def on_disconnect(self) -> None:
        """Called when the stream disconnects"""
        if not self.localbuffer.closed:
            self.localbuffer.seek(0)
            self.localbuffer.truncate()
            self.bytes_read = self.bytes_written = 0
        self.nodes.clear()

    def can_read(self) -> bool:
        return (self.bytes_written - self.bytes_read) > 0

    def try_decode(self, data: bytes, encoding: str) -> Union[bytes, str]:
        try:
            return data.decode(encoding)
        except ValueError:
            return data

    def get_response(
        self,
        decode: bool,
        encoding: Optional[str] = None,
        push_message_types: Optional[Set[bytes]] = None,
    ) -> Union[NotEnoughData, ResponseType]:
        """

        :param decode: Whether to decode simple or bulk strings
        :param push_message_types: the push message types to return if they
         arrive. If a message arrives that does not match the filter, it will
         be put on the :data:`~coredis.connection.BaseConnection.push_messages`
         queue
        :return: The next available parsed response read from the connection.
         If there is not enough data on the wire a ``NotEnoughData`` instance
         will be returned.
        """
        while True:
            response = self.parse(decode, encoding)
            if isinstance(response, NotEnoughData):
                return response
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
                    else:
                        break
                else:
                    break
        return response.response if response else None

    def parse(
        self,
        decode_bytes: bool,
        encoding: Optional[str],
    ) -> Union[Optional[UnpackedResponse], NotEnoughData]:
        parsed: Optional[UnpackedResponse] = None

        while True:
            data = self.localbuffer.readline()
            if not data[-2::] == SYM_CRLF:
                return NOT_ENOUGH_DATA
            data_len = len(data)
            self.bytes_read += data_len
            marker, chunk = data[0], data[1:-2]
            response: ResponseType = None
            if marker == RESPDataType.SIMPLE_STRING:
                response = chunk
                if decode_bytes and encoding:
                    response = self.try_decode(response, encoding)
            elif marker == RESPDataType.BULK_STRING or marker == RESPDataType.VERBATIM:
                length = int(chunk)
                if length == -1:
                    response = None
                else:
                    if (self.bytes_written - self.bytes_read) < length + 2:
                        self.bytes_read -= data_len
                        return NOT_ENOUGH_DATA
                    data = self.localbuffer.read(length + 2)
                    self.bytes_read += length + 2
                    response = data[:-2]
                    if marker == RESPDataType.VERBATIM:
                        if response[:3] != b"txt":
                            raise InvalidResponse(
                                f"Unexpected verbatim string of type {response[:3]!r}"
                            )
                        response = response[4:]
                    if decode_bytes and encoding:
                        response = self.try_decode(response, encoding)
            elif marker in [RESPDataType.INT, RESPDataType.BIGNUMBER]:
                response = int(chunk)
            elif marker == RESPDataType.DOUBLE:
                response = float(chunk)
            elif marker == RESPDataType.NONE:
                response = None
            elif marker == RESPDataType.BOOLEAN:
                response = chunk[0] == ord(b"t")
            elif (
                marker == RESPDataType.ARRAY
                or marker == RESPDataType.PUSH
                or marker == RESPDataType.MAP
                or marker == RESPDataType.SET
            ):
                length = int(chunk)
                if length >= 0:
                    if marker in {RESPDataType.ARRAY, RESPDataType.PUSH}:
                        self.nodes.append(ListNode(length, marker))
                    elif marker == RESPDataType.MAP:
                        self.nodes.append(DictNode(length))
                    else:
                        self.nodes.append(SetNode(length))
                    if length > 0:
                        continue
            elif marker == RESPDataType.ERROR:
                response = cast(ResponseType, self.parse_error(bytes(chunk).decode()))
            else:
                raise InvalidResponse(
                    f"Protocol Error: {chr(marker)}, {bytes(chunk)!r}"
                )

            if self.nodes:
                if self.nodes[-1].depth > 0:
                    self.nodes[-1].append(response)
                    if self.nodes[-1].depth > 1:
                        continue

                while len(self.nodes) > 1 and self.nodes[-1].depth == 0:
                    self.nodes[-2].append(self.nodes[-1].container)
                    self.nodes.pop()

            if len(self.nodes) == 1 and self.nodes[-1].depth == 0:
                node = self.nodes.pop()
                parsed = UnpackedResponse(node.node_type, node.container)
                break
            if not self.nodes:
                parsed = UnpackedResponse(marker, response)
                break

        if self.bytes_read == self.bytes_written:
            self.localbuffer.seek(0)
            self.localbuffer.truncate()
            self.bytes_read = self.bytes_written = 0
        return parsed

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
                    if response.lower().startswith(err):
                        exception_class = exc
                        break
            return exception_class(response)
        return ResponseError(response)
