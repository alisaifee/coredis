from __future__ import annotations

from abc import abstractmethod
from collections.abc import Hashable
from io import BytesIO
from typing import cast

from coredis.constants.resp import SYM_CRLF, SYM_TRUE, DataType
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
    Final,
    Literal,
    MutableSet,
    ResponsePrimitive,
    ResponseType,
    StringT,
)


class NotEnoughData:
    pass


NOT_ENOUGH_DATA: Final[NotEnoughData] = NotEnoughData()


class RESPNode:
    __slots__ = ("depth", "key", "node_type")
    depth: int
    key: Hashable
    node_type: Literal[DataType.PUSH, DataType.ARRAY, DataType.MAP, DataType.SET]

    def __init__(
        self,
        depth: int,
        node_type: Literal[DataType.PUSH, DataType.ARRAY, DataType.MAP, DataType.SET],
        key: (ResponsePrimitive | tuple[ResponsePrimitive, ...] | frozenset[ResponsePrimitive]),
    ):
        self.depth = depth
        self.node_type = node_type
        self.key = key

    @abstractmethod
    def append(self, item: ResponseType) -> int: ...

    def ensure_hashable(self, item: ResponseType) -> Hashable:
        if isinstance(item, (int, float, bool, str, bytes)):
            return item
        elif isinstance(item, set):
            return frozenset(self.ensure_hashable(cast(ResponseType, i)) for i in item)
        elif isinstance(item, list):
            return tuple(self.ensure_hashable(i) for i in item)
        elif isinstance(item, dict):
            return tuple(
                (cast(ResponsePrimitive, k), self.ensure_hashable(v)) for k, v in item.items()
            )
        return item


class ListNode(RESPNode):
    __slots__ = ("container",)

    def __init__(self, depth: int, node_type: Literal[DataType.PUSH, DataType.ARRAY]) -> None:
        self.container: list[ResponseType] = []
        super().__init__(depth, node_type, None)

    def append(self, item: ResponseType) -> int:
        self.depth -= 1
        self.container.append(item)
        return self.depth


class DictNode(RESPNode):
    __slots__ = "container"

    def __init__(self, depth: int) -> None:
        self.container: dict[Hashable, ResponseType] = {}
        super().__init__(depth * 2, DataType.MAP, None)

    def append(self, item: ResponseType) -> int:
        self.depth -= 1
        if self.key is None:
            self.key = self.ensure_hashable(item)
        else:
            self.container[self.key] = item
            self.key = None
        return self.depth


class SetNode(RESPNode):
    __slots__ = ("container",)

    def __init__(self, depth: int) -> None:
        self.container: MutableSet[Hashable] = set()
        super().__init__(depth, DataType.SET, None)

    def append(
        self,
        item: ResponseType,
    ) -> int:
        self.depth -= 1
        self.container.add(self.ensure_hashable(item))
        return self.depth


RESPScalar = (
    tuple[Literal[DataType.SIMPLE_STRING], StringT]
    | tuple[Literal[DataType.BULK_STRING], StringT | None]
    | tuple[Literal[DataType.VERBATIM], StringT]
    | tuple[Literal[DataType.INT], int]
    | tuple[Literal[DataType.BIGNUMBER], int]
    | tuple[Literal[DataType.DOUBLE], float]
    | tuple[Literal[DataType.BOOLEAN], bool]
    | tuple[Literal[DataType.NONE], None]
    | tuple[Literal[DataType.ERROR], RedisError]
)
RESPContainer = (
    tuple[Literal[DataType.ARRAY], list[ResponseType]]
    | tuple[Literal[DataType.PUSH], list[ResponseType]]
    | tuple[Literal[DataType.MAP], dict[Hashable, ResponseType]]
    | tuple[Literal[DataType.SET], MutableSet[Hashable]]
)

UnpackedResponse = RESPScalar | RESPContainer


class Parser:
    """ """

    EXCEPTION_CLASSES: dict[str, type[RedisError] | dict[str, type[RedisError]]] = {
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
        self.localbuffer: BytesIO = BytesIO(b"")
        self.bytes_read: int = 0
        self.bytes_written: int = 0
        self.nodes: list[ListNode | SetNode | DictNode] = []
        self.current_node: ListNode | SetNode | DictNode | None = None

    def feed(self, data: bytes) -> None:
        self.localbuffer.seek(self.bytes_written)
        self.bytes_written += self.localbuffer.write(data)
        self.localbuffer.seek(self.bytes_read)

    def on_disconnect(self) -> None:
        """Called when the stream disconnects"""
        if not self.localbuffer.closed:
            self.localbuffer.seek(0)
            self.localbuffer.truncate()
            self.bytes_read = self.bytes_written = 0
        self.nodes.clear()

    def can_read(self) -> bool:
        return (self.bytes_written - self.bytes_read) > 0

    def try_decode(self, data: bytes, encoding: str) -> bytes | str:
        try:
            return data.decode(encoding)
        except ValueError:
            return data

    def parse(
        self,
        decode: bool = False,
        encoding: str | None = None,
    ) -> UnpackedResponse | NotEnoughData:
        """
        :param decode: Whether to decode simple or bulk strings
        :param encoding: The encoding to use if :paramref:`decode` is set
        :return: The next available parsed response read from the connection.
         If there is not enough data on the wire a ``NotEnoughData`` instance
         will be returned.
        """
        while True:
            data = self.localbuffer.readline()
            if not data.endswith(SYM_CRLF):
                return NOT_ENOUGH_DATA
            data_len = len(data)
            self.bytes_read += data_len
            marker, chunk = data[0], data[1:-2]
            response: ResponseType = None
            match marker:
                case DataType.SIMPLE_STRING:
                    response = chunk
                    if decode and encoding:
                        response = self.try_decode(response, encoding)
                case DataType.BULK_STRING | DataType.VERBATIM:
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
                        if marker == DataType.VERBATIM:
                            if response[:3] != b"txt":
                                raise InvalidResponse(
                                    f"Unexpected verbatim string of type {response[:3]!r}"
                                )
                            response = response[4:]
                        if decode and encoding:
                            response = self.try_decode(response, encoding)
                case DataType.INT | DataType.BIGNUMBER:
                    response = int(chunk)
                case DataType.DOUBLE:
                    response = float(chunk)
                case DataType.NONE:
                    response = None
                case DataType.BOOLEAN:
                    response = chunk[0] == ord(SYM_TRUE)
                case DataType.ARRAY | DataType.PUSH | DataType.MAP | DataType.SET:
                    length = int(chunk)
                    if length >= 0:
                        match marker:
                            case DataType.ARRAY:
                                self.current_node = ListNode(length, DataType.ARRAY)
                                self.nodes.append(self.current_node)
                            case DataType.PUSH:
                                self.current_node = ListNode(length, DataType.PUSH)
                                self.nodes.append(self.current_node)
                            case DataType.MAP:
                                self.current_node = DictNode(length)
                                self.nodes.append(self.current_node)
                            case DataType.SET:
                                self.current_node = SetNode(length)
                                self.nodes.append(self.current_node)
                        if length > 0:
                            continue
                case DataType.ERROR:
                    response = self.parse_error(bytes(chunk).decode())
                case _:
                    raise InvalidResponse(
                        f"Protocol Error: Unknown RESP data type: {chr(marker)!r}"
                    )
            if self.current_node:
                if self.current_node.depth > 0:
                    if self.current_node.append(response) > 1:
                        continue
                while len(self.nodes) > 1 and self.current_node.depth == 0:
                    self.nodes[-2].append(self.current_node.container)
                    self.nodes.pop()
                    self.current_node = self.nodes[-1]

            if self.current_node and self.current_node.depth == 0:
                self.nodes.pop()
                parsed = cast(
                    UnpackedResponse,
                    (DataType(self.current_node.node_type), self.current_node.container),
                )
                self.current_node = None
                break
            if not self.nodes:
                parsed = cast(UnpackedResponse, (DataType(marker), response))
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
