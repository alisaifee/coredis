from __future__ import annotations

from io import BytesIO
from typing import Optional, cast

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
from coredis.typing import (
    Dict,
    Final,
    List,
    MutableSet,
    NamedTuple,
    ResponsePrimitive,
    ResponseType,
    Set,
    Tuple,
    Type,
    Union,
)


class RESPNode:
    __slots__ = ("container", "depth", "key", "node_type")
    depth: int
    node_type: int
    key: ResponsePrimitive

    def __init__(
        self,
        container: Union[
            List[ResponseType],
            MutableSet[Union[ResponsePrimitive, Tuple[ResponsePrimitive, ...]]],
            Dict[ResponsePrimitive, ResponseType],
        ],
        depth: int,
        node_type: int,
        key: ResponsePrimitive,
    ):
        self.container = container
        self.depth = depth
        self.node_type = node_type
        self.key = key


class NotEnoughData:
    pass


class UnpackedResponse(NamedTuple):
    response_type: int
    response: ResponseType


NOT_ENOUGH_DATA: Final[NotEnoughData] = NotEnoughData()


class Unpacker:
    ALLOWED_TYPES: Final[Set[int]] = {
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

    def __init__(
        self,
        encoding: str,
    ):
        self.encoding = encoding
        self.localbuffer = BytesIO(b"")
        self.bytes_read = 0
        self.bytes_written = 0
        self.nodes: List[RESPNode] = []

    def feed(self, data: bytes) -> None:
        self.localbuffer.seek(self.bytes_written)
        self.bytes_written += self.localbuffer.write(data)
        self.localbuffer.seek(self.bytes_read)

    def parse(
        self,
        decode_bytes: bool,
    ) -> Union[Optional[UnpackedResponse], NotEnoughData]:
        parsed: Optional[UnpackedResponse] = None

        assert self.localbuffer

        while True:
            data = self.localbuffer.readline()
            if not data[-2::] == SYM_CRLF:
                return NOT_ENOUGH_DATA
            data_len = len(data)
            self.bytes_read += data_len
            chunk = data[:-2]

            marker, chunk = chunk[0], chunk[1:]
            response: ResponsePrimitive | RedisError = None
            if marker == RESPDataType.SIMPLE_STRING:
                response = chunk
                if decode_bytes and self.encoding:
                    response = response.decode(self.encoding)
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
            elif (
                marker == RESPDataType.ARRAY
                or marker == RESPDataType.PUSH
                or marker == RESPDataType.MAP
                or marker == RESPDataType.SET
            ):
                if marker in {RESPDataType.ARRAY, RESPDataType.PUSH}:
                    length = int(chunk)
                    if length >= 0:
                        self.nodes.append(
                            RESPNode(
                                [],
                                length,
                                marker,
                                None,
                            )
                        )
                elif marker == RESPDataType.MAP:
                    length = 2 * int(chunk)
                    if length >= 0:
                        self.nodes.append(RESPNode({}, length, marker, None))
                else:
                    length = int(chunk)
                    if length >= 0:
                        self.nodes.append(RESPNode(set(), length, marker, None))
                if length > 0:
                    continue
            elif marker == RESPDataType.ERROR:
                error = self.parse_error(bytes(chunk).decode())
                if isinstance(error, ConnectionError):
                    raise error
                response = error
            elif marker not in Unpacker.ALLOWED_TYPES:  # noqa
                raise InvalidResponse(
                    f"Protocol Error: {chr(marker)}, {bytes(chunk)!r}"
                )

            if self.nodes:
                if self.nodes[-1].depth > 0:
                    self.nodes[-1].depth -= 1
                    if isinstance(self.nodes[-1].container, list):
                        self.nodes[-1].container.append(response)
                    elif isinstance(self.nodes[-1].container, dict):
                        if self.nodes[-1].key is not None:
                            self.nodes[-1].container[self.nodes[-1].key] = response
                            self.nodes[-1].key = None
                        else:
                            self.nodes[-1].key = cast(ResponsePrimitive, response)
                    else:
                        self.nodes[-1].container.add(cast(ResponsePrimitive, response))
                    if self.nodes[-1].depth > 1:
                        continue

                while len(self.nodes) > 1 and self.nodes[-1].depth == 0:
                    self.nodes[-2].depth -= 1
                    if isinstance(self.nodes[-2].container, list):
                        self.nodes[-2].container.append(self.nodes[-1].container)
                    elif isinstance(self.nodes[-2].container, dict):
                        if self.nodes[-2].key is not None:
                            self.nodes[-2].container[self.nodes[-2].key] = self.nodes[
                                -1
                            ].container
                            self.nodes[-2].key = None
                        else:
                            raise TypeError(
                                f"unhashable type {self.nodes[-1].container} as dictionary key"
                            )
                    else:
                        raise TypeError(
                            f"unhashable type {self.nodes[-1].container} as set member"
                        )
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
