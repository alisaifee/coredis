from __future__ import annotations

from io import BytesIO
from typing import NamedTuple, Optional

from coredis.constants import SYM_CRLF, RESPDataType
from coredis.exceptions import ConnectionError, InvalidResponse, RedisError
from coredis.typing import (
    Callable,
    Dict,
    Final,
    List,
    MutableSet,
    ResponsePrimitive,
    ResponseType,
    Set,
    Tuple,
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

    def __init__(
        self,
        encoding: str,
        parse_error: Callable[[str], RedisError],
    ):
        self.encoding = encoding
        self.localbuffer = BytesIO(b"")
        self.bytes_read = 0
        self.bytes_written = 0
        self.parse_error = parse_error
        self.nodes: List[RESPNode] = []

    def feed(self, data: bytes) -> None:
        self.localbuffer.seek(self.bytes_written)
        self.localbuffer.write(data)
        self.bytes_written += len(data)
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
            self.bytes_read += len(data)
            chunk = data[:-2]

            marker, chunk = chunk[0], chunk[1:]
            response: ResponsePrimitive | RedisError = None
            if marker not in Unpacker.ALLOWED_TYPES:  # noqa
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
                    if (self.bytes_written - self.bytes_read) < length + 2:
                        self.bytes_read -= len(data)
                        return NOT_ENOUGH_DATA
                    data = self.localbuffer.read(length + 2)
                    self.bytes_read += len(data)
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
                            if isinstance(response, (bool, int, float, bytes, str)):
                                self.nodes[-1].key = response
                            else:
                                raise TypeError(
                                    f"unhashable type {response} as dictionary key"
                                )
                    else:
                        if isinstance(response, (bool, int, float, bytes, str)):
                            self.nodes[-1].container.add(response)
                        else:
                            raise TypeError(f"unhashable type {response} as set member")
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
