from __future__ import annotations

import asyncio
import sys
from asyncio import StreamReader

from coredis._unpacker import NotEnoughData, Unpacker
from coredis._utils import b
from coredis.constants import RESPDataType
from coredis.exceptions import ConnectionError
from coredis.protocols import ConnectionP
from coredis.typing import Optional, ResponseType, Set


class Parser:
    """
    Interface between a connection and Unpacker
    """

    def __init__(self, read_size: int) -> None:
        self._stream: Optional[StreamReader] = None
        self.encoding: Optional[str] = None
        self.unpacker: Optional[Unpacker] = None
        self.push_messages: Optional[asyncio.Queue[ResponseType]] = None
        self._read_size = read_size

    def __del__(self) -> None:
        self.on_disconnect()

    def on_connect(self, connection: ConnectionP) -> None:
        """Called when the stream connects"""
        self._stream = connection.reader
        if not self.unpacker:
            self.unpacker = Unpacker(connection.encoding)
        self.push_messages = connection.push_messages
        if connection.decode_responses:
            self.encoding = connection.encoding

    def on_disconnect(self) -> None:
        """Called when the stream disconnects"""

        if self._stream is not None:
            self._stream = None

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

        decode_bytes = (decode is None or decode) if self.encoding else False

        while True:
            assert self.unpacker
            response = self.unpacker.parse(decode_bytes)
            if isinstance(response, NotEnoughData):
                try:
                    buffer = await self._stream.read(self._read_size)
                    if not buffer:
                        raise ConnectionError("Socket closed on remote end")
                    self.unpacker.feed(buffer)
                except asyncio.CancelledError:
                    raise
                except ConnectionError:
                    raise
                except Exception:  # noqa
                    e = sys.exc_info()[1]
                    if e:
                        raise ConnectionError(
                            f"Error while reading from stream: {e}"
                        ) from e
                    raise
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
