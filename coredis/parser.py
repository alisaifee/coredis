from __future__ import annotations

import asyncio

from coredis._protocols import ConnectionP
from coredis._unpacker import NotEnoughData, Unpacker
from coredis._utils import b
from coredis.constants import RESPDataType
from coredis.exceptions import ConnectionError
from coredis.typing import Optional, ResponseType, Set, Union


class Parser:
    """
    Interface between a connection and Unpacker
    """

    def __init__(self) -> None:
        self.encoding: Optional[str] = None
        self.unpacker: Optional[Unpacker] = None
        self.push_messages: Optional[asyncio.Queue[ResponseType]] = None
        self.connected = asyncio.Event()
        self._last_error: Optional[BaseException] = None

    def __del__(self) -> None:
        self.on_disconnect(None)

    def on_connect(self, connection: ConnectionP) -> None:
        """Called when the stream connects"""
        self.connected.set()
        self._last_error = None
        if not self.unpacker:
            self.unpacker = Unpacker(connection.encoding)
        self.push_messages = connection.push_messages
        if connection.decode_responses:
            self.encoding = connection.encoding

    def on_disconnect(self, exc: Optional[BaseException]) -> None:
        """Called when the stream disconnects"""
        self._last_error = exc
        self.connected.clear()
        self.encoding = None

    def can_read(self) -> bool:
        return (
            (self.unpacker.bytes_written - self.unpacker.bytes_read) > 0
            if self.unpacker
            else False
        )

    def get_response(
        self,
        decode: Optional[bool] = None,
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
        decode_bytes = (decode is None or decode) if self.encoding else False
        assert self.unpacker
        if not self.connected.is_set():
            if self._last_error:
                raise self._last_error
            else:
                raise ConnectionError("Socket closed")
        while True:
            response = self.unpacker.parse(decode_bytes)
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
