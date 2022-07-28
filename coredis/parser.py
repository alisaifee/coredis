from __future__ import annotations

import asyncio

from coredis._protocols import ConnectionP
from coredis._unpacker import NotEnoughData, Unpacker
from coredis._utils import b
from coredis.constants import RESPDataType
from coredis.typing import Optional, ResponseType, Set, Union


class Parser:
    """
    Interface between a connection and Unpacker
    """

    def __init__(self, encoding: str, decode_responses: bool) -> None:
        self.decode_responses: bool = decode_responses
        self.unpacker: Unpacker = Unpacker(encoding)
        self.push_messages: Optional[asyncio.Queue[ResponseType]] = None

    def on_connect(self, connection: ConnectionP) -> None:
        """Called when the stream connects"""
        self.push_messages = connection.push_messages

    def on_disconnect(self) -> None:
        """Called when the stream disconnects"""

    def can_read(self) -> bool:
        return (self.unpacker.bytes_written - self.unpacker.bytes_read) > 0

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
        decode_bytes = (decode is None or decode) if self.decode_responses else False
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
