from __future__ import annotations

import asyncio
import weakref
from typing import TYPE_CHECKING, Tuple

from coredis.connection import BaseConnection, Connection
from coredis.exceptions import ConnectionError
from coredis.typing import Optional, ResponseType, Set, TypeVar

if TYPE_CHECKING:
    import coredis.client

SidecarT = TypeVar("SidecarT", bound="Sidecar")


class Sidecar:
    """
    A sidecar to a redis client that reserves a single connection
    and moves any responses from the socket to a FIFO queue
    """

    def __init__(
        self,
        push_message_types: Set[bytes],
    ) -> None:
        self._client: Optional[
            weakref.ReferenceType["coredis.client.RedisConnection"]
        ] = None
        self.messages: asyncio.Queue[ResponseType] = asyncio.Queue()
        self.connection: Optional[Connection] = None
        self.client_id: Optional[int] = None
        self.read_task: Optional[asyncio.Task[None]] = None
        self.push_message_types = push_message_types

    @property
    def client(self) -> "Optional[coredis.client.RedisConnection]":
        if self._client:
            return self._client()
        return None  # noqa

    async def start(
        self: SidecarT, client: "coredis.client.RedisConnection"
    ) -> SidecarT:
        self._client = weakref.ref(client, lambda *_: self.shutdown())
        if not self.connection and self.client:
            self.connection = await self.client.connection_pool.get_connection()
            self.connection.register_connect_callback(self.on_reconnect)
            await self.connection.connect()
            if self.connection.tracking_client_id:  # noqa
                await self.connection.update_tracking_client(False)
        if not self.read_task or self.read_task.done():
            self.read_task = asyncio.create_task(self.__read_loop())
        return self

    def process_message(self, message: ResponseType) -> Tuple[ResponseType, ...]:
        return (message,)  # noqa

    def shutdown(self) -> None:
        if self.connection:
            self.connection.disconnect()
            if self.client and self.connection:  # noqa
                self.client.connection_pool.release(self.connection)
            self.connection = None
            self.client_id = None
            if self.read_task and not self.read_task.done():
                self.read_task.cancel()

    def __del__(self) -> None:
        self.shutdown()

    async def on_reconnect(self, connection: BaseConnection) -> None:
        self.client_id = connection.client_id

    async def __read_loop(self) -> None:
        while self.connection:
            try:
                response = await self.connection.read_response(
                    decode=False, push_message_types=self.push_message_types
                )
                if response:
                    for m in self.process_message(response):
                        self.messages.put_nowait(m)
            except asyncio.CancelledError:
                break
            except ConnectionError:
                if self.client and self.connection:
                    self.client.connection_pool.release(self.connection)
                self.connection = None

                if self.client:
                    await self.start(self.client)
