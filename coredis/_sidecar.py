from __future__ import annotations

import asyncio
import time
import weakref
from typing import TYPE_CHECKING, Any

from anyio import sleep

from coredis.connection import BaseConnection, Connection
from coredis.exceptions import ConnectionError
from coredis.typing import ResponseType, TypeVar

if TYPE_CHECKING:
    import coredis.client

SidecarT = TypeVar("SidecarT", bound="Sidecar")


class Sidecar:
    """
    A sidecar to a redis client that reserves a single connection
    and moves any responses from the socket to a FIFO queue
    """

    def __init__(
        self, push_message_types: set[bytes], health_check_interval_seconds: int = 5
    ) -> None:
        self._client: weakref.ReferenceType[coredis.client.Client[Any]] | None = None
        self.messages: asyncio.Queue[ResponseType] = asyncio.Queue()
        self.connection: Connection | None = None
        self.client_id: int | None = None
        self.read_task: asyncio.Task[None] | None = None
        self.push_message_types = push_message_types
        self.health_check_interval = health_check_interval_seconds
        self.health_check_task: asyncio.Task[None] | None = None
        self.last_checkin: float = 0

    @property
    def client(self) -> coredis.client.Client[Any] | None:
        if self._client:
            return self._client()
        return None  # noqa

    async def start(self: SidecarT, client: coredis.client.Client[Any]) -> SidecarT:
        self._client = weakref.ref(client, lambda *_: self.stop())
        if not self.connection and self.client:
            self.connection = await self.client.connection_pool.acquire()
            self.connection.register_connect_callback(self.on_reconnect)
            await self.connection.connect()
            if self.connection.tracking_client_id:  # noqa
                await self.connection.update_tracking_client(False)
        if not self.read_task or self.read_task.done():
            self.read_task = asyncio.create_task(self.__read_loop())
        if not self.health_check_task or self.health_check_task.done():
            self.health_check_task = asyncio.create_task(self.__health_check())
        return self

    def process_message(self, message: ResponseType) -> tuple[ResponseType, ...]:
        return (message,)  # noqa

    def stop(self) -> None:
        try:
            asyncio.get_running_loop()
            if self.read_task and not self.read_task.done():
                self.read_task.cancel()
            if self.health_check_task and not self.health_check_task.done():
                self.health_check_task.cancel()
        except RuntimeError:
            pass
        if self.connection:
            self.connection.disconnect()
            if self.client and self.connection:  # noqa
                self.client.connection_pool.release(self.connection)
            self.connection = None
            self.client_id = None

    def __del__(self) -> None:
        self.stop()

    async def on_reconnect(self, connection: BaseConnection) -> None:
        self.client_id = connection.client_id
        self.last_checkin = time.monotonic()

    async def __health_check(self) -> None:
        while True:
            if self.connection:
                await self.connection.send_command(b"PING")
            await sleep(self.health_check_interval)

    async def __read_loop(self) -> None:
        while self.connection:
            try:
                response = await self.connection.fetch_push_message(
                    decode=False, push_message_types=self.push_message_types
                )
                self.last_checkin = time.monotonic()
                if response == b"PONG" or b"pong" in response:  # type: ignore
                    continue
                for m in self.process_message(response):
                    self.messages.put_nowait(m)
            except ConnectionError:
                if self.client and self.connection:
                    self.client.connection_pool.release(self.connection)
                self.connection = None

                if self.client:
                    asyncio.get_running_loop().call_soon(
                        asyncio.create_task, self.start(self.client)
                    )
                    break
