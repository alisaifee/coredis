from __future__ import annotations

import math
from typing import Generic

from anyio import create_memory_object_stream
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

from coredis.typing import R


class AsyncQueue(Generic[R]):
    def __init__(self, maxsize: int = 0):
        send: MemoryObjectSendStream[R]
        recv: MemoryObjectReceiveStream[R]

        send, recv = create_memory_object_stream[R](maxsize if maxsize > 0 else math.inf)

        self._send = send
        self._recv = recv
        self._maxsize = maxsize

    async def put(self, item: R) -> None:
        await self._send.send(item)

    async def get(self) -> R:
        return await self._recv.receive()

    def put_nowait(self, item: R) -> None:
        self._send.send_nowait(item)

    def get_nowait(self) -> R:
        return self._recv.receive_nowait()

    async def close(self) -> None:
        await self._send.aclose()
        await self._recv.aclose()
