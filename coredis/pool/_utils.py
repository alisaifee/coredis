from __future__ import annotations

from collections import deque

import anyio

from coredis.connection import BaseConnection
from coredis.typing import Generic, TypeVar

ConnectionT = TypeVar("ConnectionT", bound=BaseConnection)


class QueueEmpty(Exception): ...


class QueueFull(Exception): ...


class ConnectionQueue(Generic[ConnectionT]):
    def __init__(self, maxsize: int = 0):
        self._maxsize = maxsize
        self._queue: deque[ConnectionT | None] = deque(
            [None for _ in range(self._maxsize)], maxlen=self._maxsize
        )
        self._getters: deque[anyio.Event] = deque()
        self._putters: deque[anyio.Event] = deque()
        self._lock = anyio.Lock()

    def empty(self) -> bool:
        return not self._queue

    def full(self) -> bool:
        return self._maxsize > 0 and len(self._queue) >= self._maxsize

    async def put(self, item: ConnectionT) -> None:
        async with self._lock:
            while self.full():
                ev = anyio.Event()
                self._putters.append(ev)
                await ev.wait()
            self._queue.append(item)
            if self._getters:
                self._getters.popleft().set()

    def put_nowait(self, item: ConnectionT) -> None:
        if self.full():
            raise QueueFull()
        self._queue.append(item)
        if self._getters:
            ev = self._getters.popleft()
            ev.set()

    async def get(self) -> ConnectionT | None:
        async with self._lock:
            while self.empty():
                ev = anyio.Event()
                self._getters.append(ev)
                await ev.wait()
            item = self._queue.pop()
            if self._putters and not self.full():
                self._putters.popleft().set()

            return item

    def get_nowait(self) -> ConnectionT | None:
        if self.empty():
            raise QueueEmpty()
        item = self._queue.pop()
        if self._putters and not self.full():
            self._putters.popleft().set()

        return item

    def reset(self) -> None:
        self._queue.clear()
        for _ in range(self._maxsize):
            self._queue.append(None)
        self._getters.clear()
        self._putters.clear()
