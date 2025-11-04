from __future__ import annotations

from collections import deque

import anyio

from coredis.typing import Generic, R


class QueueEmpty(Exception): ...


class QueueFull(Exception): ...


class AsyncQueue(Generic[R]):
    def __init__(self, maxsize: int = 0):
        self._maxsize = maxsize
        self._queue: deque[R] = deque()
        self._getters: deque[anyio.Event] = deque()
        self._putters: deque[anyio.Event] = deque()
        self._lock = anyio.Lock()

    def empty(self) -> bool:
        return not self._queue

    def full(self) -> bool:
        return self._maxsize > 0 and len(self._queue) >= self._maxsize

    async def put(self, item: R) -> None:
        async with self._lock:
            while self.full():
                ev = anyio.Event()
                self._putters.append(ev)
                await ev.wait()

            self._queue.append(item)

            if self._getters:
                self._getters.popleft().set()

    def put_nowait(self, item: R) -> None:
        if self.full():
            raise QueueFull()
        self._queue.append(item)

        if self._getters:
            ev = self._getters.popleft()
            ev.set()

    async def get(self) -> R:
        async with self._lock:
            while self.empty():
                ev = anyio.Event()
                self._getters.append(ev)
                await ev.wait()

            item = self._queue.pop()

            if self._putters and not self.full():
                self._putters.popleft().set()

            return item

    def get_nowait(self) -> R:
        if self.empty():
            raise QueueEmpty()
        item = self._queue.pop()

        if self._putters and not self.full():
            self._putters.popleft().set()

        return item
