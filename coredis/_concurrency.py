from __future__ import annotations

from collections import deque
from typing import Any, Awaitable, Generic, TypeVar, overload

from anyio import Event, Lock, create_task_group

T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")
T6 = TypeVar("T6")


@overload
async def gather(
    awaitable1: Awaitable[T1],
    awaitable2: Awaitable[T2],
    /,
    *,
    return_exceptions: bool = False,
) -> tuple[T1, T2]: ...


@overload
async def gather(
    awaitable1: Awaitable[T1],
    awaitable2: Awaitable[T2],
    awaitable3: Awaitable[T3],
    /,
    *,
    return_exceptions: bool = False,
) -> tuple[T1, T2, T3]: ...


@overload
async def gather(
    awaitable1: Awaitable[T1],
    awaitable2: Awaitable[T2],
    awaitable3: Awaitable[T3],
    awaitable4: Awaitable[T4],
    /,
    *,
    return_exceptions: bool = False,
) -> tuple[T1, T2, T3, T4]: ...


@overload
async def gather(
    awaitable1: Awaitable[T1],
    awaitable2: Awaitable[T2],
    awaitable3: Awaitable[T3],
    awaitable4: Awaitable[T4],
    awaitable5: Awaitable[T5],
    /,
    *,
    return_exceptions: bool = False,
) -> tuple[T1, T2, T3, T4, T5]: ...


@overload
async def gather(
    awaitable1: Awaitable[T1],
    awaitable2: Awaitable[T2],
    awaitable3: Awaitable[T3],
    awaitable4: Awaitable[T4],
    awaitable5: Awaitable[T5],
    awaitable6: Awaitable[T6],
    /,
    *,
    return_exceptions: bool = False,
) -> tuple[T1, T2, T3, T4, T5, T6]: ...


@overload
async def gather(
    *awaitables: Awaitable[T1],
    return_exceptions: bool = False,
) -> tuple[T1, ...]: ...


async def gather(*awaitables: Awaitable[Any], return_exceptions: bool = False) -> tuple[Any, ...]:
    if not awaitables:
        return ()
    results: list[Any] = [None] * len(awaitables)

    async def runner(awaitable: Awaitable[Any], i: int) -> None:
        try:
            results[i] = await awaitable
        except Exception as exc:
            if not return_exceptions:
                raise
            results[i] = exc

    async with create_task_group() as tg:
        for i, awaitable in enumerate(awaitables):
            tg.start_soon(runner, awaitable, i)

    return tuple(results)


class QueueEmpty(Exception): ...


class QueueFull(Exception): ...


class Queue(Generic[T1]):
    """
    Generic LIFO queue (stack) for use with connections.
    """

    def __init__(self, maxsize: int = 0):
        self._maxsize = maxsize
        self._queue: deque[T1 | None] = deque(
            [None for _ in range(self._maxsize)], maxlen=self._maxsize
        )
        self._getters: deque[Event] = deque()
        self._putters: deque[Event] = deque()
        self._lock = Lock()

    def empty(self) -> bool:
        return not self._queue

    def full(self) -> bool:
        return self._maxsize > 0 and len(self._queue) >= self._maxsize

    async def put(self, item: T1 | None) -> None:
        async with self._lock:
            while self.full():
                ev = Event()
                self._putters.append(ev)
                await ev.wait()
            self._queue.append(item)
            if self._getters:
                self._getters.popleft().set()

    def put_nowait(self, item: T1 | None) -> None:
        if self.full():
            raise QueueFull()
        self._queue.append(item)
        if self._getters:
            ev = self._getters.popleft()
            ev.set()

    def append_nowait(self, item: T1 | None) -> None:
        if self.full():
            raise QueueFull()
        self._queue.appendleft(item)
        if self._getters:
            ev = self._getters.popleft()
            ev.set()

    async def get(self) -> T1 | None:
        async with self._lock:
            while self.empty():
                ev = Event()
                self._getters.append(ev)
                await ev.wait()
            item = self._queue.pop()
            if self._putters and not self.full():
                self._putters.popleft().set()

            return item

    def get_nowait(self) -> T1 | None:
        if self.empty():
            raise QueueEmpty()
        item = self._queue.pop()
        if self._putters and not self.full():
            self._putters.popleft().set()

        return item

    def remove(self, item: T1) -> None:
        self._queue.remove(item)

    def __len__(self) -> int:
        return len(self._queue)

    def __contains__(self, item: T1) -> bool:
        return item in self._queue
