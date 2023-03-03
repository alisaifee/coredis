from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from functools import wraps
from typing import Any, AsyncIterator

from coredis.typing import Callable, Coroutine, Optional, P, R, Tuple, Type


class RetryPolicy(ABC):
    @abstractmethod
    async def retries(self) -> AsyncIterator[int]:
        # mypy shenanigans
        raise NotImplementedError()
        if False:
            yield 0


class ConstantRetryPolicy(RetryPolicy):
    def __init__(self, retries: int, delay: float) -> None:
        self.__retries = retries
        self.__delay = delay

    async def retries(self) -> AsyncIterator[int]:
        for i in range(self.__retries):
            yield i
            await asyncio.sleep(self.__delay)


class ExponentialBackoffRetryPolicy(RetryPolicy):
    def __init__(self, retries: int, initial_delay: float) -> None:
        self.__retries = retries
        self.__initial_delay = initial_delay

    async def retries(self) -> AsyncIterator[int]:
        for i in range(self.__retries):
            yield i
            await asyncio.sleep(pow(2, i) * self.__initial_delay)


def retryable(
    retryable_exceptions: Tuple[Type[BaseException], ...],
    failure_hook: Optional[Callable[..., Coroutine[Any, Any, None]]] = None,
    policy: RetryPolicy = ConstantRetryPolicy(3, 0),
) -> Callable[
    [Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    def inner(
        func: Callable[P, Coroutine[Any, Any, R]]
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        @wraps(func)
        async def _inner(*args: P.args, **kwargs: P.kwargs) -> R:
            last_error: Optional[BaseException] = None
            async for _ in policy.retries():
                try:
                    return await func(*args, **kwargs)
                except retryable_exceptions as e:
                    if failure_hook:
                        await failure_hook(args[0])
                    last_error = e
            if last_error:
                raise last_error
            assert False

        return _inner

    return inner
