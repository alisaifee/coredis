from __future__ import annotations

import asyncio
import functools
from abc import ABC, abstractmethod
from functools import wraps
from typing import Any, AsyncIterator

from coredis.typing import Callable, Coroutine, Optional, P, R, Tuple, Type


class RetryPolicy(ABC):
    """
    Abstract retry policy
    """

    def __init__(self, retryable_exceptions: Tuple[Type[BaseException], ...]) -> None:
        self.retryable_exceptions = retryable_exceptions

    @abstractmethod
    async def retries(self) -> AsyncIterator[int]:
        """
        Iterator that yields the number of retries and delays if
        desired
        """
        # mypy shenanigans
        raise NotImplementedError()
        if False:
            yield 0

    async def call_with_retries(
        self,
        func: Callable[..., Coroutine[Any, Any, R]],
        failure_hook: Optional[Callable[..., Coroutine[Any, Any, None]]] = None,
    ) -> R:
        """
        Calls :paramref:`func` repeatedly according to this retry policy
        if :paramref:`RetryPolicy.retryable_exceptions` is encountered.

        If :paramref:`failure_hook` is provided it will be called everytime
        a retryable exception is encountered.
        """
        last_error: Optional[BaseException] = None
        async for _ in self.retries():
            try:
                return await func()
            except self.retryable_exceptions as e:
                if failure_hook:
                    try:
                        await failure_hook()
                    except:  # noqa
                        pass
                last_error = e
        if last_error:
            raise last_error
        assert False


class ConstantRetryPolicy(RetryPolicy):
    """
    Retry policy that pauses :paramref:`ConstantRetryPolicy.delay`
    seconds between :paramref:`ConstantRetryPolicy.retries`
    if any of :paramref:`ConstantRetryPolicy.retryable_exceptions` are
    encountered.
    """

    def __init__(
        self,
        retryable_exceptions: Tuple[Type[BaseException], ...],
        retries: int,
        delay: float,
    ) -> None:
        self.__retries = retries
        self.__delay = delay
        super().__init__(retryable_exceptions)

    async def retries(self) -> AsyncIterator[int]:
        for i in range(self.__retries):
            if i > 0:
                await asyncio.sleep(self.__delay)
            yield i


class ExponentialBackoffRetryPolicy(RetryPolicy):
    """
    Retry policy that exponentially backs off before retrying up to
    :paramref:`ExponentialBackoffRetryPolicy.retries` if any
    of :paramref:`ExponentialBackoffRetryPolicy.retryable_exceptions` are
    encountered. :paramref:`ExponentialBackoffRetryPolicy.initial_delay`
    is used as the initial value for calculating the exponential backoff.

    """

    def __init__(
        self,
        retryable_exceptions: Tuple[Type[BaseException], ...],
        retries: int,
        initial_delay: float,
    ) -> None:
        self.__retries = retries
        self.__initial_delay = initial_delay
        super().__init__(retryable_exceptions)

    async def retries(self) -> AsyncIterator[int]:
        for i in range(self.__retries):
            if i > 0:
                await asyncio.sleep(pow(2, i) * self.__initial_delay)
            yield i


class CompositeRetryPolicy(RetryPolicy):
    """
    Convenience class to combine multiple retry policies
    """

    def __init__(self, *retry_policies: RetryPolicy):
        self._retry_policies = retry_policies

    async def retries(self) -> AsyncIterator[int]:
        raise NotImplementedError()
        if False:
            yield 0

    async def call_with_retries(
        self,
        func: Callable[..., Coroutine[Any, Any, R]],
        failure_hook: Optional[Callable[..., Coroutine[Any, Any, None]]] = None,
    ) -> R:
        """
        Calls :paramref:`func` repeatedly according to the retry policies that
        this class was instantiated with (:paramref:`CompositeRetryPolicy.retry_policies`).

        If :paramref:`failure_hook` is provided it will be called everytime
        a retryable exception is encountered.
        """
        wrapped = None
        for policy in self._retry_policies:
            wrapped = functools.partial(
                policy.call_with_retries, wrapped or func, failure_hook
            )
        return await (wrapped or func)()  # type: ignore


def retryable(
    policy: RetryPolicy,
    failure_hook: Optional[Callable[..., Coroutine[Any, Any, None]]] = None,
) -> Callable[
    [Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    """
    Decorator to be used to apply a retry policy to a coroutine
    """

    def inner(
        func: Callable[P, Coroutine[Any, Any, R]]
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        @wraps(func)
        async def _inner(*args: P.args, **kwargs: P.kwargs) -> R:
            return await policy.call_with_retries(
                lambda: func(*args, **kwargs), failure_hook
            )

        return _inner

    return inner
