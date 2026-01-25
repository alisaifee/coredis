from __future__ import annotations

import math
from abc import ABC, abstractmethod
from functools import wraps
from random import randint
from typing import Any

from anyio import sleep
from exceptiongroup import BaseExceptionGroup

from coredis._utils import logger
from coredis.typing import Awaitable, Callable, P, R


class RetryPolicy(ABC):
    """
    Abstract retry policy
    """

    def __init__(
        self, *, retryable_exceptions: tuple[type[BaseException], ...], retries: int
    ) -> None:
        """
        :param retryable_exceptions: The exceptions to trigger a retry for
        :param retries: number of times to retry if a :paramref:`retryable_exception`
         is encountered.
        """
        self.retryable_exceptions = retryable_exceptions
        self.retries = retries

    @abstractmethod
    async def delay(self, attempt_number: int) -> None:
        pass

    async def call_with_retries(
        self,
        func: Callable[..., Awaitable[R]],
        before_hook: Callable[..., Awaitable[Any]] | None = None,
        failure_hook: Callable[..., Awaitable[Any]]
        | dict[type[BaseException], Callable[..., Awaitable[None]]]
        | None = None,
    ) -> R:
        """
        :param func: a function that should return the coroutine that will be
         awaited when retrying if :paramref:`RetryPolicy.retryable_exceptions` is encountered.
        :param before_hook: if provided will be called on every attempt.
        :param failure_hook: if provided and is a callable it will be
         called everytime a retryable exception is encountered. If it is a mapping
         of exception types to callables, the first exception type that is a parent
         of any encountered exception will be called.
        """
        last_error: BaseException
        for attempt in range(1, self.retries + 2):
            try:
                if before_hook:
                    await before_hook()
                return await func()
            except BaseException as e:
                if self.will_retry(e):
                    logger.info(f"Retry attempt {attempt} due to error: {e}")
                    if failure_hook:
                        try:
                            if isinstance(failure_hook, dict):
                                for exc_type, hook in failure_hook.items():
                                    if self._exception_matches(e, exc_type):
                                        await hook(e)
                                        break
                            else:
                                await failure_hook(e)
                        except:  # noqa
                            pass
                    last_error = e
                    # no more retries.
                    if attempt < self.retries + 1:
                        await self.delay(attempt)
                else:
                    raise
        raise last_error

    def will_retry(self, exc: BaseException) -> bool:
        return self._exception_matches(exc, *self.retryable_exceptions)

    def _exception_matches(cls, needle: BaseException, *haystack: type[BaseException]) -> bool:
        if isinstance(needle, BaseExceptionGroup):
            for exc in haystack:
                match, unmatched = needle.split(exc)
                if match:
                    return True
        else:
            return isinstance(needle, haystack)
        return False

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}<"
            f"retries={self.retries}, "
            f"retryable_exceptions={','.join(e.__name__ for e in self.retryable_exceptions)}"
            ">"
        )


class NoRetryPolicy(RetryPolicy):
    def __init__(self) -> None:
        super().__init__(retryable_exceptions=(), retries=0)

    async def delay(self, attempt_number: int) -> None:
        pass


class ConstantRetryPolicy(RetryPolicy):
    """
    Retry policy that pauses :paramref:`ConstantRetryPolicy.delay`
    seconds between :paramref:`ConstantRetryPolicy.retries`
    if any of :paramref:`ConstantRetryPolicy.retryable_exceptions` are
    encountered.
    """

    def __init__(
        self,
        retryable_exceptions: tuple[type[BaseException], ...],
        retries: int,
        delay: float,
    ) -> None:
        self.__delay = delay
        super().__init__(retryable_exceptions=retryable_exceptions, retries=retries)

    async def delay(self, attempt_number: int) -> None:
        await sleep(self.__delay)


class ExponentialBackoffRetryPolicy(RetryPolicy):
    """
    Retry policy that exponentially backs off before retrying up to
    :paramref:`ExponentialBackoffRetryPolicy.retries` if any
    of :paramref:`ExponentialBackoffRetryPolicy.retryable_exceptions` are
    encountered. :paramref:`ExponentialBckoffRetryPolicy.base_delay`
    is used as the base value for calculating the exponential backoff given the attempt.

    For example with ``base_delay`` == 1::

        attempt 1 = 2^(1-1)*1 == 1
        attempt 2 = 2^(2-1)*1 == 2
        attempt 3 = 2^(3-1)*1 == 4

    To cap the delay to a maximum value, use :paramref:`max_delay`.

    """

    def __init__(
        self,
        retryable_exceptions: tuple[type[BaseException], ...],
        retries: int,
        base_delay: float,
        max_delay: float = math.inf,
        jitter: bool = False,
    ) -> None:
        self.__base_delay = base_delay
        self.__max_delay = max_delay
        self.__jitter = jitter
        super().__init__(retryable_exceptions=retryable_exceptions, retries=retries)

    async def delay(self, attempt_number: int) -> None:
        delay = min(self.__max_delay, pow(2, attempt_number - 1) * self.__base_delay)
        if self.__jitter:
            delay = randint(int(self.__base_delay), int(delay))
        await sleep(delay)


class CompositeRetryPolicy(RetryPolicy):
    """
    Convenience class to combine multiple retry policies
    """

    def __init__(self, *retry_policies: RetryPolicy):
        self._retry_policies = set(retry_policies)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}<{','.join(str(p) for p in self._retry_policies)}>"

    def add_retry_policy(self, policy: RetryPolicy) -> None:
        """
        Add to the retry policies that this instance was created with
        """
        self._retry_policies.add(policy)

    async def delay(self, attempt_number: int) -> None:
        raise NotImplementedError()

    async def call_with_retries(
        self,
        func: Callable[..., Awaitable[R]],
        before_hook: Callable[..., Awaitable[Any]] | None = None,
        failure_hook: None
        | (
            Callable[..., Awaitable[Any]] | dict[type[BaseException], Callable[..., Awaitable[Any]]]
        ) = None,
    ) -> R:
        """
        Calls :paramref:`func` repeatedly according to the retry policies that
        this class was instantiated with (:paramref:`CompositeRetryPolicy.retry_policies`).

        :param func: a function that should return the coroutine that will be
         awaited when retrying if :paramref:`RetryPolicy.retryable_exceptions` is encountered.
        :param before_hook: if provided will be called before every attempt.
        :param failure_hook: if provided and is a callable it will be
         called everytime a retryable exception is encountered. If it is a mapping
         of exception types to callables, the first exception type that is a parent
         of any encountered exception will be called.
        """
        attempts = {policy: 0 for policy in self._retry_policies}
        while True:
            try:
                if before_hook:
                    await before_hook()
                return await func()
            except BaseException as e:
                will_retry = False
                for policy in attempts:
                    if policy.will_retry(e) and attempts[policy] < policy.retries:
                        attempts[policy] += 1
                        await policy.delay(attempts[policy])
                        will_retry = True
                        logger.info(f"Retry attempt {attempts[policy]} due to error: {e}")
                        break

                if failure_hook:
                    if isinstance(failure_hook, dict):
                        for exc_type in failure_hook:
                            if self._exception_matches(e, exc_type):
                                await failure_hook[exc_type](e)
                                break
                    else:
                        await failure_hook(e)

                if will_retry:
                    continue
                raise e


def retryable(
    policy: RetryPolicy,
    failure_hook: Callable[..., Awaitable[Any]] | None = None,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    """
    Decorator to be used to apply a retry policy to a coroutine
    """

    def inner(
        func: Callable[P, Awaitable[R]],
    ) -> Callable[P, Awaitable[R]]:
        @wraps(func)
        async def _inner(*args: P.args, **kwargs: P.kwargs) -> R:
            return await policy.call_with_retries(
                lambda: func(*args, **kwargs), failure_hook=failure_hook
            )

        return _inner

    return inner
