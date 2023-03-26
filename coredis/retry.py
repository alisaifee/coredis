from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from functools import wraps
from typing import Any

from coredis.typing import Callable, Coroutine, Dict, Optional, P, R, Tuple, Type, Union

logger = logging.getLogger(__name__)


class RetryPolicy(ABC):
    """
    Abstract retry policy
    """

    def __init__(
        self, retries: int, retryable_exceptions: Tuple[Type[BaseException], ...]
    ) -> None:
        """
        :param retries: number of times to retry if a :paramref:`retryable_exception`
         is encountered.
        :param retryable_exceptions: The exceptions to trigger a retry for
        """
        self.retryable_exceptions = retryable_exceptions
        self.retries = retries

    @abstractmethod
    async def delay(self, attempt_number: int) -> None:
        pass

    async def call_with_retries(
        self,
        func: Callable[..., Coroutine[Any, Any, R]],
        before_hook: Optional[Callable[..., Coroutine[Any, Any, Any]]] = None,
        failure_hook: Optional[
            Union[
                Callable[..., Coroutine[Any, Any, None]],
                Dict[Type[BaseException], Callable[..., Coroutine[Any, Any, None]]],
            ]
        ] = None,
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
        last_error: Optional[BaseException] = None
        for attempt in range(self.retries + 1):
            try:
                await self.delay(attempt)
                if before_hook:
                    await before_hook()
                return await func()
            except self.retryable_exceptions as e:
                logger.info(f"Retry attempt {attempt + 1} due to error: {e}")
                if failure_hook:
                    try:
                        if isinstance(failure_hook, dict):
                            for exc_type, hook in failure_hook.items():
                                if isinstance(e, exc_type):
                                    await hook(e)
                                    break
                        else:
                            await failure_hook(e)
                    except:  # noqa
                        pass
                last_error = e
        if last_error:
            raise last_error
        assert False

    def will_retry(self, exc: BaseException) -> bool:
        return isinstance(exc, self.retryable_exceptions)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}<"
            f"retries={self.retries}, "
            f"retryable_exceptions={','.join(e.__name__ for e in self.retryable_exceptions)}"
            ">"
        )


class NoRetryPolicy(RetryPolicy):
    def __init__(self) -> None:
        super().__init__(1, ())

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
        retryable_exceptions: Tuple[Type[BaseException], ...],
        retries: int,
        delay: float,
    ) -> None:
        self.__delay = delay
        super().__init__(retries, retryable_exceptions)

    async def delay(self, attempt_number: int) -> None:
        if attempt_number > 0:
            await asyncio.sleep(self.__delay)


class ExponentialBackoffRetryPolicy(RetryPolicy):
    """
    Retry policy that exponentially backs off before retrying up to
    :paramref:`ExponentialBackoffRetryPolicy.retries` if any
    of :paramref:`ExponentialBackoffRetryPolicy.retryable_exceptions` are
    encountered. :paramref:`ExponentialBckoffRetryPolicy.initial_delay`
    is used as the initial value for calculating the exponential backoff.

    """

    def __init__(
        self,
        retryable_exceptions: Tuple[Type[BaseException], ...],
        retries: int,
        initial_delay: float,
    ) -> None:
        self.__initial_delay = initial_delay
        super().__init__(retries, retryable_exceptions)

    async def delay(self, attempt_number: int) -> None:
        if attempt_number > 0:
            await asyncio.sleep(pow(2, attempt_number) * self.__initial_delay)


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
        func: Callable[..., Coroutine[Any, Any, R]],
        before_hook: Optional[Callable[..., Coroutine[Any, Any, Any]]] = None,
        failure_hook: Optional[
            Union[
                Callable[..., Coroutine[Any, Any, None]],
                Dict[Type[BaseException], Callable[..., Coroutine[Any, Any, None]]],
            ]
        ] = None,
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
            except Exception as e:
                will_retry = False
                attempt = 0
                for policy in attempts:
                    if policy.will_retry(e) and attempts[policy] < policy.retries:
                        attempt = attempts[policy]
                        attempts[policy] += 1
                        await policy.delay(attempts[policy])
                        will_retry = True
                        break

                if failure_hook:
                    if isinstance(failure_hook, dict):
                        for exc_type in failure_hook:
                            if isinstance(e, exc_type):
                                await failure_hook[exc_type](e)
                                break
                    else:
                        await failure_hook(e)

                if will_retry:
                    logger.info(f"Retry attempt {attempt} due to error: {e}")
                    continue

                raise e


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
                lambda: func(*args, **kwargs), failure_hook=failure_hook
            )

        return _inner

    return inner
