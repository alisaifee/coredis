from __future__ import annotations

import dataclasses
import math
import random
import time
from abc import ABC, abstractmethod
from functools import wraps
from typing import Any

from anyio import sleep
from exceptiongroup import BaseExceptionGroup

from coredis._utils import logger
from coredis.typing import Awaitable, Callable, Generator, P, R


@dataclasses.dataclass
class Attempt:
    attempt: int
    final: bool


class RetryPolicy(ABC):
    """
    Abstract retry policy
    """

    def __init__(
        self,
        retryable_exceptions: tuple[type[BaseException], ...],
        retries: int | None,
        *,
        deadline: float = math.inf,
    ) -> None:
        """
        :param retryable_exceptions: The exceptions to trigger a retry for
        :param retries: number of times to retry if a :paramref:`retryable_exception`
         is encountered.
        :param deadline: Stop retrying when the time from the first attempt > deadline

        .. warning:: If :paramref:`retries` is ``None`` and deadline is ``math.inf``
           this policy effectively becomes an infinite retry policy.
        """
        self.retryable_exceptions = retryable_exceptions
        self.__retries = retries
        self.__deadline = deadline

    @abstractmethod
    def delay(self, attempt_number: int) -> float:
        """
        Returns the amount of time to pause after the ``attempt_number`` attempt
        """
        raise NotImplementedError()

    def attempts(self) -> Generator[Attempt]:
        attempt = 1
        start = time.monotonic()
        retries_complete = False
        while True:
            now = time.monotonic()
            if retries_complete:
                break
            retries_complete = (self.__retries is not None and attempt == self.__retries + 1) or (
                now + self.delay(attempt) - start >= self.__deadline
            )
            yield Attempt(attempt, retries_complete)
            attempt += 1

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
        last_error: BaseException | None = None
        for attempt in self.attempts():
            try:
                if before_hook:
                    await before_hook()
                return await func()
            except BaseException as e:
                if self.will_retry(e):
                    logger.info(f"Retry attempt {attempt.attempt} due to error: {e}")
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
                    if not attempt.final:
                        await sleep(self.delay(attempt.attempt))
                else:
                    raise
        assert last_error
        raise last_error

    def will_retry(self, exc: BaseException) -> bool:
        return RetryPolicy._exception_matches(exc, *self.retryable_exceptions)

    @classmethod
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
            f"retries={self.__retries}, "
            f"deadline={self.__deadline}, "
            f"retryable_exceptions={','.join(e.__name__ for e in self.retryable_exceptions)}"
            ">"
        )


class NoRetryPolicy(RetryPolicy):
    def __init__(self) -> None:
        super().__init__(retryable_exceptions=(), retries=0, deadline=0)

    def delay(self, attempt_number: int) -> float:
        return 0


class ConstantRetryPolicy(RetryPolicy):
    """
    Retry policy that pauses :paramref:`delay`
    seconds between :paramref:`retries` attempts
    or until :paramref:`deadline` is met if any of
    :paramref:`retryable_exceptions` are encountered.
    """

    def __init__(
        self,
        retryable_exceptions: tuple[type[BaseException], ...],
        retries: int | None,
        *,
        deadline: float = math.inf,
        delay: float = 1,
    ) -> None:
        self.__delay = delay
        super().__init__(
            retryable_exceptions=retryable_exceptions, retries=retries, deadline=deadline
        )

    def delay(self, attempt_number: int) -> float:
        return self.__delay


class ExponentialBackoffRetryPolicy(RetryPolicy):
    """
    Retry policy that exponentially backs off before retrying up to
    :paramref:`retries` or :paramref:`deadline` if any
    of :paramref:`retryable_exceptions` are encountered.
    :paramref:`base_delay` is used as the base value for calculating the
    exponential backoff given the attempt.

    For example with ``base_delay`` == 1::

        attempt 1 = 2^(1-1)*1 == 1
        attempt 2 = 2^(2-1)*1 == 2
        attempt 3 = 2^(3-1)*1 == 4

    To cap the delay to a maximum value, use :paramref:`max_delay`.

    If :paramref:`jitter` is true the delay will be randomly varied
    between :paramref:`base_delay` and the exponential delay for the
    given attempt
    """

    def __init__(
        self,
        retryable_exceptions: tuple[type[BaseException], ...],
        retries: int | None,
        *,
        deadline: float = math.inf,
        base_delay: float = 1,
        max_delay: float = math.inf,
        jitter: bool = False,
    ) -> None:
        self.__base_delay = base_delay
        self.__max_delay = max_delay
        self.__jitter = jitter
        super().__init__(
            retryable_exceptions=retryable_exceptions, retries=retries, deadline=deadline
        )

    def delay(self, attempt_number: int) -> float:
        delay: float = min(self.__max_delay, pow(2, attempt_number - 1) * self.__base_delay)
        if self.__jitter:
            delay = random.uniform(self.__base_delay, delay)
        return delay


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

    def delay(self, attempt_number: int) -> float:
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

        policy_attempts = {policy: policy.attempts() for policy in self._retry_policies}
        total_attempts: int = 0
        while True:
            try:
                total_attempts += 1
                if before_hook:
                    await before_hook()
                return await func()
            except BaseException as e:
                retry_delays = []
                for policy, attempts in policy_attempts.items():
                    if policy.will_retry(e):
                        try:
                            attempt = next(attempts)
                            if not attempt.final:
                                retry_delays.append(policy.delay(attempt.attempt))
                        except StopIteration:
                            pass  # This policy is exhausted

                if failure_hook:
                    if isinstance(failure_hook, dict):
                        for exc_type, hook in failure_hook.items():
                            if RetryPolicy._exception_matches(e, exc_type):
                                await hook(e)
                                break
                    else:
                        await failure_hook(e)

                if retry_delays:
                    logger.info(f"Retry attempt {total_attempts} due to error: {e}")
                    await sleep(max(retry_delays))
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
