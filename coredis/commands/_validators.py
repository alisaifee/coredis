from __future__ import annotations

import functools
import inspect
from typing import Any

from coredis.config import Config
from coredis.exceptions import CommandSyntaxError
from coredis.typing import (
    Callable,
    Iterable,
    ParamSpec,
    TypeVar,
)

R = TypeVar("R")
P = ParamSpec("P")


class RequiredParameterError(CommandSyntaxError):
    def __init__(self, arguments: set[str], details: str | None):
        message = (
            f"One of [{','.join(arguments)}] must be provided.{' ' + details if details else ''}"
        )
        super().__init__(arguments, message)


class MutuallyExclusiveParametersError(CommandSyntaxError):
    def __init__(self, arguments: set[str], details: str | None):
        message = (
            f"The [{','.join(arguments)}] parameters are mutually exclusive."
            f"{' ' + details if details else ''}"
        )
        super().__init__(arguments, message)


class MutuallyInclusiveParametersMissing(CommandSyntaxError):
    def __init__(self, arguments: set[str], leaders: set[str], details: str | None):
        if leaders:
            message = (
                f"The [{','.join(arguments)}] parameters(s)"
                f" must be provided together with [{','.join(leaders)}]."
                f"{' ' + details if details else ''}"
            )
        else:
            message = (
                f"The [{','.join(arguments)}] parameters are mutually"
                " inclusive and must be provided together."
                f"{' ' + details if details else ''}"
            )
        super().__init__(arguments, message)


def mutually_exclusive_parameters(
    *exclusive_params: str | Iterable[str],
    details: str | None = None,
    required: bool = False,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    primary = {k for k in exclusive_params if isinstance(k, str)}
    secondary = [k for k in set(exclusive_params) - primary]

    def wrapper(
        func: Callable[P, R],
    ) -> Callable[P, R]:
        sig = inspect.signature(func)

        @functools.wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            if not Config.optimized:
                call_args = sig.bind_partial(*args, **kwargs)
                params = {
                    k
                    for k in primary
                    if not call_args.arguments.get(k) == getattr(sig.parameters.get(k), "default")
                }

                if params:
                    for group in secondary:
                        for k in group:
                            if not call_args.arguments.get(k) == getattr(
                                sig.parameters.get(k), "default"
                            ):
                                params.add(k)

                                break

                if len(params) > 1:
                    raise MutuallyExclusiveParametersError(params, details)
                if len(params) == 0 and required:
                    raise RequiredParameterError(primary, details)

            return func(*args, **kwargs)

        return wrapped if not Config.optimized else func

    return wrapper


def mutually_inclusive_parameters(
    *inclusive_params: str,
    leaders: Iterable[str] | None = None,
    details: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    _leaders = set(leaders or [])
    _inclusive_params = set(inclusive_params)

    def wrapper(
        func: Callable[P, R],
    ) -> Callable[P, R]:
        sig = inspect.signature(func)

        @functools.wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            if not Config.optimized:
                call_args = sig.bind_partial(*args, **kwargs)
                params = {
                    k
                    for k in _inclusive_params | _leaders
                    if not call_args.arguments.get(k) == getattr(sig.parameters.get(k), "default")
                }
                if _leaders and _leaders & params != _leaders and len(params) > 0:
                    raise MutuallyInclusiveParametersMissing(_inclusive_params, _leaders, details)
                elif not _leaders and params and len(params) != len(_inclusive_params):
                    raise MutuallyInclusiveParametersMissing(_inclusive_params, _leaders, details)
            return func(*args, **kwargs)

        return wrapped if not Config.optimized else func

    return wrapper


def ensure_iterable_valid(
    argument: str,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def iterable_valid(value: Any) -> bool:
        return isinstance(value, Iterable) and not isinstance(value, (str, bytes))

    def wrapper(
        func: Callable[P, R],
    ) -> Callable[P, R]:
        sig = inspect.signature(func)
        expected_type = sig.parameters[argument].annotation

        @functools.wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            if Config.optimized:
                return func(*args, **kwargs)
            bound = sig.bind_partial(*args, **kwargs)
            value = bound.arguments.get(argument)
            if not iterable_valid(value):
                raise TypeError(
                    f"{func.__name__} parameter {argument}={value!r} violates expected iterable of type {expected_type}"
                )

            return func(*args, **kwargs)

        return wrapped if not Config.optimized else func

    return wrapper
