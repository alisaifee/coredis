from __future__ import annotations

import functools
import inspect
from typing import Any

from coredis.config import Config
from coredis.exceptions import CommandSyntaxError
from coredis.typing import (
    Callable,
    Coroutine,
    Iterable,
    Optional,
    ParamSpec,
    Set,
    TypeVar,
    Union,
)

R = TypeVar("R")
P = ParamSpec("P")


class MutuallyExclusiveParametersError(CommandSyntaxError):
    def __init__(self, arguments: Set[str], details: Optional[str]):
        message = (
            f"The [{','.join(arguments)}] parameters are mutually exclusive."
            f"{' '+details if details else ''}"
        )
        super().__init__(arguments, message)


class MutuallyInclusiveParametersMissing(CommandSyntaxError):
    def __init__(self, arguments: Set[str], leaders: Set[str], details: Optional[str]):
        if leaders:
            message = (
                f"The [{','.join(arguments)}] parameters(s)"
                " must be provided together with [{','.join(leaders)}]."
                f"{' ' + details if details else ''}"
            )
        else:
            message = (
                f"The [{','.join(arguments)}] parameters are mutually"
                " inclusive and must be provided together."
                f"{' '+details if details else ''}"
            )
        super().__init__(arguments, message)


def mutually_exclusive_parameters(
    *exclusive_params: Union[str, Iterable[str]], details: Optional[str] = None
) -> Callable[
    [Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    primary = {k for k in exclusive_params if isinstance(k, str)}
    secondary = [k for k in set(exclusive_params) - primary]

    def wrapper(
        func: Callable[P, Coroutine[Any, Any, R]],
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        sig = inspect.signature(func)

        @functools.wraps(func)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            if not Config.optimized:
                call_args = sig.bind_partial(*args, **kwargs)
                params = {
                    k
                    for k in primary
                    if not call_args.arguments.get(k)
                    == getattr(sig.parameters.get(k), "default")
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

            return await func(*args, **kwargs)

        return wrapped if not Config.optimized else func

    return wrapper


def mutually_inclusive_parameters(
    *inclusive_params: str,
    leaders: Optional[Iterable[str]] = None,
    details: Optional[str] = None,
) -> Callable[
    [Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    _leaders = set(leaders or [])
    _inclusive_params = set(inclusive_params)

    def wrapper(
        func: Callable[P, Coroutine[Any, Any, R]],
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        sig = inspect.signature(func)

        @functools.wraps(func)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            if not Config.optimized:
                call_args = sig.bind_partial(*args, **kwargs)
                params = {
                    k
                    for k in _inclusive_params | _leaders
                    if not call_args.arguments.get(k)
                    == getattr(sig.parameters.get(k), "default")
                }
                if _leaders and _leaders & params != _leaders and len(params) > 0:
                    raise MutuallyInclusiveParametersMissing(
                        _inclusive_params, _leaders, details
                    )
                elif not _leaders and params and len(params) != len(_inclusive_params):
                    raise MutuallyInclusiveParametersMissing(
                        _inclusive_params, _leaders, details
                    )
            return await func(*args, **kwargs)

        return wrapped if not Config.optimized else func

    return wrapper


def ensure_iterable_valid(
    argument: str,
) -> Callable[
    [Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    def iterable_valid(value: Any) -> bool:
        return isinstance(value, Iterable) and not isinstance(value, (str, bytes))

    def wrapper(
        func: Callable[P, Coroutine[Any, Any, R]],
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        sig = inspect.signature(func)
        expected_type = sig.parameters[argument].annotation

        @functools.wraps(func)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            if Config.optimized:
                return await func(*args, **kwargs)
            bound = sig.bind_partial(*args, **kwargs)
            value = bound.arguments.get(argument)
            if not iterable_valid(value):
                raise TypeError(
                    f"{func.__name__} parameter {argument}={value!r} "
                    f"violates expected iterable of type {expected_type}"
                )

            return await func(*args, **kwargs)

        return wrapped if not Config.optimized else func

    return wrapper
