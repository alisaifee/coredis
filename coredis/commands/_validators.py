from __future__ import annotations

import functools
import inspect

from coredis.config import Config
from coredis.exceptions import CommandSyntaxError
from coredis.typing import (
    Callable,
    Iterable,
    Mapping,
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


class ParameterAvailability:
    def __init__(self, sig: inspect.Signature, names: set[str]):
        self.sig = sig
        self.positional_indexes = {
            name: idx
            for idx, (name, parameter) in enumerate(sig.parameters.items())
            if parameter.kind
            in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        }
        self.defaults = {name: self._get_parameter_default(name) for name in names}

    def _get_parameter_default(self, name: str) -> object:
        parameter = self.sig.parameters.get(name)
        return parameter.default if parameter else None

    def is_provided(
        self,
        name: str,
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> bool:
        if name in kwargs:
            value = kwargs[name]
        elif (position := self.positional_indexes.get(name)) is not None and position < len(args):
            value = args[position]
        else:
            value = None

        return value != self.defaults.get(name)

    def provided_parameters(
        self,
        names: Iterable[str],
        args: tuple[object, ...],
        kwargs: Mapping[str, object],
    ) -> set[str]:
        return {name for name in names if self.is_provided(name, args, kwargs)}


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
        tracker = ParameterAvailability(sig, primary.union(*secondary) if secondary else primary)

        @functools.wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            if not Config.optimized:
                if params := tracker.provided_parameters(primary, args, kwargs):
                    for group in secondary:
                        for parameter in group:
                            if parameter in params:
                                break
                            if tracker.is_provided(parameter, args, kwargs):
                                params.add(parameter)
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
        tracked_params = _inclusive_params | _leaders
        tracker = ParameterAvailability(sig, tracked_params)

        @functools.wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            if not Config.optimized:
                params = tracker.provided_parameters(tracked_params, args, kwargs)
                if _leaders and _leaders & params != _leaders and len(params) > 0:
                    raise MutuallyInclusiveParametersMissing(_inclusive_params, _leaders, details)
                elif not _leaders and params and len(params) != len(_inclusive_params):
                    raise MutuallyInclusiveParametersMissing(_inclusive_params, _leaders, details)
            return func(*args, **kwargs)

        return wrapped if not Config.optimized else func

    return wrapper
