from __future__ import annotations

from typing import cast

from coredis.typing import (
    Callable,
    Iterable,
    OrderedDict,
    StringT,
    TypeVar,
)

T_co = TypeVar("T_co")


def flat_pairs_to_dict(
    response: tuple[T_co, ...] | list[T_co],
    value_transform: Callable[..., T_co] | None = None,
) -> dict[T_co, T_co]:
    """Creates a dict given a flat list of key/value pairs"""
    if isinstance(response, dict):
        return response
    it = iter(response)
    if value_transform:
        return dict(zip(it, map(value_transform, it)))
    else:
        return dict(zip(it, it))


def flat_pairs_to_ordered_dict(response: Iterable[T_co]) -> OrderedDict[StringT, T_co]:
    """Creates a dict given a flat list of key/value pairs"""
    it = iter(response)
    return cast(OrderedDict[StringT, T_co], OrderedDict(zip(it, it)))
