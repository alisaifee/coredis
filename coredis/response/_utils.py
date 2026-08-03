from __future__ import annotations

from typing import cast

from coredis.typing import (
    Callable,
    Iterable,
    OrderedDict,
    StringT,
    TypeVar,
)

T = TypeVar("T")


def flat_pairs_to_dict(
    response: tuple[T, ...] | list[T],
    value_transform: Callable[..., T] | None = None,
) -> dict[T, T]:
    """Creates a dict given a flat list of key/value pairs"""
    it = iter(response)
    if value_transform:
        return dict(zip(it, map(value_transform, it)))
    else:
        return dict(zip(it, it))


def flat_pairs_to_ordered_dict(response: Iterable[T]) -> OrderedDict[StringT, T]:
    """Creates a dict given a flat list of key/value pairs"""
    it = iter(response)
    return cast(OrderedDict[StringT, T], OrderedDict(zip(it, it)))
