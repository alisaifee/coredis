from __future__ import annotations

from coredis.typing import (
    Iterable,
    List,
    Mapping,
    MutableMapping,
    OrderedDict,
    Tuple,
    TypeVar,
    Union,
)

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")
W = TypeVar("W")


def flat_pairs_to_dict(
    response: Union[Mapping[T, T], Tuple[T, ...], List[T]]
) -> MutableMapping[T, T]:
    """Creates a dict given a flat list of key/value pairs"""
    if isinstance(response, MutableMapping):
        return response
    it = iter(response)
    return dict(zip(it, it))


def flat_pairs_to_ordered_dict(response: Tuple[T, ...]) -> OrderedDict[T, T]:
    """Creates a dict given a flat list of key/value pairs"""
    it = iter(response)
    return OrderedDict(zip(it, it))


def pairs_to_dict(
    response: Union[Mapping[T, T], Tuple[Tuple[T, T], ...]]
) -> MutableMapping[T, T]:
    """Creates a dict given an array of tuples"""
    if isinstance(response, MutableMapping):
        return response
    return dict(response)


def pairs_to_ordered_dict(response: Tuple[Tuple[T, T]]) -> OrderedDict[T, T]:
    """Creates a dict given a flat list of key/value pairs"""
    return OrderedDict(response)


def quadruples_to_dict(
    response: Iterable[Tuple[T, U, V, W]]
) -> MutableMapping[T, Tuple[U, V, W]]:
    return {k[0]: (k[1], k[2], k[3]) for k in response}
