from __future__ import annotations

from typing import cast

from coredis.typing import (
    Dict,
    Iterable,
    List,
    OrderedDict,
    StringT,
    Tuple,
    TypeVar,
    Union,
)

T_co = TypeVar("T_co")


def flat_pairs_to_dict(
    response: Union[Tuple[T_co, ...], List[T_co]]
) -> Dict[T_co, T_co]:
    """Creates a dict given a flat list of key/value pairs"""
    if isinstance(response, Dict):
        return response
    it = iter(response)
    return dict(zip(it, it))


def flat_pairs_to_ordered_dict(response: Iterable[T_co]) -> OrderedDict[StringT, T_co]:
    """Creates a dict given a flat list of key/value pairs"""
    it = iter(response)
    return cast(OrderedDict[StringT, T_co], OrderedDict(zip(it, it)))
