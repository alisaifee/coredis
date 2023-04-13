from __future__ import annotations

from typing import cast

from coredis.typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    OrderedDict,
    StringT,
    Tuple,
    TypeVar,
    Union,
)

T_co = TypeVar("T_co")


def flat_pairs_to_dict(
    response: Union[Tuple[T_co, ...], List[T_co]],
    value_transform: Optional[Callable[..., T_co]] = None,
) -> Dict[T_co, T_co]:
    """Creates a dict given a flat list of key/value pairs"""
    if isinstance(response, Dict):
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
