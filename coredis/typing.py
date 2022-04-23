from __future__ import annotations

import os
import warnings
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    AnyStr,
    AsyncGenerator,
    Awaitable,
    Callable,
    ClassVar,
    Coroutine,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    MutableMapping,
    MutableSequence,
    MutableSet,
    NamedTuple,
    Optional,
    Protocol,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from typing_extensions import (
    Deque,
    NotRequired,
    OrderedDict,
    ParamSpec,
    Required,
    Self,
    TypeAlias,
    TypedDict,
    TypeGuard,
)

_runtime_checks = False

if os.environ.get("COREDIS_RUNTIME_CHECKS", "").lower() in ["1", "true", "t"]:
    try:
        import beartype

        if not TYPE_CHECKING:
            from beartype.typing import (  # noqa: F811
                AbstractSet,
                Deque,
                Dict,
                Iterable,
                Iterator,
                List,
                Mapping,
                MutableMapping,
                MutableSequence,
                MutableSet,
                OrderedDict,
                Sequence,
                Set,
                Tuple,
                TypedDict,
            )

        _runtime_checks = True
    except ImportError:  # noqa
        warnings.warn("Runtime checks were requested but could not import beartype")

RUNTIME_TYPECHECKS = _runtime_checks

P = ParamSpec("P")
R = TypeVar("R")


def add_runtime_checks(func: Callable[P, R]) -> Callable[P, R]:
    if RUNTIME_TYPECHECKS and not TYPE_CHECKING:
        return beartype.beartype(func)

    return func


CommandArgList = List[Union[str, bytes, int, float]]


KeyT: TypeAlias = Union[str, bytes]
ValueT: TypeAlias = Union[str, bytes, int, float]
StringT: TypeAlias = KeyT

ResponsePrimitive: TypeAlias = Optional[Union[StringT, int, float, bool]]

# TODO: use a recursive definition of ResponseType
#
# Currently unsupported by mypy & beartype.
#    ResponseType = Union[
#        ResponsePrimitive,
#        List["ResponseType"],
#        List[ResponsePrimitive],
#        Set[ResponsePrimitive],
#        #Dict[ResponsePrimitive, ResponsePrimitive],
#        Dict[ResponsePrimitive, "ResponseType"],
#        BaseException,
#    ]
ResponseType = Union[
    ResponsePrimitive,
    List[Any],
    Set[ResponsePrimitive],
    Dict[ResponsePrimitive, Any],
    BaseException,
]


class PubSubMessage(TypedDict):
    type: str
    pattern: Optional[StringT]
    channel: StringT
    data: StringT


__all__ = [
    "AbstractSet",
    "AnyStr",
    "AsyncGenerator",
    "Awaitable",
    "Callable",
    "ClassVar",
    "CommandArgList",
    "Coroutine",
    "Deque",
    "Dict",
    "Generic",
    "KeyT",
    "Iterable",
    "Iterator",
    "List",
    "Literal",
    "Mapping",
    "MutableMapping",
    "MutableSet",
    "MutableSequence",
    "NamedTuple",
    "NotRequired",
    "OrderedDict",
    "Optional",
    "ParamSpec",
    "Protocol",
    "Required",
    "ResponsePrimitive",
    "ResponseType",
    "Self",
    "Sequence",
    "Set",
    "StringT",
    "Tuple",
    "Type",
    "TypeGuard",
    "TypedDict",
    "TypeVar",
    "Union",
    "ValueT",
    "TYPE_CHECKING",
    "RUNTIME_TYPECHECKS",
]
