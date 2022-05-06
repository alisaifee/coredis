from __future__ import annotations

import os
import warnings
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    AnyStr,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    ClassVar,
    Coroutine,
    Dict,
    Generator,
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
    OrderedDict,
    ParamSpec,
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
if TYPE_CHECKING:
    import coredis.exceptions

P = ParamSpec("P")
R = TypeVar("R")


def add_runtime_checks(func: Callable[P, R]) -> Callable[P, R]:
    if RUNTIME_TYPECHECKS and not TYPE_CHECKING:
        return beartype.beartype(func)

    return func


CommandArgList = List[Union[str, bytes, int, float]]


#: Represents the acceptable types of a redis key
KeyT = Union[str, bytes]

#: Represents the different python primitives that are accepted
#: as input parameters for commands that can be used with loosely
#: defined types. These are encoded using the configured encoding
#: before being transmitted.
ValueT = Union[str, bytes, int, float]

#: The canonical type used for input parameters that represent "strings"
#: that are transmitted to redis.
StringT = Union[str, bytes]

#: Mapping of primitives returned by redis
ResponsePrimitive: TypeAlias = Optional[Union[StringT, int, float, bool]]

#: Represents the total structure of any response for a redis
#: command.
#:
#: This should preferably be represented by a recursive definition to allow for
#: nested collections, however limitations in both static and runtime
#: type checkers (mypy & beartype) requires loosening the definition with the use of
#: :class:`typing.Any` for now.
#:
#: Ideally the representation should be::
#:
#:    ResponseType = Union[
#:          ResponsePrimitive,
#:          List["ResponseType"],
#:          Set[ResponsePrimitive],
#:          Dict[ResponsePrimitive, "ResponseType"],
#:          "coredis.exceptions.RedisError", # response errors get mapped to exceptions.
#:      ]
ResponseType = Union[
    ResponsePrimitive,
    List[Any],
    Set[ResponsePrimitive],
    Dict[ResponsePrimitive, Any],
    "coredis.exceptions.RedisError",
]

__all__ = [
    "AbstractSet",
    "AnyStr",
    "AsyncIterator",
    "AsyncGenerator",
    "Awaitable",
    "Callable",
    "ClassVar",
    "CommandArgList",
    "Coroutine",
    "Deque",
    "Dict",
    "Generic",
    "Generator",
    "Iterable",
    "Iterator",
    "KeyT",
    "List",
    "Literal",
    "Mapping",
    "MutableMapping",
    "MutableSet",
    "MutableSequence",
    "NamedTuple",
    "OrderedDict",
    "Optional",
    "ParamSpec",
    "Protocol",
    "ResponsePrimitive",
    "ResponseType",
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
