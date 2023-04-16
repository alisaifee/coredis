from __future__ import annotations

import warnings
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    AnyStr,
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    ClassVar,
    ContextManager,
    Coroutine,
    Dict,
    FrozenSet,
    Generator,
    Generic,
    Hashable,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    MutableSet,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    ValuesView,
)

from typing_extensions import (
    Deque,
    Final,
    Literal,
    OrderedDict,
    ParamSpec,
    Protocol,
    Self,
    TypedDict,
    TypeGuard,
    runtime_checkable,
)

from coredis.config import Config

_runtime_checks = False
_beartype_found = False

try:
    import beartype  # pyright: reportUnusedImport=false

    if not TYPE_CHECKING:
        from beartype.typing import (  # noqa: F811
            AbstractSet,
            Deque,
            Dict,
            FrozenSet,
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
            ValuesView,
        )
    _beartype_found = True
except ImportError:  # pragma: no cover
    pass

if Config.runtime_checks and not TYPE_CHECKING:  # pragma: no cover
    if _beartype_found:
        _runtime_checks = True
    else:
        warnings.warn(
            "Runtime checks were enabled via environment variable COREDIS_RUNTIME_CHECKS"
            " but could not import beartype"
        )

RUNTIME_TYPECHECKS = _runtime_checks

P = ParamSpec("P")
T_co = TypeVar("T_co", covariant=True)
R = TypeVar("R")


def safe_beartype(func: Callable[P, R]) -> Callable[P, R]:
    if TYPE_CHECKING:
        return func

    return beartype.beartype(func) if _beartype_found else func


def add_runtime_checks(func: Callable[P, R]) -> Callable[P, R]:
    if RUNTIME_TYPECHECKS and not TYPE_CHECKING:
        return safe_beartype(func)

    return func


class RedisError(Exception):
    """
    Base exception from which all other exceptions in coredis
    derive from.
    """


CommandArgList = List[Union[str, bytes, int, float]]


class Node(TypedDict):
    """
    Definition of a cluster node
    """

    host: str
    port: int


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

#: Restricted union of container types accepted as arguments to apis
#: that accept a variable number values for an argument (such as keys, values).
#: This is used instead of :class:`typing.Iterable` as the latter allows
#: :class:`str` to be passed in as valid values for :class:`Iterable[str]` or :class:`bytes`
#: to be passed in as a valid value for :class:`Iterable[bytes]` which is never the actual
#: expectation in the scope of coredis.
#: For example::
#:
#:     def length(values: Parameters[ValueT]) -> int:
#:         return len(list(values))
#:
#:     length(["1", 2, 3, 4])      # valid
#:     length({"1", 2, 3, 4})      # valid
#:     length(("1", 2, 3, 4))      # valid
#:     length({"1": 2}.keys())     # valid
#:     length({"1": 2}.values())   # valid
#:     length(map(str, range(10))) # valid
#:     length({"1": 2})            # invalid
#:     length("123")               # invalid
#:     length(b"123")              # invalid
Parameters = Union[
    List[T_co], AbstractSet[T_co], Tuple[T_co, ...], ValuesView[T_co], Iterator[T_co]
]

#: Mapping of primitives returned by redis
ResponsePrimitive = Optional[Union[StringT, int, float, bool]]

#: Represents the total structure of any response for a redis
#: command.
#:
#: This should preferably be represented by a recursive definition to allow for
#: Limitations in runtime type checkers (beartype) requires conditionally loosening
#: the definition with the use of  :class:`typing.Any` for now.

if TYPE_CHECKING:
    ResponseType = Union[
        ResponsePrimitive,
        List["ResponseType"],
        MutableSet[
            Union[
                ResponsePrimitive,
                Tuple[ResponsePrimitive, ...],
                FrozenSet[ResponsePrimitive],
            ]
        ],
        Dict[
            Union[
                ResponsePrimitive,
                Tuple[ResponsePrimitive, ...],
                FrozenSet[ResponsePrimitive],
            ],
            "ResponseType",
        ],
        RedisError,  # response errors get mapped to exceptions.
    ]
else:
    from typing import Any

    ResponseType = Union[
        ResponsePrimitive,
        List[Any],
        MutableSet[
            Union[
                ResponsePrimitive,
                Tuple[ResponsePrimitive, ...],
                FrozenSet[ResponsePrimitive],
            ]
        ],
        Dict[
            Union[
                ResponsePrimitive,
                Tuple[ResponsePrimitive, ...],
                FrozenSet[ResponsePrimitive],
            ],
            Any,
        ],
        RedisError,  # response errors get mapped to exceptions.
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
    "ContextManager",
    "Coroutine",
    "Deque",
    "Dict",
    "Final",
    "FrozenSet",
    "Generic",
    "Generator",
    "Hashable",
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
    "Node",
    "OrderedDict",
    "Optional",
    "Parameters",
    "ParamSpec",
    "Protocol",
    "ResponsePrimitive",
    "ResponseType",
    "runtime_checkable",
    "Sequence",
    "Self",
    "Set",
    "StringT",
    "Tuple",
    "Type",
    "TypeGuard",
    "TypedDict",
    "TypeVar",
    "Union",
    "ValueT",
    "ValuesView",
    "TYPE_CHECKING",
    "RUNTIME_TYPECHECKS",
]
