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
    Final,
    Generator,
    Generic,
    Hashable,
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
    ValuesView,
)

from typing_extensions import (
    Deque,
    OrderedDict,
    ParamSpec,
    Self,
    TypeAlias,
    TypedDict,
    TypeGuard,
)

_runtime_checks = False
_beartype_found = False

try:
    import beartype  # pyright: reportUnusedImport=false

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
            ValuesView,
        )

    _beartype_found = True
except ImportError:  # pragma: no cover
    pass

if (
    os.environ.get("COREDIS_RUNTIME_CHECKS", "").lower() in ["1", "true", "t"]
    and not TYPE_CHECKING
):  # pragma: no cover
    if _beartype_found:
        _runtime_checks = True
    else:
        warnings.warn(
            "Runtime checks were enabled via environment variable COREDIS_RUNTIME_CHECKS"
            " but could not import beartype"
        )

RUNTIME_TYPECHECKS = _runtime_checks

if TYPE_CHECKING:
    import coredis.exceptions

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


CommandArgList = List[Union[str, bytes, int, float]]


class Node(TypedDict):
    """
    Definition of a cluster node
    """

    host: str
    port: int
    name: str
    server_type: Optional[str]
    node_id: Optional[str]


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
ResponsePrimitive: TypeAlias = Optional[Union[StringT, int, float, bool]]

#: Represents the total structure of any response for a redis
#: command.
#:
#: This should preferably be represented by a recursive definition to allow for
#: nested containers, however limitations in both static and runtime
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
    MutableSet[Union[ResponsePrimitive, Tuple[ResponsePrimitive, ...]]],
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
    "Final",
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
