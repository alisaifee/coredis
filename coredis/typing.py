from __future__ import annotations

import dataclasses
import sys
import warnings
from collections import OrderedDict
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Generator,
    Hashable,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    MutableSequence,
    MutableSet,
    Sequence,
    Set,
    ValuesView,
)
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    AnyStr,
    ClassVar,
    Final,
    Generic,
    Literal,
    NamedTuple,
    ParamSpec,
    Protocol,
    TypedDict,
    TypeGuard,
    TypeVar,
    runtime_checkable,
)

from typing_extensions import NotRequired, Self, Unpack

from coredis.config import Config

_runtime_checks = False
_beartype_found = False

try:
    import beartype

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


class Node(TypedDict):
    """
    Definition of a cluster node
    """

    host: str
    port: int


class RedisCommandP(Protocol):
    """
    Protocol of a redis command with all associated arguments
    converted into the shape expected by the redis server.
    Used by :meth:`~coredis.Redis.execute_command`
    """

    #: The name of the redis command
    name: bytes
    #: All arguments to be passed to the command
    arguments: tuple[ValueT, ...]


@dataclasses.dataclass
class RedisCommand:
    """
    Convenience data class that conforms to :class:`~coredis.typing.RedisCommandP`
    """

    #: The name of the redis command
    name: bytes
    #: All arguments to be passed to the command
    arguments: tuple[ValueT, ...]


class ExecutionParameters(TypedDict):
    """
    Extra parameters that can be passed to :meth:`~coredis.Redis.execute_command`
    """

    #: Whether to decode the response
    #: (ignoring the value of :paramref:`~coredis.Redis.decode_responses`)
    decode: NotRequired[bool]
    slot_arguments_range: NotRequired[tuple[int, int]]


#: Represents the acceptable types of a redis key
KeyT = str | bytes

#: Represents the different python primitives that are accepted
#: as input parameters for commands that can be used with loosely
#: defined types. These are encoded using the configured encoding
#: before being transmitted.
ValueT = str | bytes | int | float

#: The canonical type used for input parameters that represent "strings"
#: that are transmitted to redis.
StringT = str | bytes

CommandArgList = list[ValueT]


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
Parameters = list[T_co] | Set[T_co] | tuple[T_co, ...] | ValuesView[T_co] | Iterator[T_co]

#: Primitives returned by redis
ResponsePrimitive = StringT | int | float | bool | None

if sys.version_info >= (3, 12):
    from ._py_312_typing import HashableResponseType, JsonType, ResponseType
else:
    from ._py_311_typing import HashableResponseType, JsonType, ResponseType


__all__ = [
    "AnyStr",
    "AsyncIterator",
    "AsyncGenerator",
    "Awaitable",
    "Callable",
    "ClassVar",
    "CommandArgList",
    "Coroutine",
    "Final",
    "Generic",
    "Generator",
    "Hashable",
    "HashableResponseType",
    "Iterable",
    "Iterator",
    "JsonType",
    "KeyT",
    "Literal",
    "Mapping",
    "ModuleType",
    "MutableMapping",
    "MutableSet",
    "MutableSequence",
    "NamedTuple",
    "Node",
    "NotRequired",
    "OrderedDict",
    "Parameters",
    "ParamSpec",
    "Protocol",
    "RedisCommand",
    "RedisCommandP",
    "ExecutionParameters",
    "ResponsePrimitive",
    "ResponseType",
    "runtime_checkable",
    "Self",
    "Sequence",
    "StringT",
    "TypeGuard",
    "TypedDict",
    "TypeVar",
    "Unpack",
    "ValueT",
    "ValuesView",
    "TYPE_CHECKING",
    "RUNTIME_TYPECHECKS",
]
