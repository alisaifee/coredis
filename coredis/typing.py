from __future__ import annotations

import dataclasses
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
    Any,
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
    cast,
    get_args,
    runtime_checkable,
)

from beartype import BeartypeConf, beartype
from beartype.door import die_if_unbearable, infer_hint, is_bearable, is_subhint
from typing_extensions import NotRequired, Self, Unpack

from coredis.config import Config

_runtime_checks = False

RUNTIME_TYPECHECKS = Config.runtime_checks and not TYPE_CHECKING

P = ParamSpec("P")
T_co = TypeVar("T_co", covariant=True)
R = TypeVar("R")


def safe_beartype(func: Callable[P, R]) -> Callable[P, R]:
    return beartype(func) if RUNTIME_TYPECHECKS else func


def add_runtime_checks(func: Callable[P, R]) -> Callable[P, R]:
    if RUNTIME_TYPECHECKS and not TYPE_CHECKING:
        return beartype(func)
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


#: Represents the acceptable types of a redis key
KeyT = str | bytes


class TypeAdapter:
    """
    TODO: Add stuff
    """

    def __init__(
        self,
        to_redis: dict[type, Callable[[Any], RedisValueT]] | None = None,
        from_redis: dict[type, dict[type, Callable[..., Any]]] | None = None,
    ) -> None:
        self.to_redis: dict[type, Callable[[Any], RedisValueT]] = to_redis or {}
        self.from_redis: dict[type, dict[type, Callable[..., Any]]] = from_redis or {}
        self.bear_conf = BeartypeConf(violation_door_type=TypeError)

    def register_adapter(
        self,
        py_type: type[R],
        to_redis: Callable[[R], RedisValueT],
        from_redis: Callable[..., Any],
    ) -> None:
        """
        TODO: Add stuff
        """
        self.register_input_adapter(py_type, to_redis)
        self.register_response_adapter(py_type, from_redis)

    def register_input_adapter(
        self,
        py_type: type[R],
        to_redis: Callable[[R], RedisValueT],
    ) -> None:
        """
        TODO: Add stuff
        """
        self.to_redis[py_type] = to_redis

    def register_response_adapter(
        self, py_type: type[R], from_redis: Callable[..., R], redis_type: type = object
    ) -> None:
        """
        TODO: Add stuff
        """
        self.from_redis.setdefault(py_type, {})[redis_type] = from_redis

    def to_redis_value(self, value: CustomInputT[R]) -> RedisValueT:
        """
        TODO: Add stuff
        """
        for t in self.to_redis:
            if is_subhint(infer_hint(value.value), t):
                return self.to_redis[t](value.value)
        raise TypeError(
            f"No type adapter registered to serialize {type(value.value)} for value {value.value}"
        )

    def from_redis_value(self, value: Any, return_type: type[R]) -> R:
        """
        Find and apply the most specific matching adapter.
        Preference order:
        1. Exact match (e.g., list[int] to list[int])
        2. Subhint match (e.g., list[int | str] → list[Any])
        3. Fallback to `object`
        """
        transform_function = None
        candidates = []

        for registered_type in self.from_redis:
            if registered_type is object:
                continue  # handle object fallback later
            if is_subhint(return_type, registered_type):
                candidates.append(registered_type)

        # Prioritize exact matches, then more specific subhints
        candidates.sort(
            key=lambda t: (
                t != return_type,  # exact match first (False < True)
                -self.hint_specificity_score(t),  # more specific types before generic ones
            )
        )

        for candidate in candidates:
            redis_handlers = self.from_redis[candidate]
            for redis_type in redis_handlers:
                if redis_type is object or is_bearable(value, redis_type):
                    transform_function = redis_handlers[redis_type]
                    break
            if transform_function:
                break

        # Fallback to object adapter if no match
        if not transform_function:
            for candidate, redis_handlers in self.from_redis.items():
                if candidate is object:
                    transform_function = redis_handlers.get(object, None)
                    if transform_function:
                        break

        if not transform_function:
            raise TypeError(f"No type adapter registered to deserialize into: {return_type}")

        transformed = transform_function(value)
        die_if_unbearable(
            transformed, return_type, conf=self.bear_conf, exception_prefix="Transform mismatch: "
        )
        return cast(R, transformed)

    @classmethod
    def hint_specificity_score(cls, hint: type) -> int:
        """
        Higher score = more specific type
        """
        if args := get_args(hint):
            return 1 + sum(cls.hint_specificity_score(arg) for arg in args if isinstance(arg, type))
        return 0


class CustomInputT(Generic[R]):
    """
    TODO: add stuff
    """

    def __init__(self, value: R) -> None:
        self.value = value


#: Represents the different python primitives that are accepted
#: as input parameters for commands that can be used with loosely
#: defined types. These are encoded using the configured encoding
#: before being transmitted. Additionally any object wrapped in a
#: :class:`CustomInputT` will be accepted and will be serialized
#: using an appropriate type adapter registered with the client.
#: TODO: add more details here.
ValueT = str | bytes | int | float | CustomInputT[Any]

#: The canonical type used for input parameters that represent "strings"
#: that are transmitted to redis.
StringT = str | bytes

CommandArgList = list[ValueT]

#: Primitive types that we can expect to be sent to redis with
#: simple serialization. The internals of coredis
#: pass around arguments to redis commands as this type.
RedisValueT = str | bytes | int | float

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

#: Mapping of primitives returned by redis
ResponsePrimitive = StringT | int | float | bool | None

#: Represents the total structure of any response for a redis
#: command.
#:
#: This should preferably be represented by a recursive definition to allow for
#: Limitations in runtime type checkers (beartype) requires conditionally loosening
#: the definition with the use of  :class:`typing.Any` for now.

if TYPE_CHECKING:
    ResponseType = (
        ResponsePrimitive
        | list["ResponseType"]
        | MutableSet[
            ResponsePrimitive | tuple[ResponsePrimitive, ...] | frozenset[ResponsePrimitive]
        ]
        | dict[
            ResponsePrimitive | tuple[ResponsePrimitive, ...] | frozenset[ResponsePrimitive],
            "ResponseType",
        ]
        | RedisError  # response errors get mapped to exceptions.
    )
else:
    from typing import Any

    ResponseType = (
        ResponsePrimitive
        | list[Any]
        | MutableSet[
            ResponsePrimitive | tuple[ResponsePrimitive, ...] | frozenset[ResponsePrimitive]
        ]
        | dict[
            ResponsePrimitive | tuple[ResponsePrimitive, ...] | frozenset[ResponsePrimitive],
            Any,
        ]
        | RedisError  # response errors get mapped to exceptions.
    )

#: Type alias for valid python types that can be represented as json
JsonType = str | int | float | bool | dict[str, Any] | list[Any] | None


@dataclasses.dataclass
class RedisCommand:
    """
    Structure of a redis command with arguments
    to be used by :meth:`~coredis.Redis.execute_command`
    """

    name: bytes
    arguments: tuple[RedisValueT, ...]


class ExecutionParameters(TypedDict):
    """
    Extra parameters that can be passed to :meth:`~coredis.Redis.execute_command`
    """

    #: Whether to decode the response
    #: (ignoring the value of :paramref:`~coredis.Redis.decode_responses`)
    decode: NotRequired[bool]
    slot_arguments_range: NotRequired[tuple[int, int]]


__all__ = [
    "CustomInputT",
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
    "RedisValueT",
    "ValuesView",
    "TYPE_CHECKING",
    "TypeAdapter",
    "ValueT",
    "RUNTIME_TYPECHECKS",
]
