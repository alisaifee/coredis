from __future__ import annotations

import dataclasses
import sys
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
    runtime_checkable,
)

from beartype import beartype
from beartype.door import infer_hint, is_subhint
from typing_extensions import (
    NotRequired,
    Self,
    Unpack,
)

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


class RedisCommandP(Protocol):
    """
    Protocol of a redis command with all associated arguments
    converted into the shape expected by the redis server.
    Used by :meth:`~coredis.Redis.execute_command`
    """

    #: The name of the redis command
    name: bytes
    #: All arguments to be passed to the command
    arguments: tuple[RedisValueT, ...]


@dataclasses.dataclass
class RedisCommand:
    """
    Convenience data class that conforms to :class:`~coredis.typing.RedisCommandP`
    """

    #: The name of the redis command
    name: bytes
    #: All arguments to be passed to the command
    arguments: tuple[RedisValueT, ...]


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


class TypeAdapter:
    """
    TODO: Add stuff
    """

    def __init__(
        self,
        to_redis: dict[type, Callable[[Any], RedisValueT]] | None = None,
        from_redis: dict[type, dict[type, Callable[..., Any]]] | None = None,
    ) -> None:
        self.__to_redis: dict[type, Callable[[Any], RedisValueT]] = to_redis or {}
        self.__from_redis: dict[type, dict[type, Callable[..., Any]]] = from_redis or {}
        self.__transformer_cache: dict[tuple[type, type], Callable[..., Any]] = {}

    def register_adapter(
        self,
        custom_type: type[R],
        to_redis: Callable[[R], RedisValueT],
        from_redis: Callable[[Any], R],
        from_redis_type: type = object,
    ) -> None:
        """
        TODO: Add stuff
        """
        self.register_input_adapter(custom_type, to_redis)
        self.register_response_adapter(custom_type, from_redis, from_redis_type)

    def register_input_adapter(
        self,
        custom_type: type[R],
        to_redis: Callable[[R], RedisValueT],
    ) -> None:
        """
        TODO: Add stuff
        """
        self.__to_redis[custom_type] = to_redis

    def register_response_adapter(
        self,
        custom_type: type[R],
        from_redis: Callable[[Any], R],
        from_redis_type: type = object,
    ) -> None:
        """
        TODO: Add stuff
        """
        self.__from_redis.setdefault(custom_type, {})[from_redis_type or object] = from_redis

    def to_redis_value(self, value: CustomInputT[R]) -> RedisValueT:
        """
        TODO: Add stuff
        """

        for t in self.__to_redis:
            if is_subhint(infer_hint(value.value), t):
                return self.__to_redis[t](value.value)
        raise TypeError(
            f"No type adapter registered to serialize {infer_hint(value.value)} for value {value.value}"
        )

    def from_redis_value(self, value: Any, return_type: type[R]) -> R:
        value_type = cast(type, infer_hint(value))
        if not (
            transform_function := self.__transformer_cache.get((value_type, return_type), None)
        ):
            if return_type in self.__from_redis and value_type in self.__from_redis[return_type]:
                transform_function = self.__from_redis[return_type][value_type]
            else:
                candidate: tuple[type, type, Callable[[Any], R] | None] = (object, object, None)
                for registered_type, transforms in self.__from_redis.items():
                    if is_subhint(return_type, registered_type):
                        for expected_value_type in transforms:
                            if (
                                is_subhint(value_type, expected_value_type)
                                and is_subhint(registered_type, candidate[0])
                                and is_subhint(expected_value_type, candidate[1])
                            ):
                                candidate = (
                                    registered_type,
                                    expected_value_type,
                                    transforms[expected_value_type],
                                )
                transform_function = candidate[-1]
                # TODO: see if this is worth it, i.e. allowing users to pass in a type which
                # can also be used as a constructor (for example, int, float etc...)
                #
                # if not transform_function and callable(return_type) and type(return_type) is type:
                #    transform_function = cast(Callable[[Any], R], return_type)
        if transform_function:
            transformed = transform_function(value)
            transformed_type = infer_hint(transformed)
            if not is_subhint(transformed_type, return_type) and RUNTIME_TYPECHECKS:
                raise TypeError(
                    f"Invalid transform. Requested {return_type} but transform returned {transformed_type}"
                )
            self.__transformer_cache[(value_type, return_type)] = transform_function
            return transformed
        else:
            raise TypeError(
                f"No type adapter registered to deserialize {infer_hint(value)} into {return_type}"
            )


class CustomInputT(Generic[R]):
    """
    TODO: add stuff
    """

    def __init__(self, value: R) -> None:
        self.value = value


#: Represents the different python primitives that are accepted
#: as input parameters for commands that can be used with loosely
#: defined types. These will eventually be serialized before being
#: sent to redis.
#:
#: Additionally any object wrapped in a :class:`CustomInputT` will be
#: accepted and will be serialized using an appropriate type adapter
#: registered with the client.
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

#: Primitives returned by redis
ResponsePrimitive = StringT | int | float | bool | None

if sys.version_info >= (3, 12):
    from ._py_312_typing import HashableResponseType, JsonType, ResponseType
else:
    from ._py_311_typing import HashableResponseType, JsonType, ResponseType


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
    "RedisValueT",
    "ValuesView",
    "TYPE_CHECKING",
    "TypeAdapter",
    "ValueT",
    "RUNTIME_TYPECHECKS",
]
