from __future__ import annotations

import dataclasses
import inspect
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
from types import GenericAlias, ModuleType, UnionType
from typing import (
    TYPE_CHECKING,
    Annotated,
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
    get_origin,
    get_type_hints,
    runtime_checkable,
)

from beartype import __version__ as beartype_version
from beartype import beartype
from packaging import version

if TYPE_CHECKING:
    infer_hint: Callable[..., Any]
    is_bearable: Callable[[Any, Any], bool]
    is_subhint: Callable[[Any, Any], bool]
else:
    if version.parse(beartype_version) < version.parse("0.22"):
        from beartype.door import infer_hint
    else:
        from beartype.bite import infer_hint

from beartype.door import is_bearable, is_subhint
from typing_extensions import (
    NotRequired,
    Self,
    TypeIs,
    Unpack,
)

from coredis.config import Config

RUNTIME_TYPECHECKS = Config.runtime_checks and not TYPE_CHECKING

P = ParamSpec("P")
T_co = TypeVar("T_co", covariant=True)
R = TypeVar("R")


def safe_beartype(func: Callable[P, R]) -> Callable[P, R]:
    return beartype(func)


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
KeyT = Annotated[str | bytes, "KeyT"]


class Serializable(Generic[R]):
    """
    Wrapper to be used to pass arbitrary types to redis commands
    to be eventually serialized by :class:`coredis.typing.TypeAdapter.serialize`

    Wrapping a value in :class:`Serializable` will pass type checking
    wherever a method expects a :class:`coredis.typing.ValueT` - however
    it will still fail if there is no serializer registered through the instance
    of :class:`coredis.typing.TypeAdapter` that is associated with the client.

    For example::

      class MyThing:
          ...

      client = coredis.Redis()

      # This will pass type checking but will fail with an :exc:`LookupError`
      # at runtime
      await client.set("fubar", coredis.typing.Serializable(MyThing()))

      # however, if a serializer is registered, the above would succeed
      @client.type_adapter.serializer
      def _(value: MyThing) -> str:
         ... # some way to convert it to a string
    """

    def __init__(self, value: R) -> None:
        self.value = value


AdaptableType = type | UnionType | GenericAlias


class TypeAdapter:
    """
    Used by the coredis clients :class:`~coredis.Redis` and :class:`~coredis.RedisCluster`
    through :paramref:`~coredis.Redis.type_adapter` for adapting complex types that require
    custom serialization/deserialization with redis commands.

    For example to use Decimal types with some common redis operations::

      from decimal import Decimal
      from typing import Any, Mapping, Iterable
      from coredis import Redis
      from coredis.typing import TypeAdapter, Serializable

      adapter = TypeAdapter()

      @adapter.serializer
      def decimal_to_str(value: Decimal) -> str:
          return str(value)

      @adapter.deserializer
      def value_to_decimal(value: str|bytes) -> Decimal:
          return Decimal(value.decode("utf-8") if isinstance(value, bytes) else value)

      @adapter.deserializer
      def list_to_decimal_list(items: Iterable[str|bytes]) -> list[Decimal]:
          return [value_to_decimal(value) for value in items]

      @adapter.deserializer
      def mapping_to_decimal_mapping(mapping: Mapping[str|bytes, str|bytes]) -> dict[str|bytes, Decimal]:
          return {key: value_to_decimal(value) for key, value in mapping.items()}

      client = coredis.Redis(type_adapter=adapter, decode_responses=True)
      await client.set("key", Serializable(Decimal(1.5)))
      await client.lpush("list", [Serializable(Decimal(1.5))])
      await client.hset("dict", {"first": Serializable(Decimal(1.5))})
      assert Decimal(1.5) == await client.get("key").transform(Decimal)
      assert [Decimal(1.5)] == await client.lrange("list", 0, 0).transform(list[Decimal])
      assert {"first": Decimal(1.5)} == await client.hgetall("dict").transform(dict[str, Decimal])
    """

    def __init__(
        self,
    ) -> None:
        self.__serializers: dict[
            AdaptableType,
            tuple[Callable[[Any], RedisValueT], int],
        ] = {}
        self.__deserializers: dict[
            AdaptableType,
            dict[AdaptableType, tuple[Callable[..., Any], int]],
        ] = {}
        self.__deserializer_cache: dict[
            tuple[AdaptableType, AdaptableType | GenericAlias],
            Callable[..., Any],
        ] = {}
        self.__serializer_cache: dict[AdaptableType, Callable[[Any], RedisValueT]] = {}

    @classmethod
    def format_type(cls, type_like: AdaptableType) -> str:
        if get_origin(type_like):
            return str(type_like)
        else:
            return getattr(type_like, "__name__", str(type_like))

    def register(
        self,
        type: type[R] | UnionType,
        serializer: Callable[[R], RedisValueT],
        deserializer: Callable[[Any], R],
        deserializable_type: type = object,
    ) -> None:
        """
        Register both a serializer and a deserializer for :paramref:`type`

        :param type: The type that should be serialized/deserialized
        :param serializer: a function that receives an instance of :paramref:`type`
         and returns a value of type :data:`coredis.typing.RedisValueT`
        :param deserializer: a function that accepts the return types from
         the redis commands that are expected to be used when deserializing
         to :paramref:`type`.
        :param deserializable_type: the types of values :paramref:`deserializer` should
         be considered for
        """
        self.register_serializer(type, serializer)
        self.register_deserializer(type, deserializer, deserializable_type)

    def register_serializer(
        self,
        serializable_type: type[R] | UnionType,
        serializer: Callable[[R], RedisValueT],
    ) -> None:
        """
        Register a serializer for :paramref:`type`

        :param type: The type that will be serialized
        :param serializer: a function that receives an instance of :paramref:`type`
         and returns a value of type :data:`coredis.typing.RedisValueT`
        """
        self.__serializers.setdefault(serializable_type, (serializer, 0))
        self.__serializer_cache.clear()

    def register_deserializer(
        self,
        deserialized_type: type[R] | UnionType,
        deserializer: Callable[[Any], R],
        deserializable_type: AdaptableType = object,
    ) -> None:
        """
        Register a deserializer for :paramref:`type` and automatically register
        deserializers for common collection types that use this type.

        :param type: The type that should be deserialized
        :param deserializer: a function that accepts the return types from
         the redis commands that are expected to be used when deserializing
         to :paramref:`type`.
        :param deserializable_type: the types of values :paramref:`deserializer` should
         be considered for
        """

        def register_collection_deserializer(
            collection_type: AdaptableType,
            deserializable_type: AdaptableType,
            deserializer: Callable[[Any], Any],
        ) -> None:
            self.__deserializers.setdefault(collection_type, {}).setdefault(
                deserializable_type,
                (deserializer, -1),
            )

        # Register the base deserializer
        self.__deserializers.setdefault(deserialized_type, {})[deserializable_type or object] = (
            deserializer,
            0,
        )

        # Register collection deserializers
        register_collection_deserializer(
            GenericAlias(list, (deserialized_type,)),
            GenericAlias(Iterable, deserializable_type),
            lambda v: [deserializer(item) for item in v],
        )
        register_collection_deserializer(
            GenericAlias(set, (deserialized_type,)),
            GenericAlias(Iterable, deserializable_type),
            lambda v: {deserializer(item) for item in v},
        )
        register_collection_deserializer(
            GenericAlias(tuple, (deserialized_type, ...)),
            GenericAlias(Iterable, deserializable_type),
            lambda v: tuple([deserializer(item) for item in v]),
        )

        # Register dictionary deserializers for existing types
        for t in list(self.__deserializers):
            if t != deserialized_type:
                for rt in list(self.__deserializers[t]):
                    _deserializer, priority = self.__deserializers[t][rt]
                    if priority >= 0:
                        register_collection_deserializer(
                            GenericAlias(dict, (t, deserialized_type)),
                            GenericAlias(Mapping, (rt, deserializable_type)),
                            lambda m, key_deserializer=_deserializer: {  # type: ignore
                                key_deserializer(k): deserializer(v) for k, v in m.items()
                            },
                        )
                        register_collection_deserializer(
                            GenericAlias(dict, (deserialized_type, t)),
                            GenericAlias(Mapping, (deserializable_type, rt)),
                            lambda m, value_deserializer=_deserializer: {  # type: ignore
                                deserializer(k): value_deserializer(v) for k, v in m.items()
                            },
                        )

        # Register dictionary deserializers for primitive types
        for t in {bytes, str}:
            register_collection_deserializer(
                GenericAlias(dict, (t, deserialized_type)),
                GenericAlias(Mapping, (t, deserializable_type)),
                lambda v: {k: deserializer(v) for k, v in v.items()},
            )
            register_collection_deserializer(
                GenericAlias(dict, (deserialized_type, t)),
                GenericAlias(Mapping, (deserializable_type, t)),
                lambda v: {deserializer(k): v for k, v in v.items()},
            )

        self.__deserializer_cache.clear()

    def serializer(self, func: Callable[[R], RedisValueT]) -> Callable[[R], RedisValueT]:
        """
        Decorator for registering a serializer

        :param func: A serialization function that accepts an instance of
         type `R` and returns one of the types defined by :data:`coredis.typing.RedisValueT`
         The acceptable  serializable types are inferred
         from the annotations in the function signature.

        :raises ValueError: when the appropriate serializable type cannot be
         inferred.
        """
        if (parameters := list(inspect.signature(func).parameters.keys())) and (
            input_hint := get_type_hints(func).get(parameters[0])
        ):
            self.register_serializer(input_hint, func)
            return func
        else:
            raise ValueError(
                "Unable to infer custom input type from decorated function. Check type annotations."
            )

    def deserializer(self, func: Callable[[Any], R]) -> Callable[[Any], R]:
        """
        Decorator for registering a deserializer

        :param func: A deserialization function that returns an instance of
         type `R` that can be used with :meth:`deserialize`. The acceptable
         deserializable types and the expected deserialized type are inferred
         from the annotations in the function signature.

        :raises ValueError: when the appropriate input/output types cannot be
         inferred.
        """
        if (
            (parameters := list(inspect.signature(func).parameters.keys()))
            and (input_hint := get_type_hints(func).get(parameters[0]))
        ) and (response_type := get_type_hints(func).get("return")):
            self.register_deserializer(response_type, func, input_hint)
            return func
        else:
            raise ValueError(
                "Unable to infer response type from decorated function. Check annotations."
            )

    def serialize(self, value: Serializable[R]) -> RedisValueT:
        """
        Serializes :paramref:`value` into one of the types represented by
        :data:`~coredis.typing.RedisValueT` using a serializer registered
        via :meth:`register_serializer` or decorated by :meth:`serializer`.

        :param: a value wrapped in :class:`coredis.typing.Serializable`
        """
        value_type = cast(AdaptableType, infer_hint(value.value))
        if not (transform_function := self.__serializer_cache.get(value_type, None)):
            candidate: tuple[AdaptableType, Callable[[R], RedisValueT] | None] = (object, None)

            for t in self.__serializers:
                if is_bearable(value.value, t):
                    if not candidate[1] or is_subhint(t, candidate[0]):
                        candidate = (t, self.__serializers[t][0])
            if candidate[1]:
                transform_function = candidate[1]
                self.__serializer_cache[value_type] = transform_function
        if not transform_function:
            raise LookupError(
                f"No registered serializer to serialize {self.format_type(value_type)}"
            )
        return transform_function(value.value)

    def deserialize(self, value: Any, return_type: type[R]) -> R:
        """
        Deserializes :paramref:`value` into an instance of :paramref:`return_type`
        using a deserializer registered via :meth:`register_deserializer` or decorated
        by :meth:`deserializer`.

        :param value: the value to be deserialized (typically something returned by one of
         the redis commands)
        :param return_type: The type to deserialize to
        """
        value_type = cast(AdaptableType, infer_hint(value))
        if not (deserializer := self.__deserializer_cache.get((value_type, return_type), None)):
            if exact_match := self.__deserializers.get(return_type, {}).get(value_type, None):
                deserializer = exact_match[0]
            else:
                candidate: tuple[AdaptableType, AdaptableType, Callable[[Any], R] | None, int] = (
                    object,
                    object,
                    None,
                    -100,
                )
                for registered_type, transforms in self.__deserializers.items():
                    if is_subhint(return_type, registered_type):
                        for expected_value_type in transforms:
                            if (
                                is_bearable(value, expected_value_type)
                                and is_subhint(registered_type, candidate[0])
                                and is_subhint(expected_value_type, candidate[1])
                                and transforms[expected_value_type][1] >= candidate[3]
                            ):
                                candidate = (
                                    registered_type,
                                    expected_value_type,
                                    transforms[expected_value_type][0],
                                    transforms[expected_value_type][1],
                                )
                deserializer = candidate[2]
        if deserializer:
            deserialized = deserializer(value)
            if RUNTIME_TYPECHECKS and not is_subhint(
                transformed_type := cast(type, infer_hint(deserialized)), return_type
            ):
                raise TypeError(
                    f"Invalid deserializer. Requested {self.format_type(return_type)} but deserializer returned {self.format_type(transformed_type)}"
                )
            self.__deserializer_cache[(value_type, return_type)] = deserializer
            return deserialized
        elif is_subhint(value_type, return_type):
            return cast(R, value)
        else:
            raise LookupError(
                f"No registered deserializer to convert {self.format_type(value_type)} to {self.format_type(return_type)}"
            )


#: Represents the different python primitives that are accepted
#: as input parameters for commands that can be used with loosely
#: defined types. These will eventually be serialized before being
#: sent to redis.
#:
#: Additionally any object wrapped in a :class:`Serializable` will be
#: accepted and will be serialized using an appropriate type adapter
#: registered with the client. See :ref:`api/typing:custom types` for more details.
ValueT = str | bytes | int | float | Serializable[Any]

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
    from ._py_312_typing import JsonType, ResponseType
else:
    from ._py_311_typing import JsonType, ResponseType


__all__ = [
    "Serializable",
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
    "TypeIs",
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
