from __future__ import annotations

from typing import Any, TypeAlias

from .typing import MutableSet, RedisError, ResponsePrimitive

#: Represents the structure of hashable response types (i.e. those that can
#: be members of sets or keys for maps)
HashableResponseType: TypeAlias = (
    ResponsePrimitive | tuple[ResponsePrimitive, ...] | frozenset[ResponsePrimitive]
)

#: Represents the total structure of any response for any redis command.
ResponseType: TypeAlias = (
    ResponsePrimitive
    | list[Any]
    | MutableSet[HashableResponseType]
    | dict[
        HashableResponseType,
        Any,
    ]
    | RedisError
)
#: Type alias for valid python types that can be represented as json
JsonType: TypeAlias = str | int | float | bool | dict[str, Any] | list[Any] | None
