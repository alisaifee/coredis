from __future__ import annotations

from .typing import MutableSet, RedisError, ResponsePrimitive

#: Represents the structure of hashable response types (i.e. those that can
#: be members of sets or keys for maps)
type HashableResponseType = (
    ResponsePrimitive | tuple[HashableResponseType, ...] | frozenset[HashableResponseType]
)

#: Represents the total structure of any response for any redis command.
type ResponseType = (
    ResponsePrimitive
    | RedisError
    | list[ResponseType]
    | MutableSet[HashableResponseType]
    | dict[HashableResponseType, ResponseType]
)

#: Type alias for valid python types that can be represented as json
type JsonType = str | int | float | bool | dict[str, JsonType] | list[JsonType] | None
