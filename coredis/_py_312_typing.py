from __future__ import annotations

from .typing import MutableSet, RedisError, ResponsePrimitive

#: Represents the total structure of any response for a redis
#: command.
type ResponseType = (
    ResponsePrimitive
    | list[ResponseType]
    | MutableSet[ResponsePrimitive | tuple[ResponseType, ...] | frozenset[ResponsePrimitive]]
    | dict[
        ResponsePrimitive | tuple[ResponseType, ...] | frozenset[ResponsePrimitive],
        ResponseType,
    ]
    | RedisError
)
#: Type alias for valid python types that can be represented as json
type JsonType = str | int | float | bool | dict[str, JsonType] | list[JsonType] | None
