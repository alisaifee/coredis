from __future__ import annotations

from collections.abc import Hashable

from .typing import StringT, MutableSet, RedisError

#: Type alias for valid python types that can be represented as json
type JsonType = str | int | float | bool | dict[str, JsonType] | list[JsonType] | None

#: Primitives returned by redis
type ResponsePrimitive = StringT | int | float | bool | None

#: Represents the total structure of any response for any redis command.
type ResponseType = (
    ResponsePrimitive
    | RedisError
    | list[ResponseType]
    | MutableSet[Hashable]
    | dict[Hashable, ResponseType]
)
