from __future__ import annotations

from collections.abc import Hashable

from .typing import MutableSet, RedisError, ResponsePrimitive

#: Type alias for valid python types that can be represented as json
type JsonType = str | int | float | bool | dict[str, JsonType] | list[JsonType] | None

#: Represents the total structure of any response for any redis command.
type ResponseType = (
    ResponsePrimitive
    | RedisError
    | list[ResponseType]
    | MutableSet[Hashable]
    | dict[Hashable, ResponseType]
)
