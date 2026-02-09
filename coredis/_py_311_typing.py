from __future__ import annotations

from collections.abc import Hashable
from typing import Any

from .typing import MutableSet, RedisError, StringT, TypeAliasType

#: Primitives returned by redis
ResponsePrimitive = TypeAliasType("ResponsePrimitive", StringT | int | float | bool | None)

#: Represents the total structure of any response for any redis command.
ResponseType = TypeAliasType(
    "ResponseType",
    ResponsePrimitive
    | list[Any]
    | MutableSet[Hashable]
    | dict[
        Hashable,
        Any,
    ]
    | RedisError,
)
#: Type alias for valid python types that can be represented as json
JsonType = TypeAliasType("JsonType", str | int | float | bool | dict[str, Any] | list[Any] | None)
