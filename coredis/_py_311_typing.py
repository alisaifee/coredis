from __future__ import annotations

from collections.abc import Hashable
from typing import Any, TypeAlias

from .typing import MutableSet, RedisError, ResponsePrimitive

#: Represents the total structure of any response for any redis command.
ResponseType: TypeAlias = (
    ResponsePrimitive
    | list[Any]
    | MutableSet[Hashable]
    | dict[
        Hashable,
        Any,
    ]
    | RedisError
)
#: Type alias for valid python types that can be represented as json
JsonType: TypeAlias = str | int | float | bool | dict[str, Any] | list[Any] | None
