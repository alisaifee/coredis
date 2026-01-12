from __future__ import annotations

from coredis.response._callbacks import ResponseCallback
from coredis.typing import (
    AnyStr,
    ResponsePrimitive,
)


class ModuleInfoCallback(
    ResponseCallback[
        list[dict[AnyStr, ResponsePrimitive]],
        tuple[dict[AnyStr, ResponsePrimitive], ...],
    ]
):
    def transform(
        self,
        response: list[dict[AnyStr, ResponsePrimitive]],
    ) -> tuple[dict[AnyStr, ResponsePrimitive], ...]:
        return tuple(response)
