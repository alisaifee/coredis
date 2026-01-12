from __future__ import annotations

from coredis.response._callbacks import ResponseCallback
from coredis.typing import (
    AnyStr,
    ResponsePrimitive,
    Sequence,
)


class ACLLogCallback(
    ResponseCallback[
        list[Sequence[ResponsePrimitive] | None],
        tuple[dict[AnyStr, ResponsePrimitive] | None, ...],
    ]
):
    def transform(
        self,
        response: list[dict[AnyStr, ResponsePrimitive] | None],
    ) -> tuple[dict[AnyStr, ResponsePrimitive] | None, ...]:
        return tuple(response)
