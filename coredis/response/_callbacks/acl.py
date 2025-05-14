from __future__ import annotations

from coredis.response._callbacks import DictCallback, ResponseCallback
from coredis.typing import (
    AnyStr,
    ResponsePrimitive,
    Sequence,
)


class ACLLogCallback(
    ResponseCallback[
        list[Sequence[ResponsePrimitive] | None],
        list[dict[AnyStr, ResponsePrimitive] | None],
        tuple[dict[AnyStr, ResponsePrimitive] | None, ...],
    ]
):
    def transform(
        self,
        response: list[Sequence[ResponsePrimitive] | None],
    ) -> tuple[dict[AnyStr, ResponsePrimitive] | None, ...]:
        return tuple(
            DictCallback[AnyStr, ResponsePrimitive]()(r, version=self.version)
            for r in response
            if r
        )

    def transform_3(
        self,
        response: list[dict[AnyStr, ResponsePrimitive] | None],
    ) -> tuple[dict[AnyStr, ResponsePrimitive] | None, ...]:
        return tuple(response)
