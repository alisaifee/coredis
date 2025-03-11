from __future__ import annotations

from coredis.response._callbacks import DictCallback, ResponseCallback
from coredis.typing import (
    AnyStr,
    Optional,
    ResponsePrimitive,
    Sequence,
    ValueT,
)


class ACLLogCallback(
    ResponseCallback[
        list[Optional[Sequence[ResponsePrimitive]]],
        list[Optional[dict[AnyStr, ResponsePrimitive]]],
        tuple[Optional[dict[AnyStr, ResponsePrimitive]], ...],
    ]
):
    def transform(
        self,
        response: list[Optional[Sequence[ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> tuple[Optional[dict[AnyStr, ResponsePrimitive]], ...]:
        return tuple(
            DictCallback[AnyStr, ResponsePrimitive]()(r, version=self.version)
            for r in response
            if r
        )

    def transform_3(
        self,
        response: list[Optional[dict[AnyStr, ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> tuple[Optional[dict[AnyStr, ResponsePrimitive]], ...]:
        return tuple(response)
