from __future__ import annotations

from typing import Sequence

from coredis.response.callbacks import DictCallback, ResponseCallback
from coredis.typing import (
    AnyStr,
    Dict,
    List,
    Optional,
    ResponsePrimitive,
    Tuple,
    ValueT,
)


class ACLLogCallback(
    ResponseCallback[
        List[Optional[Sequence[ResponsePrimitive]]],
        List[Optional[Dict[AnyStr, ResponsePrimitive]]],
        Tuple[Optional[Dict[AnyStr, ResponsePrimitive]], ...],
    ]
):
    def transform(
        self,
        response: List[Optional[Sequence[ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> Tuple[Optional[Dict[AnyStr, ResponsePrimitive]], ...]:
        return tuple(
            DictCallback[AnyStr, ResponsePrimitive]()(r, version=self.version)
            for r in response
            if r
        )

    def transform_3(
        self,
        response: List[Optional[Dict[AnyStr, ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> Tuple[Optional[Dict[AnyStr, ResponsePrimitive]], ...]:
        return tuple(response)
