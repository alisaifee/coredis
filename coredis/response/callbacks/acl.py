from __future__ import annotations

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
        List[Optional[List[ResponsePrimitive]]],
        List[Optional[Dict[AnyStr, ResponsePrimitive]]],
        Tuple[Optional[Dict[AnyStr, ResponsePrimitive]], ...],
    ]
):
    def transform(
        self,
        response: List[Optional[List[ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> Tuple[Optional[Dict[AnyStr, ResponsePrimitive]], ...]:
        return tuple(
            DictCallback[AnyStr, ResponsePrimitive]()(r, version=self.version)
            for r in response
        )

    def transform_3(
        self,
        response: List[Optional[Dict[AnyStr, ResponsePrimitive]]],
        **options: Optional[ValueT],
    ) -> Tuple[Optional[Dict[AnyStr, ResponsePrimitive]], ...]:
        return tuple(response)
