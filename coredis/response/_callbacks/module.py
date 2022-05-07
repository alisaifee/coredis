from __future__ import annotations

from typing import cast

from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.typing import (
    AnyStr,
    Dict,
    List,
    Optional,
    ResponsePrimitive,
    ResponseType,
    Tuple,
    ValueT,
)


class ModuleInfoCallback(
    ResponseCallback[
        List[List[ResponseType]],
        List[Dict[AnyStr, ResponsePrimitive]],
        Tuple[Dict[AnyStr, ResponsePrimitive], ...],
    ]
):
    def transform(
        self, response: List[List[ResponseType]], **options: Optional[ValueT]
    ) -> Tuple[Dict[AnyStr, ResponsePrimitive], ...]:
        return tuple(
            cast(Dict[AnyStr, ResponsePrimitive], flat_pairs_to_dict(mod))
            for mod in response
        )

    def transform_3(
        self,
        response: List[Dict[AnyStr, ResponsePrimitive]],
        **options: Optional[ValueT],
    ) -> Tuple[Dict[AnyStr, ResponsePrimitive], ...]:
        return tuple(response)
