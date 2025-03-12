from __future__ import annotations

from typing import cast

from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.typing import (
    AnyStr,
    ResponsePrimitive,
    ResponseType,
    ValueT,
)


class ModuleInfoCallback(
    ResponseCallback[
        list[list[ResponseType]],
        list[dict[AnyStr, ResponsePrimitive]],
        tuple[dict[AnyStr, ResponsePrimitive], ...],
    ]
):
    def transform(
        self, response: list[list[ResponseType]], **options: ValueT | None
    ) -> tuple[dict[AnyStr, ResponsePrimitive], ...]:
        return tuple(
            cast(dict[AnyStr, ResponsePrimitive], flat_pairs_to_dict(mod)) for mod in response
        )

    def transform_3(
        self,
        response: list[dict[AnyStr, ResponsePrimitive]],
        **options: ValueT | None,
    ) -> tuple[dict[AnyStr, ResponsePrimitive], ...]:
        return tuple(response)
