from __future__ import annotations

from typing import cast

from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.typing import (
    AnyStr,
    ResponsePrimitive,
    StringT,
)


class HScanCallback(
    ResponseCallback[
        list[ResponsePrimitive | list[ResponsePrimitive]],
        tuple[int, dict[AnyStr, AnyStr] | tuple[AnyStr, ...]],
    ]
):
    def transform(
        self,
        response: list[ResponsePrimitive | list[ResponsePrimitive]],
    ) -> tuple[int, dict[AnyStr, AnyStr] | tuple[AnyStr, ...]]:
        cursor = int(cast(StringT, response[0]))
        results = cast(list[AnyStr], response[1])
        if self.options.get("novalues"):
            return cursor, tuple(results)
        else:
            return cursor, flat_pairs_to_dict(results)


class HRandFieldCallback(
    ResponseCallback[
        StringT | list[StringT] | list[list[StringT]] | None,
        AnyStr | tuple[AnyStr, ...] | dict[AnyStr, AnyStr] | None,
    ]
):
    def transform(
        self,
        response: StringT | list[StringT] | list[list[StringT]] | None,
    ) -> AnyStr | tuple[AnyStr, ...] | dict[AnyStr, AnyStr] | None:
        if not response:
            return None
        if self.options.get("count"):
            if self.options.get("withvalues"):
                return dict(
                    (
                        cast(tuple[AnyStr, AnyStr], tuple(v))
                        for v in cast(list[list[StringT]], response)
                    )
                )
            return tuple(cast(list[AnyStr], response))
        return cast(AnyStr, response)


class HGetAllCallback(ResponseCallback[dict[AnyStr, AnyStr], dict[AnyStr, AnyStr]]):
    def transform(
        self,
        response: dict[AnyStr, AnyStr],
    ) -> dict[AnyStr, AnyStr]:
        return response
