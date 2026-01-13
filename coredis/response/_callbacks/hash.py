from __future__ import annotations

from typing import cast

from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.typing import (
    AnyStr,
    ResponseType,
    StringT,
    TypeGuard,
)


class HScanCallback(
    ResponseCallback[
        list[ResponseType],
        tuple[int, dict[AnyStr, AnyStr] | tuple[AnyStr, ...]],
    ]
):
    def guard(self, response: list[ResponseType]) -> TypeGuard[tuple[StringT, list[AnyStr]]]:
        return isinstance(response[0], (str, bytes)) and isinstance(response[1], list)

    def transform(
        self,
        response: list[ResponseType],
    ) -> tuple[int, dict[AnyStr, AnyStr] | tuple[AnyStr, ...]]:
        assert self.guard(response)
        cursor, r = response
        if self.options.get("novalues"):
            return int(cursor), tuple(r)
        else:
            return int(cursor), flat_pairs_to_dict(r)


class HRandFieldCallback(
    ResponseCallback[
        AnyStr | list[AnyStr] | list[list[AnyStr]] | None,
        AnyStr | tuple[AnyStr, ...] | dict[AnyStr, AnyStr] | None,
    ]
):
    def transform(
        self,
        response: AnyStr | list[AnyStr] | list[list[AnyStr]] | None,
    ) -> AnyStr | tuple[AnyStr, ...] | dict[AnyStr, AnyStr] | None:
        if not response:
            return None
        if self.options.get("count"):
            assert isinstance(response, list)
            if self.options.get("withvalues"):
                return dict(cast(list[tuple[AnyStr, AnyStr]], response))
            return tuple(cast(tuple[AnyStr, AnyStr], response))
        assert isinstance(response, (str, bytes))
        return response


class HGetAllCallback(ResponseCallback[dict[AnyStr, AnyStr], dict[AnyStr, AnyStr]]):
    def transform(
        self,
        response: dict[AnyStr, AnyStr],
    ) -> dict[AnyStr, AnyStr]:
        return response
