from __future__ import annotations

from typing import cast

from coredis.response._callbacks import ResponseCallback
from coredis.typing import (
    AnyStr,
    Iterable,
    ResponsePrimitive,
    ResponseType,
)


class SScanCallback(ResponseCallback[list[ResponseType], tuple[int, set[AnyStr]]]):
    def transform(
        self,
        response: list[ResponseType],
    ) -> tuple[int, set[AnyStr]]:
        cursor, r = response
        assert isinstance(cursor, (bytes, str)) and isinstance(r, Iterable)
        return int(cursor), set(cast(Iterable[AnyStr], r))


class ItemOrSetCallback(
    ResponseCallback[
        AnyStr | list[ResponsePrimitive] | set[ResponsePrimitive],
        AnyStr | set[AnyStr],
    ]
):
    def transform(
        self,
        response: AnyStr | list[ResponsePrimitive] | set[ResponsePrimitive],
    ) -> AnyStr | set[AnyStr]:
        if self.options.get("count"):
            if isinstance(response, set):
                return cast(set[AnyStr], response)
            if isinstance(response, list):
                return cast(set[AnyStr], set(response) if response else set())
            raise ValueError(f"Unable to map {response!r} to set")
        else:
            return cast(AnyStr, response)
