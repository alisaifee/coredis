from __future__ import annotations

from datetime import datetime

from coredis.exceptions import DataError, NoKeyError, RedisError
from coredis.response._callbacks import DateTimeCallback, ResponseCallback
from coredis.typing import (
    AnyStr,
    ResponseType,
    StringT,
    TypeGuard,
)


class SortCallback(
    ResponseCallback[
        int | list[AnyStr],
        int | tuple[AnyStr, ...],
    ]
):
    def transform(
        self,
        response: int | list[AnyStr],
    ) -> int | tuple[AnyStr, ...]:
        if isinstance(response, list):
            return tuple(response)
        return response


class ScanCallback(ResponseCallback[list[ResponseType], tuple[int, tuple[AnyStr, ...]]]):
    def guard(self, response: list[ResponseType]) -> TypeGuard[tuple[StringT, list[AnyStr]]]:
        return isinstance(response[0], (str, bytes)) and isinstance(response[1], list)

    def transform(
        self,
        response: list[ResponseType],
    ) -> tuple[int, tuple[AnyStr, ...]]:
        assert self.guard(response)
        cursor, r = response
        return int(cursor), tuple(r)


class ExpiryCallback(DateTimeCallback):
    def transform(
        self,
        response: int,
    ) -> datetime:
        if response > 0:
            return super().transform(response)
        else:
            if response == -2:
                raise NoKeyError()
            elif response == -1:
                raise DataError()
            else:
                raise RedisError()
