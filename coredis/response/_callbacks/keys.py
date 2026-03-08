from __future__ import annotations

from datetime import datetime
from typing import cast

from coredis.exceptions import DataError, NoKeyError, RedisError
from coredis.response._callbacks import DateTimeCallback, ResponseCallback
from coredis.typing import (
    AnyStr,
    StringT,
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


class ScanCallback(ResponseCallback[list[StringT | list[StringT]], tuple[int, tuple[AnyStr, ...]]]):
    def transform(
        self,
        response: list[StringT | list[StringT]],
    ) -> tuple[int, tuple[AnyStr, ...]]:
        cursor, r = tuple(response)
        return int(cast(StringT, cursor)), tuple(cast(list[AnyStr], r))


class ExpiryCallback(DateTimeCallback):
    def transform(
        self,
        response: int | float,
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
