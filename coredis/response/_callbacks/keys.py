from __future__ import annotations

from datetime import datetime

from coredis.exceptions import DataError, NoKeyError, RedisError
from coredis.response._callbacks import DateTimeCallback, ResponseCallback
from coredis.typing import (
    AnyStr,
    Optional,
    ResponseType,
    StringT,
    TypeGuard,
    Union,
    ValueT,
)


class SortCallback(
    ResponseCallback[
        Union[int, list[AnyStr]],
        Union[int, list[AnyStr]],
        Union[int, tuple[AnyStr, ...]],
    ]
):
    def transform(
        self, response: Union[int, list[AnyStr]], **options: Optional[ValueT]
    ) -> Union[int, tuple[AnyStr, ...]]:
        if isinstance(response, list):
            return tuple(response)
        return response


class ScanCallback(
    ResponseCallback[list[ResponseType], list[ResponseType], tuple[int, tuple[AnyStr, ...]]]
):
    def guard(self, response: list[ResponseType]) -> TypeGuard[tuple[StringT, list[AnyStr]]]:
        return isinstance(response[0], (str, bytes)) and isinstance(response[1], list)

    def transform(
        self, response: list[ResponseType], **options: Optional[ValueT]
    ) -> tuple[int, tuple[AnyStr, ...]]:
        assert self.guard(response)
        cursor, r = response
        return int(cursor), tuple(r)


class ExpiryCallback(DateTimeCallback):
    def transform(self, response: int, **options: Optional[ValueT]) -> datetime:
        if response > 0:
            return super().transform(response, **options)
        else:
            if response == -2:
                raise NoKeyError()
            elif response == -1:
                raise DataError()
            else:
                raise RedisError()
