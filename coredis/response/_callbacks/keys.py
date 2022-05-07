from __future__ import annotations

from datetime import datetime

from coredis.exceptions import DataError, NoKeyError, RedisError
from coredis.response._callbacks import DateTimeCallback, ResponseCallback
from coredis.typing import (
    AnyStr,
    List,
    Optional,
    ResponseType,
    StringT,
    Tuple,
    TypeGuard,
    Union,
    ValueT,
)


class SortCallback(
    ResponseCallback[
        Union[int, List[AnyStr]],
        Union[int, List[AnyStr]],
        Union[int, Tuple[AnyStr, ...]],
    ]
):
    def transform(
        self, response: Union[int, List[AnyStr]], **options: Optional[ValueT]
    ) -> Union[int, Tuple[AnyStr, ...]]:
        if isinstance(response, list):
            return tuple(response)
        return response


class ScanCallback(
    ResponseCallback[
        List[ResponseType], List[ResponseType], Tuple[int, Tuple[AnyStr, ...]]
    ]
):
    def guard(
        self, response: List[ResponseType]
    ) -> TypeGuard[Tuple[StringT, List[AnyStr]]]:
        return isinstance(response[0], (str, bytes)) and isinstance(response[1], list)

    def transform(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> Tuple[int, Tuple[AnyStr, ...]]:
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
