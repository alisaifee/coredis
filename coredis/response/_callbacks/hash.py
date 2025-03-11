from __future__ import annotations

from typing import cast

from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.typing import (
    AnyStr,
    Optional,
    ResponseType,
    StringT,
    TypeGuard,
    Union,
    ValueT,
)


class HScanCallback(
    ResponseCallback[
        list[ResponseType],
        list[ResponseType],
        tuple[int, Union[dict[AnyStr, AnyStr], tuple[AnyStr, ...]]],
    ]
):
    def guard(self, response: list[ResponseType]) -> TypeGuard[tuple[StringT, list[AnyStr]]]:
        return isinstance(response[0], (str, bytes)) and isinstance(response[1], list)

    def transform(
        self, response: list[ResponseType], **options: Optional[ValueT]
    ) -> tuple[int, Union[dict[AnyStr, AnyStr], tuple[AnyStr, ...]]]:
        assert self.guard(response)
        cursor, r = response
        if options.get("novalues"):
            return int(cursor), tuple(r)
        else:
            return int(cursor), flat_pairs_to_dict(r)


class HRandFieldCallback(
    ResponseCallback[
        Optional[Union[AnyStr, list[AnyStr]]],
        Optional[Union[AnyStr, list[AnyStr], list[list[AnyStr]]]],
        Optional[Union[AnyStr, tuple[AnyStr, ...], dict[AnyStr, AnyStr]]],
    ]
):
    def transform(
        self,
        response: Optional[Union[AnyStr, list[AnyStr]]],
        **options: Optional[ValueT],
    ) -> Optional[Union[AnyStr, tuple[AnyStr, ...], dict[AnyStr, AnyStr]]]:
        if not response:
            return None
        if options.get("count"):
            assert isinstance(response, list)
            if options.get("withvalues"):
                return flat_pairs_to_dict(response)
            else:
                return tuple(response)
        assert isinstance(response, (str, bytes))
        return response

    def transform_3(
        self,
        response: Optional[Union[AnyStr, list[AnyStr], list[list[AnyStr]]]],
        **options: Optional[ValueT],
    ) -> Optional[Union[AnyStr, tuple[AnyStr, ...], dict[AnyStr, AnyStr]]]:
        if not response:
            return None
        if options.get("count"):
            assert isinstance(response, list)
            if options.get("withvalues"):
                return dict(cast(list[tuple[AnyStr, AnyStr]], response))
            return tuple(cast(tuple[AnyStr, AnyStr], response))
        assert isinstance(response, (str, bytes))
        return response


class HGetAllCallback(ResponseCallback[list[AnyStr], dict[AnyStr, AnyStr], dict[AnyStr, AnyStr]]):
    def transform(
        self, response: list[AnyStr], **options: Optional[ValueT]
    ) -> dict[AnyStr, AnyStr]:
        return flat_pairs_to_dict(response) if response else {}

    def transform_3(
        self, response: dict[AnyStr, AnyStr], **options: Optional[ValueT]
    ) -> dict[AnyStr, AnyStr]:
        return response
