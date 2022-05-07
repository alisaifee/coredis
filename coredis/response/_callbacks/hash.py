from __future__ import annotations

from typing import cast

from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.typing import (
    AnyStr,
    Dict,
    List,
    Optional,
    ResponseType,
    StringT,
    Tuple,
    TypeGuard,
    Union,
    ValueT,
)


class HScanCallback(
    ResponseCallback[
        List[ResponseType], List[ResponseType], Tuple[int, Dict[AnyStr, AnyStr]]
    ]
):
    def guard(
        self, response: List[ResponseType]
    ) -> TypeGuard[Tuple[StringT, List[AnyStr]]]:
        return isinstance(response[0], (str, bytes)) and isinstance(response[1], list)

    def transform(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> Tuple[int, Dict[AnyStr, AnyStr]]:
        assert self.guard(response)
        cursor, r = response
        return int(cursor), flat_pairs_to_dict(r)


class HRandFieldCallback(
    ResponseCallback[
        Union[AnyStr, List[AnyStr]],
        Union[AnyStr, List[List[AnyStr]]],
        Optional[Union[AnyStr, Tuple[AnyStr, ...], Dict[AnyStr, AnyStr]]],
    ]
):
    def transform(
        self, response: Union[AnyStr, List[AnyStr]], **options: Optional[ValueT]
    ) -> Optional[Union[AnyStr, Tuple[AnyStr, ...], Dict[AnyStr, AnyStr]]]:
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
        self, response: Union[AnyStr, List[List[AnyStr]]], **options: Optional[ValueT]
    ) -> Optional[Union[AnyStr, Tuple[AnyStr, ...], Dict[AnyStr, AnyStr]]]:
        if not response:
            return None
        if options.get("count"):
            assert isinstance(response, list)
            if options.get("withvalues"):
                return dict(cast(List[Tuple[AnyStr, AnyStr]], response))
            return tuple(cast(Tuple[AnyStr, AnyStr], response))
        assert isinstance(response, (str, bytes))
        return response


class HGetAllCallback(
    ResponseCallback[List[AnyStr], Dict[AnyStr, AnyStr], Dict[AnyStr, AnyStr]]
):
    def transform(
        self, response: List[AnyStr], **options: Optional[ValueT]
    ) -> Dict[AnyStr, AnyStr]:

        return flat_pairs_to_dict(response) if response else {}

    def transform_3(
        self, response: Dict[AnyStr, AnyStr], **options: Optional[ValueT]
    ) -> Dict[AnyStr, AnyStr]:
        return response
