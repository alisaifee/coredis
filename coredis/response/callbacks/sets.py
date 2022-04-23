from __future__ import annotations

from typing import cast

from coredis.response.callbacks import ResponseCallback
from coredis.typing import (
    AnyStr,
    List,
    Optional,
    ResponsePrimitive,
    ResponseType,
    Set,
    Tuple,
    Union,
    ValueT,
)


class SScanCallback(
    ResponseCallback[ResponseType, ResponseType, Tuple[int, Set[AnyStr]]]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Tuple[int, Set[AnyStr]]:

        cursor, r = response
        return int(cursor), set(r)


class ItemOrSetCallback(
    ResponseCallback[
        Union[AnyStr, List[ResponsePrimitive], Set[ResponsePrimitive]],
        Union[AnyStr, Set[ResponsePrimitive]],
        Union[AnyStr, Set[AnyStr]],
    ]
):
    def transform(
        self,
        response: Union[AnyStr, List[ResponsePrimitive], Set[ResponsePrimitive]],
        **options: Optional[ValueT],
    ) -> Union[AnyStr, Set[AnyStr]]:
        if options.get("count"):
            if isinstance(response, set):
                return cast(Set[AnyStr], response)
            if isinstance(response, list):
                return cast(Set[AnyStr], set(response) if response else set())
            raise ValueError(f"Unable to map {response!r} to set")
        else:
            return cast(AnyStr, response)
