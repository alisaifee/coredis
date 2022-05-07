from __future__ import annotations

from coredis._utils import EncodingInsensitiveDict
from coredis.response._callbacks import ResponseCallback, SimpleStringCallback
from coredis.response.types import LCSMatch, LCSResult
from coredis.typing import (
    AnyStr,
    Dict,
    List,
    Optional,
    ResponsePrimitive,
    ResponseType,
    Union,
    ValueT,
)


class StringSetCallback(
    ResponseCallback[Optional[AnyStr], Optional[AnyStr], Optional[Union[AnyStr, bool]]]
):
    def transform(
        self, response: Optional[AnyStr], **options: Optional[ValueT]
    ) -> Optional[Union[AnyStr, bool]]:
        if options.get("get"):
            return response
        else:
            return SimpleStringCallback()(response)


class LCSCallback(
    ResponseCallback[
        List[ResponseType],
        Dict[ResponsePrimitive, ResponseType],
        Union[AnyStr, int, LCSResult],
    ]
):
    def transform(
        self,
        response: Union[
            List[ResponseType],
            Dict[ResponsePrimitive, ResponseType],
        ],
        **options: Optional[ValueT],
    ) -> LCSResult:
        assert (
            isinstance(response, list)
            and isinstance(response[-1], int)
            and isinstance(response[1], list)
        )
        return LCSResult(
            tuple(
                LCSMatch(
                    (int(k[0][0]), int(k[0][1])),
                    (int(k[1][0]), int(k[1][1])),
                    k[2] if len(k) > 2 else None,
                )
                for k in response[1]
            ),
            response[-1],
        )

    def transform_3(
        self,
        response: Dict[ResponsePrimitive, ResponseType],
        **options: Optional[ValueT],
    ) -> LCSResult:
        proxy = EncodingInsensitiveDict(response)
        return LCSResult(
            tuple(
                LCSMatch(
                    (int(k[0][0]), int(k[0][1])),
                    (int(k[1][0]), int(k[1][1])),
                    k[2] if len(k) > 2 else None,
                )
                for k in proxy["matches"]
            ),
            proxy["len"],
        )
