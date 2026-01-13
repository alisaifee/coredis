from __future__ import annotations

from typing import Any

from coredis._utils import EncodingInsensitiveDict
from coredis.response._callbacks import ResponseCallback, SimpleStringCallback
from coredis.response.types import LCSMatch, LCSResult
from coredis.typing import (
    AnyStr,
    ResponsePrimitive,
    ResponseType,
)


class StringSetCallback(ResponseCallback[AnyStr | None, AnyStr | bool | None]):
    def transform(self, response: AnyStr | None, **options: Any) -> AnyStr | bool | None:
        if self.options.get("get"):
            return response
        else:
            return SimpleStringCallback()(response)


class LCSCallback(
    ResponseCallback[
        dict[ResponsePrimitive, ResponseType],
        LCSResult,
    ]
):
    def transform(
        self,
        response: dict[ResponsePrimitive, ResponseType],
        **options: Any,
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
