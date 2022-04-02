from __future__ import annotations

from coredis.commands import ResponseCallback
from coredis.response.callbacks import SimpleStringCallback
from coredis.response.types import LCSMatch, LCSResult
from coredis.typing import Any, AnyStr, Union
from coredis.utils import EncodingInsensitiveDict


class StringSetCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Union[AnyStr, bool]:
        if options.get("get"):
            return response
        else:
            return SimpleStringCallback()(response)


class LCSCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Union[AnyStr, int, LCSResult]:
        if options.get("idx") is not None:
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

        return response

    def transform_3(
        self, response: Any, **options: Any
    ) -> Union[AnyStr, int, LCSResult]:
        if options.get("idx") is not None:
            response_proxy = EncodingInsensitiveDict(response)
            return LCSResult(
                tuple(
                    LCSMatch(
                        (int(k[0][0]), int(k[0][1])),
                        (int(k[1][0]), int(k[1][1])),
                        k[2] if len(k) > 2 else None,
                    )
                    for k in response_proxy["matches"]
                ),
                response_proxy["len"],
            )
        return response
