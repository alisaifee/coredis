from __future__ import annotations

from coredis.response._callbacks import ResponseCallback
from coredis.typing import (
    AnyStr,
    ResponseType,
)


class ClientTrackingInfoCallback(
    ResponseCallback[
        ResponseType,
        dict[AnyStr, AnyStr | set[AnyStr] | list[AnyStr]],
    ]
):
    def transform(
        self,
        response: ResponseType,
    ) -> dict[AnyStr, AnyStr | set[AnyStr] | list[AnyStr]]:
        return response
