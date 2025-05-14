from __future__ import annotations

from coredis._utils import EncodingInsensitiveDict
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.typing import (
    AnyStr,
    ResponseType,
)


class ClientTrackingInfoCallback(
    ResponseCallback[
        ResponseType,
        ResponseType,
        dict[AnyStr, AnyStr | set[AnyStr] | list[AnyStr]],
    ]
):
    def transform(
        self,
        response: ResponseType,
    ) -> dict[AnyStr, AnyStr | set[AnyStr] | list[AnyStr]]:
        response = EncodingInsensitiveDict(flat_pairs_to_dict(response))
        response["flags"] = set(response["flags"])
        return dict(response)

    def transform_3(
        self,
        response: ResponseType,
    ) -> dict[AnyStr, AnyStr | set[AnyStr] | list[AnyStr]]:
        return response
