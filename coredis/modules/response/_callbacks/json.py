from __future__ import annotations

from typing import cast

from coredis._json import json
from coredis.response._callbacks import ResponseCallback
from coredis.typing import JsonType, ResponseType


class JsonCallback(ResponseCallback[ResponseType, JsonType]):
    def transform(
        self,
        response: ResponseType,
    ) -> JsonType:
        if isinstance(response, (bytes, str)):
            deser = json.loads(response)
        elif isinstance(response, list):
            deser = [json.loads(e) if isinstance(e, (bytes, str)) else e for e in response]
        else:
            deser = response
        return cast(JsonType, deser)  # alas we lie.
