from __future__ import annotations

from typing import Optional, cast

from coredis._json import json
from coredis.modules.response.types import JsonType
from coredis.response._callbacks import ResponseCallback
from coredis.typing import ResponseType, ValueT


class JsonCallback(ResponseCallback[ResponseType, ResponseType, JsonType]):
    def transform(self, response: ResponseType, **kwargs: Optional[ValueT]) -> JsonType:
        if isinstance(response, (bytes, str)):
            deser = json.loads(response)
        elif isinstance(response, list):
            deser = [
                json.loads(e) if isinstance(e, (bytes, str)) else e for e in response
            ]
        else:
            deser = response
        return cast(JsonType, deser)  # alas we lie.
