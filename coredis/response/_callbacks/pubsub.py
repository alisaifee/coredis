from __future__ import annotations

from coredis.response._callbacks import ResponseCallback
from coredis.typing import AnyStr, Dict, Optional, ResponseType, ValueT


class NumSubCallback(ResponseCallback[ResponseType, ResponseType, Dict[AnyStr, int]]):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Dict[AnyStr, int]:

        return dict(zip(response[0::2], response[1::2]))
