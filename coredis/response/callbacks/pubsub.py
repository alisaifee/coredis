from __future__ import annotations

from coredis.commands import ResponseCallback
from coredis.typing import Any, AnyStr, Dict


class NumSubCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Dict[AnyStr, int]:
        return dict(zip(response[0::2], response[1::2]))
