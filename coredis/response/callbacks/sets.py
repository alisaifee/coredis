from __future__ import annotations

from coredis.commands import ResponseCallback
from coredis.typing import Any, AnyStr, Set, Tuple, Union


class SScanCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Tuple[int, Set[AnyStr]]:
        cursor, r = response

        return int(cursor), set(r)


class ItemOrSetCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Union[AnyStr, Set[AnyStr]]:
        if options.get("count"):
            if isinstance(response, set):
                return response
            return response and set(response)
        else:
            return response
