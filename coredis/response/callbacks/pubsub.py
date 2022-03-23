from coredis.commands import SimpleCallback
from coredis.typing import Any, AnyStr, Dict


class NumSubCallback(SimpleCallback):
    def transform(self, response: Any, **options: Any) -> Dict[AnyStr, int]:
        return dict(zip(response[0::2], response[1::2]))
