from __future__ import annotations

from coredis.response.callbacks import ResponseCallback
from coredis.response.utils import flat_pairs_to_dict
from coredis.typing import Any, Mapping, Tuple


class ModuleInfoCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Tuple[Mapping, ...]:
        return tuple(flat_pairs_to_dict(mod) for mod in response)

    def transform_3(self, response: Any, **options: Any) -> Tuple[Mapping, ...]:
        return tuple(dict(r) for r in response)
