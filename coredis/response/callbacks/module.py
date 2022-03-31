from __future__ import annotations

from coredis.response.callbacks import ResponseCallback
from coredis.typing import Any, Dict, Tuple
from coredis.utils import flat_pairs_to_dict


class ModuleInfoCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Tuple[Dict, ...]:
        return tuple(flat_pairs_to_dict(mod) for mod in response)

    def transform_3(self, response: Any, **options: Any) -> Tuple[Dict, ...]:
        return tuple(dict(r) for r in response)
