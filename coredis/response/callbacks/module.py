from coredis.response.callbacks import SimpleCallback
from coredis.typing import Any, Dict, Tuple
from coredis.utils import flat_pairs_to_dict


class ModuleInfoCallback(SimpleCallback):
    def transform(self, response: Any) -> Tuple[Dict, ...]:
        return tuple(flat_pairs_to_dict(mod) for mod in response)

    def transform_3(self, response: Any) -> Tuple[Dict, ...]:
        return tuple(dict(r) for r in response)
