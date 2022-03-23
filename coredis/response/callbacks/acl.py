from coredis.response.callbacks import (
    DictCallback,
    ParametrizedCallback,
    SimpleStringCallback,
)
from coredis.typing import Any, AnyStr, Dict, Tuple, Union
from coredis.utils import flat_pairs_to_dict


class ACLLogCallback(ParametrizedCallback):
    def transform(
        self, response: Any, **options: Any
    ) -> Union[bool, Tuple[Dict[AnyStr, AnyStr], ...]]:
        if options.get("reset"):
            return SimpleStringCallback()(response)
        else:
            return tuple(
                DictCallback(transform_function=flat_pairs_to_dict)(r) for r in response
            )
