from __future__ import annotations

from coredis.response.callbacks import (
    DictCallback,
    ResponseCallback,
    SimpleStringCallback,
)
from coredis.response.utils import flat_pairs_to_dict
from coredis.typing import Any, AnyStr, Mapping, Tuple, Union


class ACLLogCallback(ResponseCallback):
    def transform(
        self, response: Any, **options: Any
    ) -> Union[bool, Tuple[Mapping[AnyStr, AnyStr], ...]]:
        if options.get("reset"):
            return SimpleStringCallback()(response)
        else:
            return tuple(
                DictCallback(transform_function=flat_pairs_to_dict)(r) for r in response
            )
