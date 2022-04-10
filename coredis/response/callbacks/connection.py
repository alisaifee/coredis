from __future__ import annotations

from coredis.response.callbacks import ResponseCallback
from coredis.response.utils import flat_pairs_to_dict
from coredis.typing import Any, AnyStr, Dict, List, Set, Union
from coredis.utils import EncodingInsensitiveDict


class ClientTrackingInfoCallback(ResponseCallback):
    def transform_3(
        self, response: Any, **options: Any
    ) -> Dict[AnyStr, Union[AnyStr, Set[AnyStr], List[AnyStr]]]:
        return response

    def transform(
        self, response: Any, **options: Any
    ) -> Dict[AnyStr, Union[AnyStr, Set[AnyStr], List[AnyStr]]]:
        response = EncodingInsensitiveDict(flat_pairs_to_dict(response))
        response["flags"] = set(response["flags"])
        return dict(response)
