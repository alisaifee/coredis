from __future__ import annotations

from coredis.response.callbacks import ResponseCallback
from coredis.typing import Any, AnyStr, Dict, List, Set, Union
from coredis.utils import AnyDict, flat_pairs_to_dict


class ClientTrackingInfoCallback(ResponseCallback):
    def transform_3(
        self, response: Any, **options: Any
    ) -> Dict[AnyStr, Union[AnyStr, Set[AnyStr], List[AnyStr]]]:
        return response

    def transform(
        self, response: Any, **options: Any
    ) -> Dict[AnyStr, Union[AnyStr, Set[AnyStr], List[AnyStr]]]:
        response = AnyDict(flat_pairs_to_dict(response))
        response["flags"] = set(response["flags"])
        return dict(response)
