from __future__ import annotations

from coredis._utils import EncodingInsensitiveDict
from coredis.response._callbacks import ResponseCallback
from coredis.response._utils import flat_pairs_to_dict
from coredis.typing import (
    AnyStr,
    Dict,
    List,
    Optional,
    ResponseType,
    Set,
    Union,
    ValueT,
)


class ClientTrackingInfoCallback(
    ResponseCallback[
        ResponseType,
        ResponseType,
        Dict[AnyStr, Union[AnyStr, Set[AnyStr], List[AnyStr]]],
    ]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Dict[AnyStr, Union[AnyStr, Set[AnyStr], List[AnyStr]]]:

        response = EncodingInsensitiveDict(flat_pairs_to_dict(response))
        response["flags"] = set(response["flags"])
        return dict(response)

    def transform_3(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Dict[AnyStr, Union[AnyStr, Set[AnyStr], List[AnyStr]]]:

        return response
