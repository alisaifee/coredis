from __future__ import annotations

from typing import cast

from coredis._utils import EncodingInsensitiveDict, nativestr
from coredis.response._callbacks import ResponseCallback
from coredis.response._callbacks.server import InfoCallback
from coredis.typing import (
    AnyStr,
    Dict,
    List,
    Optional,
    ResponsePrimitive,
    ResponseType,
    Tuple,
    Union,
    ValueT,
)

SENTINEL_STATE_INT_FIELDS = {
    "can-failover-its-master",
    "config-epoch",
    "down-after-milliseconds",
    "failover-timeout",
    "info-refresh",
    "last-hello-message",
    "last-ok-ping-reply",
    "last-ping-reply",
    "last-ping-sent",
    "master-link-down-time",
    "master-port",
    "num-other-sentinels",
    "num-slaves",
    "o-down-time",
    "pending-commands",
    "parallel-syncs",
    "port",
    "quorum",
    "role-reported-time",
    "s-down-time",
    "slave-priority",
    "slave-repl-offset",
    "voted-leader-epoch",
}


def sentinel_state_typed(
    response: list[str],
) -> Dict[str, Union[str, int, bool]]:
    it = iter(response)
    result: Dict[str, Union[str, int, bool]] = {}

    for key, value in zip(it, it):
        if key in SENTINEL_STATE_INT_FIELDS:
            value = int(value)
        result[key] = value

    return result


def add_flags(
    result: Dict[str, Union[int, str, bool]]
) -> Dict[str, Union[int, str, bool]]:
    flags = set(nativestr(result["flags"]).split(","))
    for name, flag in (
        ("is_master", "master"),
        ("is_slave", "slave"),
        ("is_sdown", "s_down"),
        ("is_odown", "o_down"),
        ("is_sentinel", "sentinel"),
        ("is_disconnected", "disconnected"),
        ("is_master_down", "master_down"),
    ):
        result[name] = flag in flags
    return result


def parse_sentinel_state(
    item: List[ResponsePrimitive],
) -> Dict[str, Union[int, str, bool]]:
    result = sentinel_state_typed([nativestr(k) for k in item])
    result = add_flags(result)
    return result


class PrimaryCallback(
    ResponseCallback[
        ResponseType,
        Dict[ResponsePrimitive, ResponsePrimitive],
        Dict[str, Union[str, int, bool]],
    ]
):
    def transform(
        self, response: ResponseType, **options: Optional[ValueT]
    ) -> Dict[str, Union[str, int, bool]]:

        return parse_sentinel_state(cast(List[ResponsePrimitive], response))

    def transform_3(
        self,
        response: Dict[ResponsePrimitive, ResponsePrimitive],
        **options: Optional[ValueT],
    ) -> Dict[str, Union[str, int, bool]]:

        return add_flags(EncodingInsensitiveDict(response))


class PrimariesCallback(
    ResponseCallback[
        List[ResponseType],
        List[ResponseType],
        Dict[str, Dict[str, Union[str, int, bool]]],
    ]
):
    def transform(
        self,
        response: Union[List[ResponseType], Dict[ResponsePrimitive, ResponsePrimitive]],
        **options: Optional[ValueT],
    ) -> Dict[str, Dict[str, Union[str, int, bool]]]:

        result: Dict[str, Dict[str, Union[str, int, bool]]] = {}

        for item in response:
            state = PrimaryCallback()(item)
            result[str(state["name"])] = state

        return result

    def transform_3(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> Dict[str, Dict[str, Union[str, int, bool]]]:

        states: Dict[str, Dict[str, Union[str, int, bool]]] = {}
        for state in response:
            proxy = add_flags(EncodingInsensitiveDict(state))
            states[nativestr(proxy["name"])] = proxy
        return states


class SentinelsStateCallback(
    ResponseCallback[
        List[ResponseType],
        List[ResponseType],
        Tuple[Dict[str, Union[str, bool, int]], ...],
    ]
):
    def transform(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> Tuple[Dict[str, Union[str, bool, int]], ...]:

        return tuple(
            parse_sentinel_state([nativestr(i) for i in item]) for item in response
        )

    def transform_3(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> Tuple[Dict[str, Union[str, bool, int]], ...]:

        return tuple(add_flags(EncodingInsensitiveDict(state)) for state in response)


class GetPrimaryCallback(
    ResponseCallback[
        List[ResponsePrimitive],
        List[ResponsePrimitive],
        Optional[Tuple[str, int]],
    ]
):
    def transform(
        self, response: List[ResponsePrimitive], **options: Optional[ValueT]
    ) -> Optional[Tuple[str, int]]:
        return nativestr(response[0]), int(response[1]) if response else None


class SentinelInfoCallback(
    ResponseCallback[
        List[ResponseType],
        List[ResponseType],
        Dict[AnyStr, Dict[int, Dict[str, ResponseType]]],
    ]
):
    def transform(
        self, response: List[ResponseType], **options: Optional[ValueT]
    ) -> Dict[AnyStr, Dict[int, Dict[str, ResponseType]]]:

        return {response[0]: {r[0]: InfoCallback()(r[1]) for r in response[1]}}
