from __future__ import annotations

from typing import cast

from coredis._utils import EncodingInsensitiveDict, nativestr
from coredis.response._callbacks import ResponseCallback
from coredis.response._callbacks.server import InfoCallback
from coredis.typing import (
    AnyStr,
    MutableMapping,
    ResponsePrimitive,
    ResponseType,
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
) -> dict[str, str | int | bool]:
    it = iter(response)
    result: dict[str, str | int | bool] = {}

    for key, value in zip(it, it):
        if key in SENTINEL_STATE_INT_FIELDS:
            value = int(value)
        result[key] = value

    return result


def add_flags(
    result: MutableMapping[str, int | str | bool],
) -> MutableMapping[str, int | str | bool]:
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
    item: list[ResponsePrimitive],
) -> MutableMapping[str, int | str | bool]:
    result = sentinel_state_typed([nativestr(k) for k in item])
    result = add_flags(result)
    return result


class PrimaryCallback(
    ResponseCallback[
        ResponseType,
        dict[ResponsePrimitive, ResponsePrimitive],
        dict[str, str | int | bool],
    ]
):
    def transform(
        self,
        response: ResponseType,
    ) -> dict[str, str | int | bool]:
        return dict(parse_sentinel_state(cast(list[ResponsePrimitive], response)))

    def transform_3(
        self,
        response: dict[ResponsePrimitive, ResponsePrimitive],
    ) -> dict[str, str | int | bool]:
        return dict(add_flags(EncodingInsensitiveDict(response)))


class PrimariesCallback(
    ResponseCallback[
        list[ResponseType],
        list[ResponseType],
        dict[str, dict[str, str | int | bool]],
    ]
):
    def transform(
        self,
        response: list[ResponseType] | dict[ResponsePrimitive, ResponsePrimitive],
    ) -> dict[str, dict[str, str | int | bool]]:
        result: dict[str, dict[str, str | int | bool]] = {}

        for item in response:
            state = PrimaryCallback()(item)
            result[str(state["name"])] = state

        return result

    def transform_3(
        self,
        response: list[ResponseType],
    ) -> dict[str, dict[str, str | int | bool]]:
        states: dict[str, dict[str, str | int | bool]] = {}
        for state in response:
            proxy = add_flags(EncodingInsensitiveDict(state))
            states[nativestr(proxy["name"])] = dict(proxy)
        return states


class SentinelsStateCallback(
    ResponseCallback[
        list[ResponseType],
        list[ResponseType],
        tuple[dict[str, str | bool | int], ...],
    ]
):
    def transform(
        self,
        response: list[ResponseType],
    ) -> tuple[dict[str, str | bool | int], ...]:
        return tuple(dict(parse_sentinel_state([nativestr(i) for i in item])) for item in response)

    def transform_3(
        self,
        response: list[ResponseType],
    ) -> tuple[dict[str, str | bool | int], ...]:
        return tuple(dict(add_flags(EncodingInsensitiveDict(state))) for state in response)


class GetPrimaryCallback(
    ResponseCallback[
        list[ResponsePrimitive],
        list[ResponsePrimitive],
        tuple[str, int] | None,
    ]
):
    def transform(
        self,
        response: list[ResponsePrimitive],
    ) -> tuple[str, int] | None:
        return nativestr(response[0]), int(response[1]) if response else None


class SentinelInfoCallback(
    ResponseCallback[
        list[ResponseType],
        list[ResponseType],
        dict[AnyStr, dict[int, dict[str, ResponseType]]],
    ]
):
    def transform(
        self,
        response: list[ResponseType],
    ) -> dict[AnyStr, dict[int, dict[str, ResponseType]]]:
        return {response[0]: {r[0]: InfoCallback()(r[1]) for r in response[1]}}
