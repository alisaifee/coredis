from __future__ import annotations

from coredis._utils import EncodingInsensitiveDict, nativestr
from coredis.response._callbacks import ResponseCallback
from coredis.response._callbacks.server import InfoCallback
from coredis.typing import (
    AnyStr,
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
) -> EncodingInsensitiveDict[str, str | int | bool]:
    it = iter(response)
    result: EncodingInsensitiveDict[str, str | int | bool] = EncodingInsensitiveDict()

    for key, value in zip(it, it):
        if key in SENTINEL_STATE_INT_FIELDS:
            value = int(value)
        result[key] = value

    return result


def add_flags(
    result: EncodingInsensitiveDict[str, int | str | bool],
) -> EncodingInsensitiveDict[str, int | str | bool]:
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
) -> EncodingInsensitiveDict[str, int | str | bool]:
    result = sentinel_state_typed([nativestr(k) for k in item])
    result = add_flags(result)
    return result


class PrimaryCallback(
    ResponseCallback[
        ResponseType,
        dict[str, ResponsePrimitive],
    ]
):
    def transform(
        self,
        response: dict[ResponsePrimitive, ResponsePrimitive],
    ) -> dict[str, ResponsePrimitive]:
        return add_flags(EncodingInsensitiveDict(response)).stringify_keys()


class PrimariesCallback(
    ResponseCallback[
        list[ResponseType],
        dict[str, dict[str, ResponsePrimitive]],
    ]
):
    def transform(
        self,
        response: list[ResponseType],
    ) -> dict[str, dict[str, ResponsePrimitive]]:
        states: dict[str, dict[str, ResponsePrimitive]] = {}
        for state in response:
            state = add_flags(EncodingInsensitiveDict(state)).stringify_keys()
            states[nativestr(state["name"])] = state
        return states


class SentinelsStateCallback(
    ResponseCallback[
        list[ResponseType],
        tuple[dict[str, ResponsePrimitive], ...],
    ]
):
    def transform(
        self,
        response: list[ResponseType],
    ) -> tuple[dict[str, ResponsePrimitive], ...]:
        return tuple(
            add_flags(EncodingInsensitiveDict(state)).stringify_keys() for state in response
        )


class GetPrimaryCallback(
    ResponseCallback[
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
        dict[AnyStr, dict[int, dict[str, ResponsePrimitive]]],
    ]
):
    def transform(
        self,
        response: list[ResponseType],
    ) -> dict[AnyStr, dict[int, dict[str, ResponsePrimitive]]]:
        return {response[0]: {r[0]: InfoCallback()(r[1]) for r in response[1]}}
