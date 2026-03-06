from __future__ import annotations

from coredis._utils import EncodingInsensitiveDict, nativestr
from coredis.response._callbacks import ResponseCallback
from coredis.response._callbacks.server import InfoCallback
from coredis.typing import (
    AnyStr,
    ResponsePrimitive,
    ResponseType,
    StringT,
)


def add_flags(result: EncodingInsensitiveDict) -> EncodingInsensitiveDict:
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


class PrimaryCallback(
    ResponseCallback[
        dict[ResponsePrimitive, ResponsePrimitive],
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
        list[dict[StringT, StringT]],
        dict[str, dict[str, ResponsePrimitive]],
    ]
):
    def transform(
        self,
        response: list[dict[StringT, StringT]],
    ) -> dict[str, dict[str, ResponsePrimitive]]:
        states: dict[str, dict[str, ResponsePrimitive]] = {}
        for state in response:
            decoded_state = add_flags(EncodingInsensitiveDict(state)).stringify_keys()
            states[nativestr(decoded_state["name"])] = decoded_state
        return states


class SentinelsStateCallback(
    ResponseCallback[
        list[dict[AnyStr, AnyStr]],
        tuple[dict[str, ResponsePrimitive], ...],
    ]
):
    def transform(
        self,
        response: list[dict[AnyStr, AnyStr]],
    ) -> tuple[dict[str, ResponsePrimitive], ...]:
        return tuple(
            add_flags(EncodingInsensitiveDict(state)).stringify_keys() for state in response
        )


class GetPrimaryCallback(
    ResponseCallback[
        list[AnyStr] | None,
        tuple[str, int] | None,
    ]
):
    def transform(
        self,
        response: list[AnyStr] | None,
    ) -> tuple[str, int] | None:
        if response:
            return nativestr(response[0]), int(response[1])
        return None


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
        # TODO: giving up for now
        return {response[0]: {r[0]: InfoCallback()(r[1]) for r in response[1]}}  # type: ignore
