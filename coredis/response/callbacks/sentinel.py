from __future__ import annotations

from typing import Any, AnyStr, Dict, Optional, Tuple

from coredis.response.callbacks import ResponseCallback
from coredis.utils import nativestr

SENTINEL_STATE_TYPES = {
    "can-failover-its-master": int,
    "config-epoch": int,
    "down-after-milliseconds": int,
    "failover-timeout": int,
    "info-refresh": int,
    "last-hello-message": int,
    "last-ok-ping-reply": int,
    "last-ping-reply": int,
    "last-ping-sent": int,
    "master-link-down-time": int,
    "master-port": int,
    "num-other-sentinels": int,
    "num-slaves": int,
    "o-down-time": int,
    "pending-commands": int,
    "parallel-syncs": int,
    "port": int,
    "quorum": int,
    "role-reported-time": int,
    "s-down-time": int,
    "slave-priority": int,
    "slave-repl-offset": int,
    "voted-leader-epoch": int,
}


def pairs_to_dict_typed(response, type_info):
    it = iter(response)
    result = {}

    for key, value in zip(it, it):
        if key in type_info:
            try:
                value = type_info[key](value)
            except Exception:
                # if for some reason the value can't be coerced, just use
                # the string value
                pass
        result[key.replace("-", "_")] = value

    return result


def parse_sentinel_state(item) -> Dict[str, Any]:
    result = pairs_to_dict_typed([nativestr(k) for k in item], SENTINEL_STATE_TYPES)
    flags = set(result["flags"].split(","))

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


class PrimaryCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Dict[str, Any]:
        return parse_sentinel_state(response)


class PrimariesCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Dict[str, Dict[str, Any]]:
        result = {}

        for item in response:
            state = PrimaryCallback()(item)
            result[state["name"]] = state

        return result


class SentinelsStateCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Tuple[Dict[str, Any], ...]:
        return tuple(parse_sentinel_state(map(nativestr, item)) for item in response)


class GetPrimaryCallback(ResponseCallback):
    def transform(self, response: Any, **options: Any) -> Optional[Tuple[AnyStr, int]]:
        return response and (response[0], int(response[1]))
