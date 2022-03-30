from __future__ import annotations

from coredis.response.callbacks import SimpleStringCallback
from coredis.typing import Any, AnyStr, Dict, Optional, Tuple, ValueT
from coredis.utils import nativestr

from . import CommandMixin, SimpleCallback, redis_command
from .constants import CommandName

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


class PrimaryCallback(SimpleCallback):
    def transform(self, response: Any) -> Dict[str, Any]:
        return parse_sentinel_state(response)


class PrimariesCallback(SimpleCallback):
    def transform(self, response: Any) -> Dict[str, Dict[str, Any]]:
        result = {}

        for item in response:
            state = PrimaryCallback()(item)
            result[state["name"]] = state

        return result


class SentinelsStateCallback(SimpleCallback):
    def transform(self, response: Any) -> Tuple[Dict[str, Any], ...]:
        return tuple(parse_sentinel_state(map(nativestr, item)) for item in response)


class GetPrimaryCallback(SimpleCallback):
    def transform(self, response: Any) -> Optional[Tuple[AnyStr, int]]:
        return response and (response[0], int(response[1]))


class SentinelCommands(CommandMixin[AnyStr]):
    @redis_command(
        CommandName.SENTINEL_GET_MASTER_ADDR_BY_NAME,
        response_callback=GetPrimaryCallback(),
    )
    async def sentinel_get_master_addr_by_name(self, service_name: ValueT):
        """Returns a (host, port) pair for the given :paramref:`service_name`"""

        return await self.execute_command(
            CommandName.SENTINEL_GET_MASTER_ADDR_BY_NAME, service_name
        )

    @redis_command(
        CommandName.SENTINEL_MASTER,
        response_callback=PrimaryCallback(),
    )
    async def sentinel_master(self, service_name: ValueT) -> Dict[str, Any]:
        """Returns a dictionary containing the specified masters state."""

        return await self.execute_command(CommandName.SENTINEL_MASTER, service_name)

    @redis_command(
        CommandName.SENTINEL_MASTERS,
        response_callback=PrimariesCallback(),
    )
    async def sentinel_masters(self) -> Dict[str, Dict[str, Any]]:
        """Returns a list of dictionaries containing each master's state."""

        return await self.execute_command(CommandName.SENTINEL_MASTERS)

    @redis_command(
        CommandName.SENTINEL_MONITOR,
        response_callback=SimpleStringCallback(),
    )
    async def sentinel_monitor(
        self, name: ValueT, ip: ValueT, port: int, quorum
    ) -> bool:
        """Adds a new master to Sentinel to be monitored"""

        return await self.execute_command(
            CommandName.SENTINEL_MONITOR, name, ip, port, quorum
        )

    @redis_command(
        CommandName.SENTINEL_REMOVE,
        response_callback=SimpleStringCallback(),
    )
    async def sentinel_remove(self, name: ValueT) -> bool:
        """Removes a master from Sentinel's monitoring"""

        return await self.execute_command(CommandName.SENTINEL_REMOVE, name)

    @redis_command(
        CommandName.SENTINEL_SENTINELS,
        response_callback=SentinelsStateCallback(),
    )
    async def sentinel_sentinels(
        self, service_name: ValueT
    ) -> Tuple[Dict[AnyStr, Any], ...]:
        """Returns a list of sentinels for :paramref:`service_name`"""

        return await self.execute_command(CommandName.SENTINEL_SENTINELS, service_name)

    @redis_command(
        CommandName.SENTINEL_SET,
        response_callback=SimpleStringCallback(),
    )
    async def sentinel_set(self, name: ValueT, option, value) -> bool:
        """Sets Sentinel monitoring parameters for a given master"""

        return await self.execute_command(CommandName.SENTINEL_SET, name, option, value)

    @redis_command(
        CommandName.SENTINEL_SLAVES,
        response_callback=SentinelsStateCallback(),
    )
    async def sentinel_slaves(
        self, service_name: ValueT
    ) -> Tuple[Dict[AnyStr, Any], ...]:
        """Returns a list of slaves for paramref:`service_name`"""

        return await self.execute_command(CommandName.SENTINEL_SLAVES, service_name)

    @redis_command(
        CommandName.SENTINEL_REPLICAS,
        response_callback=SentinelsStateCallback(),
    )
    async def sentinel_replicas(
        self, service_name: ValueT
    ) -> Tuple[Dict[AnyStr, Any], ...]:
        """Returns a list of replicas for :paramref:`service_name`"""

        return await self.execute_command(CommandName.SENTINEL_REPLICAS, service_name)
