from __future__ import annotations

from coredis.response.callbacks import SimpleStringCallback
from coredis.response.callbacks.sentinel import (
    GetPrimaryCallback,
    PrimariesCallback,
    PrimaryCallback,
    SentinelsStateCallback,
)
from coredis.typing import Any, AnyStr, Dict, Tuple, ValueT

from . import CommandMixin, redis_command
from .constants import CommandName


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
