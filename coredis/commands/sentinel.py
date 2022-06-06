from __future__ import annotations

from coredis.response._callbacks import (
    AnyStrCallback,
    DictCallback,
    IntCallback,
    SimpleStringCallback,
)
from coredis.response._callbacks.sentinel import (
    GetPrimaryCallback,
    PrimariesCallback,
    PrimaryCallback,
    SentinelInfoCallback,
    SentinelsStateCallback,
)
from coredis.typing import (
    AnyStr,
    Dict,
    Optional,
    ResponseType,
    StringT,
    Tuple,
    Union,
    ValueT,
)

from . import CommandMixin
from ._wrappers import redis_command
from .constants import CommandName


class SentinelCommands(CommandMixin[AnyStr]):
    @redis_command(
        CommandName.SENTINEL_CKQUORUM,
    )
    async def sentinel_ckquorum(self, service_name: StringT) -> bool:
        return await self.execute_command(
            CommandName.SENTINEL_CKQUORUM,
            service_name,
            callback=SimpleStringCallback(prefix_match=True),
        )

    @redis_command(CommandName.SENTINEL_CONFIG_GET, version_introduced="6.2.0")
    async def sentinel_config_get(self, name: ValueT) -> Dict[AnyStr, AnyStr]:
        """
        Get the current value of a global Sentinel configuration parameter.
        The specified name may be a wildcard, similar to :meth:`config_get`
        """
        return await self.execute_command(
            CommandName.SENTINEL_CONFIG_GET,
            name,
            callback=DictCallback[AnyStr, AnyStr](),
        )

    @redis_command(CommandName.SENTINEL_CONFIG_SET, version_introduced="6.2")
    async def sentinel_config_set(self, name: ValueT, value: ValueT) -> bool:
        """
        Set the value of a global Sentinel configuration parameter
        """
        return await self.execute_command(
            CommandName.SENTINEL_CONFIG_SET,
            name,
            value,
            callback=SimpleStringCallback(),
        )

    @redis_command(
        CommandName.SENTINEL_GET_MASTER_ADDR_BY_NAME,
    )
    async def sentinel_get_master_addr_by_name(
        self, service_name: StringT
    ) -> Optional[Tuple[str, int]]:
        """
        Returns a (host, port) pair for the given :paramref:`service_name`
        """

        return await self.execute_command(
            CommandName.SENTINEL_GET_MASTER_ADDR_BY_NAME,
            service_name,
            callback=GetPrimaryCallback(),
        )

    @redis_command(
        CommandName.SENTINEL_FAILOVER,
    )
    async def sentinel_failover(self, service_name: StringT) -> bool:
        """
        Force a failover as if the master was not reachable, and without asking
        for agreement to other Sentinels
        """
        return await self.execute_command(
            CommandName.SENTINEL_FAILOVER, service_name, callback=SimpleStringCallback()
        )

    @redis_command(CommandName.SENTINEL_FLUSHCONFIG)
    async def sentinel_flushconfig(self) -> bool:
        """
        Force Sentinel to rewrite its configuration on disk, including the current Sentinel state.
        """
        return await self.execute_command(
            CommandName.SENTINEL_FLUSHCONFIG, callback=SimpleStringCallback()
        )

    @redis_command(CommandName.SENTINEL_INFO_CACHE)
    async def sentinel_infocache(
        self, *nodenames: StringT
    ) -> Dict[AnyStr, Dict[int, Dict[str, ResponseType]]]:
        """
        Return cached INFO output from masters and replicas.
        """
        return await self.execute_command(
            CommandName.SENTINEL_INFO_CACHE,
            *nodenames,
            callback=SentinelInfoCallback[AnyStr](),
        )

    @redis_command(
        CommandName.SENTINEL_MASTER,
    )
    async def sentinel_master(
        self, service_name: StringT
    ) -> Dict[str, Union[int, bool, str]]:
        """Returns a dictionary containing the specified masters state."""

        return await self.execute_command(
            CommandName.SENTINEL_MASTER, service_name, callback=PrimaryCallback()
        )

    @redis_command(
        CommandName.SENTINEL_MASTERS,
    )
    async def sentinel_masters(self) -> Dict[str, Dict[str, Union[int, bool, str]]]:
        """Returns a list of dictionaries containing each master's state."""

        return await self.execute_command(
            CommandName.SENTINEL_MASTERS, callback=PrimariesCallback()
        )

    @redis_command(
        CommandName.SENTINEL_MONITOR,
    )
    async def sentinel_monitor(
        self, name: ValueT, ip: ValueT, port: int, quorum: int
    ) -> bool:
        """Adds a new master to Sentinel to be monitored"""

        return await self.execute_command(
            CommandName.SENTINEL_MONITOR,
            name,
            ip,
            port,
            quorum,
            callback=SimpleStringCallback(),
        )

    @redis_command(CommandName.SENTINEL_MYID, version_introduced="6.2.0")
    async def sentinel_myid(self) -> AnyStr:
        """Return the ID of the Sentinel instance"""

        return await self.execute_command(
            CommandName.SENTINEL_MYID, callback=AnyStrCallback[AnyStr]()
        )

    @redis_command(
        CommandName.SENTINEL_REMOVE,
    )
    async def sentinel_remove(self, name: ValueT) -> bool:
        """Removes a master from Sentinel's monitoring"""

        return await self.execute_command(
            CommandName.SENTINEL_REMOVE, name, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.SENTINEL_SENTINELS,
    )
    async def sentinel_sentinels(
        self, service_name: StringT
    ) -> Tuple[Dict[str, Union[int, bool, str]], ...]:
        """Returns a list of sentinels for :paramref:`service_name`"""

        return await self.execute_command(
            CommandName.SENTINEL_SENTINELS,
            service_name,
            callback=SentinelsStateCallback(),
        )

    @redis_command(
        CommandName.SENTINEL_SET,
    )
    async def sentinel_set(self, name: ValueT, option: ValueT, value: ValueT) -> bool:
        """Sets Sentinel monitoring parameters for a given master"""

        return await self.execute_command(
            CommandName.SENTINEL_SET,
            name,
            option,
            value,
            callback=SimpleStringCallback(),
        )

    @redis_command(
        CommandName.SENTINEL_SLAVES,
    )
    async def sentinel_slaves(
        self, service_name: StringT
    ) -> Tuple[Dict[str, Union[int, bool, str]], ...]:
        """Returns a list of slaves for paramref:`service_name`"""

        return await self.execute_command(
            CommandName.SENTINEL_SLAVES, service_name, callback=SentinelsStateCallback()
        )

    @redis_command(
        CommandName.SENTINEL_REPLICAS,
    )
    async def sentinel_replicas(
        self, service_name: StringT
    ) -> Tuple[Dict[str, Union[int, bool, str]], ...]:
        """Returns a list of replicas for :paramref:`service_name`"""

        return await self.execute_command(
            CommandName.SENTINEL_REPLICAS,
            service_name,
            callback=SentinelsStateCallback(),
        )

    @redis_command(CommandName.SENTINEL_RESET)
    async def sentinel_reset(self, pattern: StringT) -> int:
        """
        Reset all the masters with matching name.
        The pattern argument is a glob-style pattern.
        The reset process clears any previous state in a master (including a
        failover in progress), and removes every replica and sentinel already
        discovered and associated with the master.
        """
        return await self.execute_command(
            CommandName.SENTINEL_RESET,
            pattern,
            callback=IntCallback(),
        )
