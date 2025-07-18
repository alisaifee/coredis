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
    RedisValueT,
    ResponsePrimitive,
    StringT,
)

from . import CommandMixin
from ._wrappers import redis_command
from .constants import CommandName
from .request import CommandRequest


class SentinelCommands(CommandMixin[AnyStr]):
    @redis_command(
        CommandName.SENTINEL_CKQUORUM,
    )
    def sentinel_ckquorum(self, service_name: StringT) -> CommandRequest[bool]:
        return CommandRequest(
            self,
            CommandName.SENTINEL_CKQUORUM,
            service_name,
            callback=SimpleStringCallback(prefix_match=True),
        )

    @redis_command(CommandName.SENTINEL_CONFIG_GET, version_introduced="6.2.0")
    def sentinel_config_get(self, name: RedisValueT) -> CommandRequest[dict[AnyStr, AnyStr]]:
        """
        Get the current value of a global Sentinel configuration parameter.
        The specified name may be a wildcard, similar to :meth:`config_get`
        """
        return CommandRequest(
            self,
            CommandName.SENTINEL_CONFIG_GET,
            name,
            callback=DictCallback[AnyStr, AnyStr](),
        )

    @redis_command(CommandName.SENTINEL_CONFIG_SET, version_introduced="6.2")
    def sentinel_config_set(self, name: RedisValueT, value: RedisValueT) -> CommandRequest[bool]:
        """
        Set the value of a global Sentinel configuration parameter
        """
        return CommandRequest(
            self,
            CommandName.SENTINEL_CONFIG_SET,
            name,
            value,
            callback=SimpleStringCallback(),
        )

    @redis_command(
        CommandName.SENTINEL_GET_MASTER_ADDR_BY_NAME,
    )
    def sentinel_get_master_addr_by_name(
        self, service_name: StringT
    ) -> CommandRequest[tuple[str, int] | None]:
        """
        Returns a (host, port) pair for the given :paramref:`service_name`
        """

        return CommandRequest(
            self,
            CommandName.SENTINEL_GET_MASTER_ADDR_BY_NAME,
            service_name,
            callback=GetPrimaryCallback(),
        )

    @redis_command(
        CommandName.SENTINEL_FAILOVER,
    )
    def sentinel_failover(self, service_name: StringT) -> CommandRequest[bool]:
        """
        Force a failover as if the master was not reachable, and without asking
        for agreement to other Sentinels
        """
        return CommandRequest(
            self, CommandName.SENTINEL_FAILOVER, service_name, callback=SimpleStringCallback()
        )

    @redis_command(CommandName.SENTINEL_FLUSHCONFIG)
    def sentinel_flushconfig(self) -> CommandRequest[bool]:
        """
        Force Sentinel to rewrite its configuration on disk, including the current Sentinel state.
        """
        return CommandRequest(
            self, CommandName.SENTINEL_FLUSHCONFIG, callback=SimpleStringCallback()
        )

    @redis_command(CommandName.SENTINEL_INFO_CACHE)
    def sentinel_infocache(
        self, *nodenames: StringT
    ) -> CommandRequest[dict[AnyStr, dict[int, dict[str, ResponsePrimitive]]]]:
        """
        Return cached INFO output from masters and replicas.
        """
        return CommandRequest(
            self,
            CommandName.SENTINEL_INFO_CACHE,
            *nodenames,
            callback=SentinelInfoCallback[AnyStr](),
        )

    @redis_command(
        CommandName.SENTINEL_MASTER,
    )
    def sentinel_master(
        self, service_name: StringT
    ) -> CommandRequest[dict[str, ResponsePrimitive]]:
        """Returns a dictionary containing the specified masters state."""

        return CommandRequest(
            self, CommandName.SENTINEL_MASTER, service_name, callback=PrimaryCallback()
        )

    @redis_command(
        CommandName.SENTINEL_MASTERS,
    )
    def sentinel_masters(self) -> CommandRequest[dict[str, dict[str, ResponsePrimitive]]]:
        """Returns a list of dictionaries containing each master's state."""

        return CommandRequest(self, CommandName.SENTINEL_MASTERS, callback=PrimariesCallback())

    @redis_command(
        CommandName.SENTINEL_MONITOR,
    )
    def sentinel_monitor(
        self, name: RedisValueT, ip: RedisValueT, port: int, quorum: int
    ) -> CommandRequest[bool]:
        """Adds a new master to Sentinel to be monitored"""

        return CommandRequest(
            self,
            CommandName.SENTINEL_MONITOR,
            name,
            ip,
            port,
            quorum,
            callback=SimpleStringCallback(),
        )

    @redis_command(CommandName.SENTINEL_MYID, version_introduced="6.2.0")
    def sentinel_myid(self) -> CommandRequest[AnyStr]:
        """Return the ID of the Sentinel instance"""

        return CommandRequest(self, CommandName.SENTINEL_MYID, callback=AnyStrCallback[AnyStr]())

    @redis_command(
        CommandName.SENTINEL_REMOVE,
    )
    def sentinel_remove(self, name: RedisValueT) -> CommandRequest[bool]:
        """Removes a master from Sentinel's monitoring"""

        return CommandRequest(
            self, CommandName.SENTINEL_REMOVE, name, callback=SimpleStringCallback()
        )

    @redis_command(
        CommandName.SENTINEL_SENTINELS,
    )
    def sentinel_sentinels(
        self, service_name: StringT
    ) -> CommandRequest[tuple[dict[str, ResponsePrimitive], ...]]:
        """Returns a list of sentinels for :paramref:`service_name`"""

        return CommandRequest(
            self,
            CommandName.SENTINEL_SENTINELS,
            service_name,
            callback=SentinelsStateCallback(),
        )

    @redis_command(
        CommandName.SENTINEL_SET,
    )
    def sentinel_set(
        self, name: RedisValueT, option: RedisValueT, value: RedisValueT
    ) -> CommandRequest[bool]:
        """Sets Sentinel monitoring parameters for a given master"""

        return CommandRequest(
            self,
            CommandName.SENTINEL_SET,
            name,
            option,
            value,
            callback=SimpleStringCallback(),
        )

    @redis_command(
        CommandName.SENTINEL_SLAVES,
    )
    def sentinel_slaves(
        self, service_name: StringT
    ) -> CommandRequest[tuple[dict[str, ResponsePrimitive], ...]]:
        """Returns a list of slaves for paramref:`service_name`"""

        return CommandRequest(
            self, CommandName.SENTINEL_SLAVES, service_name, callback=SentinelsStateCallback()
        )

    @redis_command(
        CommandName.SENTINEL_REPLICAS,
    )
    def sentinel_replicas(
        self, service_name: StringT
    ) -> CommandRequest[tuple[dict[str, ResponsePrimitive], ...]]:
        """Returns a list of replicas for :paramref:`service_name`"""

        return CommandRequest(
            self,
            CommandName.SENTINEL_REPLICAS,
            service_name,
            callback=SentinelsStateCallback(),
        )

    @redis_command(CommandName.SENTINEL_RESET)
    def sentinel_reset(self, pattern: StringT) -> CommandRequest[int]:
        """
        Reset all the masters with matching name.
        The pattern argument is a glob-style pattern.
        The reset process clears any previous state in a master (including a
        failover in progress), and removes every replica and sentinel already
        discovered and associated with the master.
        """
        return CommandRequest(
            self,
            CommandName.SENTINEL_RESET,
            pattern,
            callback=IntCallback(),
        )
