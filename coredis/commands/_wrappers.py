from __future__ import annotations

import dataclasses
import functools
import textwrap
from typing import Any

from packaging import version

from coredis.commands._utils import check_version, redis_command_link
from coredis.commands.constants import CommandGroup, CommandName
from coredis.nodemanager import NodeFlag
from coredis.response._callbacks import ClusterMultiNodeCallback
from coredis.typing import Callable, Coroutine, Dict, NamedTuple, Optional, P, R


class CommandDetails(NamedTuple):
    command: bytes
    group: Optional[CommandGroup]
    readonly: bool
    version_introduced: Optional[version.Version]
    version_deprecated: Optional[version.Version]
    arguments: Dict[str, Dict[str, str]]
    cluster: ClusterCommandConfig


@dataclasses.dataclass
class ClusterCommandConfig:
    enabled: bool = True
    combine: Optional[ClusterMultiNodeCallback] = None  # type: ignore
    route: Optional[NodeFlag] = None
    split: Optional[NodeFlag] = None

    @property
    def multi_node(self) -> bool:
        return (self.route or self.split) in [
            NodeFlag.ALL,
            NodeFlag.PRIMARIES,
            NodeFlag.REPLICAS,
        ]


def redis_command(
    command_name: CommandName,
    group: Optional[CommandGroup] = None,
    version_introduced: Optional[str] = None,
    version_deprecated: Optional[str] = None,
    deprecation_reason: Optional[str] = None,
    arguments: Optional[Dict[str, Dict[str, str]]] = None,
    readonly: bool = False,
    cluster: ClusterCommandConfig = ClusterCommandConfig(),
) -> Callable[
    [Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    command_details = CommandDetails(
        command_name,
        group,
        readonly,
        version.Version(version_introduced) if version_introduced else None,
        version.Version(version_deprecated) if version_deprecated else None,
        arguments or {},
        cluster or ClusterCommandConfig(),
    )

    def wrapper(
        func: Callable[P, Coroutine[Any, Any, R]]
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        @functools.wraps(func)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            check_version(
                args[0],  # type: ignore
                command_name,
                func.__name__,
                command_details.version_introduced,
                command_details.version_deprecated,
                deprecation_reason,
            )
            return await func(*args, **kwargs)

        wrapped.__doc__ = textwrap.dedent(wrapped.__doc__ or "")
        if group:
            wrapped.__doc__ = f"""
{wrapped.__doc__}

Redis command documentation: {redis_command_link(command_name)}
"""
        if version_introduced:
            wrapped.__doc__ += f"""
New in :redis-version:`{version_introduced}`
"""
        if version_deprecated and deprecation_reason:
            wrapped.__doc__ += f"""
Deprecated in :redis-version:`{version_deprecated}`
  {deprecation_reason.strip()}
        """
        elif version_deprecated:
            wrapped.__doc__ += f"""
Deprecated in :redis-version:`{version_deprecated}`
            """

        setattr(wrapped, "__coredis_command", command_details)
        return wrapped

    return wrapper
