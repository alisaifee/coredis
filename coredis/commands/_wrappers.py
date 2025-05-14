from __future__ import annotations

import dataclasses
import functools
import textwrap
import warnings
from typing import TYPE_CHECKING, Any

from packaging import version

from coredis.commands._utils import check_version, redis_command_link
from coredis.commands.constants import CommandFlag, CommandGroup, CommandName, NodeFlag
from coredis.globals import CACHEABLE_COMMANDS, COMMAND_FLAGS, READONLY_COMMANDS
from coredis.response._callbacks import ClusterMultiNodeCallback
from coredis.typing import (
    Callable,
    Coroutine,
    NamedTuple,
    P,
    R,
    add_runtime_checks,
)

if TYPE_CHECKING:
    pass


class RedirectUsage(NamedTuple):
    reason: str
    warn: bool


@dataclasses.dataclass
class CommandDetails:
    command: bytes
    group: CommandGroup | None
    version_introduced: version.Version | None
    version_deprecated: version.Version | None
    _arguments: dict[str, dict[str, str]] | None
    cluster: ClusterCommandConfig
    flags: set[CommandFlag]
    redirect_usage: RedirectUsage | None
    arguments: dict[str, version.Version] = dataclasses.field(
        init=False, default_factory=lambda: {}
    )

    def __post_init__(self) -> None:
        self.arguments = {
            k: version.Version(v["version_introduced"])
            for k, v in (self._arguments or {}).items()
            if v.get("version_introduced")
        }


@dataclasses.dataclass
class ClusterCommandConfig:
    enabled: bool = True
    combine: ClusterMultiNodeCallback | None = None  # type: ignore
    route: NodeFlag | None = None
    split: NodeFlag | None = None

    @property
    def multi_node(self) -> bool:
        return (self.route or self.split) in [
            NodeFlag.ALL,
            NodeFlag.PRIMARIES,
            NodeFlag.REPLICAS,
        ]


def redis_command(
    command_name: CommandName,
    group: CommandGroup | None = None,
    version_introduced: str | None = None,
    version_deprecated: str | None = None,
    deprecation_reason: str | None = None,
    redirect_usage: RedirectUsage | None = None,
    arguments: dict[str, dict[str, str]] | None = None,
    flags: set[CommandFlag] | None = None,
    cluster: ClusterCommandConfig = ClusterCommandConfig(),
    cacheable: bool | None = None,
) -> Callable[[Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]]:
    readonly = False
    if flags and CommandFlag.READONLY in flags:
        READONLY_COMMANDS.add(command_name)
        readonly = True

    if not readonly and cacheable:  # noqa
        raise RuntimeError(f"Can't decorate non readonly command {command_name} with cache config")
    if cacheable:
        CACHEABLE_COMMANDS.add(command_name)
    COMMAND_FLAGS[command_name] = flags or set()

    command_details = CommandDetails(
        command_name,
        group,
        version.Version(version_introduced) if version_introduced else None,
        version.Version(version_deprecated) if version_deprecated else None,
        arguments,
        cluster or ClusterCommandConfig(),
        flags or set(),
        redirect_usage,
    )

    def wrapper(
        func: Callable[P, Coroutine[Any, Any, R]],
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        runtime_checkable = add_runtime_checks(func)

        @functools.wraps(func)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            from coredis.client import Redis, RedisCluster

            client = args[0]
            is_regular_client = isinstance(client, (Redis, RedisCluster))
            runtime_checking = not getattr(client, "noreply", None) and is_regular_client
            if redirect_usage and is_regular_client:
                if redirect_usage.warn:
                    warnings.warn(redirect_usage.reason, UserWarning, stacklevel=2)
                else:
                    raise NotImplementedError(redirect_usage.reason)
            callable = runtime_checkable if runtime_checking else func
            await check_version(
                client,  # type: ignore
                func.__name__,
                command_details,
                deprecation_reason,
                kwargs,
            )
            return await callable(*args, **kwargs)

        wrapped.__doc__ = textwrap.dedent(wrapped.__doc__ or "")
        if group:
            wrapped.__doc__ = f"""
{wrapped.__doc__}

Redis command documentation: {redis_command_link(command_name)}
"""
        if version_introduced or command_details.arguments:
            wrapped.__doc__ += """
Compatibility:
"""

        if version_introduced:
            wrapped.__doc__ += f"""
- New in :redis-version:`{version_introduced}`
"""
        if version_deprecated and deprecation_reason:
            wrapped.__doc__ += f"""
- Deprecated in :redis-version:`{version_deprecated}`
      {deprecation_reason.strip()}
"""
        elif version_deprecated:
            wrapped.__doc__ += f"""
- Deprecated in :redis-version:`{version_deprecated}`
"""
        if command_details.arguments:
            for argument, min_version in command_details.arguments.items():
                wrapped.__doc__ += f"""
- :paramref:`{argument}`: New in :redis-version:`{min_version}`
"""
        if cacheable:
            wrapped.__doc__ += """
.. hint:: Supports client side caching
"""
        if redirect_usage:
            if redirect_usage.warn:
                preamble = f".. warning:: Using ``{func.__name__}`` directly is not recommended."
            else:
                preamble = f".. danger:: Using ``{func.__name__}`` directly is not supported."
            wrapped.__doc__ += f"""
{preamble} {redirect_usage.reason}
"""

        setattr(wrapped, "__coredis_command", command_details)
        return wrapped

    return wrapper
