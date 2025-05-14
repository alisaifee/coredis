from __future__ import annotations

import asyncio
import dataclasses
import functools
import textwrap
import warnings
from typing import Any

from packaging import version

from coredis._protocols import AbstractExecutor
from coredis.commands._utils import check_version, redis_command_link
from coredis.commands.constants import CommandFlag, CommandGroup, CommandName, NodeFlag
from coredis.globals import CACHEABLE_COMMANDS, COMMAND_FLAGS, READONLY_COMMANDS
from coredis.response._callbacks import ClusterMultiNodeCallback, ResponseCallback
from coredis.typing import (
    Callable,
    NamedTuple,
    P,
    R,
    T_co,
    TypeVar,
    ValueT,
    add_runtime_checks,
)


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
            NodeFlag.SLOT_ID,
        ]


U = TypeVar("U")


class CommandTask(asyncio.Task[T_co]):
    def __init__(
        self,
        client: AbstractExecutor,
        command_name: bytes,
        *args: ValueT,
        callback: ResponseCallback[Any, Any, T_co],
        callback_options: dict[str, Any] | None = None,
    ) -> None:
        self.command = command_name
        self.callback = callback
        self.callback_options = callback_options
        self.arguments = args
        self.client: AbstractExecutor = client

        super().__init__(self.run())

    async def run(self) -> T_co:
        return await self.client.execute_command(
            self.command,
            *self.arguments,
            **(self.callback_options or {}),
            callback=self.callback,
        )

    async def transform(self, transformer: Callable[[T_co], U]) -> U:
        return transformer(await self)


def redis_command(
    command_name: CommandName,
    *,
    group: CommandGroup | None = None,
    version_introduced: str | None = None,
    version_deprecated: str | None = None,
    deprecation_reason: str | None = None,
    redirect_usage: RedirectUsage | None = None,
    arguments: dict[str, dict[str, str]] | None = None,
    flags: set[CommandFlag] | None = None,
    cluster: ClusterCommandConfig = ClusterCommandConfig(),
    cacheable: bool | None = None,
) -> Callable[[Callable[P, CommandTask[R]]], Callable[P, CommandTask[R]]]:
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
        func: Callable[P, CommandTask[R]],
    ) -> Callable[P, CommandTask[R]]:
        runtime_checkable = add_runtime_checks(func)

        @functools.wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> CommandTask[R]:
            from coredis import Redis, RedisCluster

            is_regular_client = isinstance(args[0], (Redis, RedisCluster))
            if redirect_usage and is_regular_client:
                if redirect_usage.warn:
                    warnings.warn(redirect_usage.reason, UserWarning, stacklevel=2)
                else:
                    raise NotImplementedError(redirect_usage.reason)
            runtime_checking = not getattr(args[0], "noreply", None) and is_regular_client
            check_version(
                args[0],  # type: ignore
                func.__name__,
                command_details,
                deprecation_reason,
                kwargs,
            )
            return (func if not runtime_checking else runtime_checkable)(*args, **kwargs)

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
