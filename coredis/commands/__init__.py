"""
coredis.commands
----------------
"""
from __future__ import annotations

import dataclasses
import functools
import textwrap
import warnings
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from packaging import version

from coredis.exceptions import CommandNotSupportedError
from coredis.typing import (
    AnyStr,
    Callable,
    Coroutine,
    Dict,
    Generic,
    NamedTuple,
    Optional,
    TypeVar,
    ValueT,
)

if TYPE_CHECKING:
    import coredis.client

import coredis.pool
from coredis.nodemanager import NodeFlag
from coredis.response._callbacks import ClusterMultiNodeCallback, NoopCallback
from coredis.typing import ParamSpec

from .constants import CommandGroup, CommandName

R = TypeVar("R")
P = ParamSpec("P")


@dataclasses.dataclass
class ClusterCommandConfig:
    enabled: bool = True
    flag: Optional[NodeFlag] = None
    combine: Optional[ClusterMultiNodeCallback] = None  # type: ignore

    @property
    def multi_node(self) -> bool:
        return self.flag in [NodeFlag.ALL, NodeFlag.PRIMARIES]


class CommandDetails(NamedTuple):
    command: bytes
    group: Optional[CommandGroup]
    readonly: bool
    version_introduced: Optional[version.Version]
    version_deprecated: Optional[version.Version]
    arguments: Dict[str, Dict[str, str]]
    cluster: ClusterCommandConfig


_RESPT = TypeVar("_RESPT")
_RESP3T = TypeVar("_RESP3T")
_CR_co = TypeVar("_CR_co", covariant=True)


class CommandMixin(Generic[AnyStr], ABC):
    @abstractmethod
    async def execute_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., R] = NoopCallback(),
        **options: Optional[ValueT],
    ) -> R:
        pass


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

Redis command documentation: {_redis_command_link(command_name)}
"""
        if version_introduced:
            wrapped.__doc__ += f"""
Introduced in Redis version ``{version_introduced}``
"""

        setattr(wrapped, "__coredis_command", command_details)
        return wrapped

    return wrapper


def check_version(
    instance: coredis.client.RedisConnection,
    command: bytes,
    function_name: str,
    min_version: Optional[version.Version],
    deprecated_version: Optional[version.Version],
    deprecation_reason: Optional[str],
) -> None:
    if not any([min_version, deprecated_version]):
        return

    if getattr(instance, "verify_version", False):
        server_version = getattr(instance, "server_version", None)
        if not server_version:
            return
        if min_version and server_version < min_version:
            raise CommandNotSupportedError(
                command.decode("latin-1"), str(instance.server_version)
            )
        if deprecated_version and server_version >= deprecated_version:
            if deprecation_reason:
                warnings.warn(deprecation_reason.strip())
            else:
                warnings.warn(
                    f"{function_name}() is deprecated since redis version {deprecated_version}. "
                )


def _redis_command_link(command: CommandName) -> str:
    return f'`{str(command)} <https://redis.io/commands/{str(command).lower().replace(" ", "-")}>`_'


__all__ = (
    "CommandGroup",
    "CommandName",
    "CommandMixin",
    "redis_command",
    "ClusterCommandConfig",
    "CommandDetails",
)
