from __future__ import annotations

import functools
import textwrap
import weakref
from abc import ABCMeta
from typing import Any, cast

from .._protocols import AbstractExecutor
from ..commands._utils import redis_command_link
from ..commands._wrappers import (
    CacheConfig,
    ClusterCommandConfig,
    CommandCache,
    CommandDetails,
)
from ..commands.constants import CommandFlag, CommandGroup, CommandName
from ..globals import COMMAND_FLAGS, MODULE_GROUPS, READONLY_COMMANDS
from ..response._callbacks import NoopCallback
from ..typing import (
    AnyStr,
    Callable,
    ClassVar,
    Coroutine,
    Dict,
    Generic,
    Optional,
    P,
    R,
    Set,
    Tuple,
    ValueT,
    add_runtime_checks,
)


def module_command(
    command_name: CommandName,
    module: str,
    group: CommandGroup,
    flags: Optional[Set[CommandFlag]] = None,
    cluster: ClusterCommandConfig = ClusterCommandConfig(),
    cache_config: Optional[CacheConfig] = None,
    version_introduced: Optional[str] = None,
    version_deprecated: Optional[str] = None,
    arguments: Optional[Dict[str, Dict[str, str]]] = None,
) -> Callable[
    [Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    command_details = CommandDetails(
        command_name,
        group,
        None,
        None,
        {},
        cluster or ClusterCommandConfig(),
        cache_config,
        flags or set(),
        None,
    )

    def wrapper(
        func: Callable[P, Coroutine[Any, Any, R]]
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        runtime_checkable = add_runtime_checks(func)
        command_cache = CommandCache(command_name, cache_config)
        if flags and CommandFlag.READONLY in flags:
            READONLY_COMMANDS.add(command_name)
        COMMAND_FLAGS[command_name] = flags or set()

        @functools.wraps(func)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            from coredis.client import Redis, RedisCluster

            mg = cast(ModuleGroup[bytes], args[0])
            client = mg.client
            is_regular_client = isinstance(client, (Redis, RedisCluster))
            runtime_checking = (
                not getattr(client, "noreply", None) and is_regular_client
            )
            callable = runtime_checkable if runtime_checking else func
            async with command_cache(callable, *args, **kwargs) as response:
                return response

        wrapped.__doc__ = textwrap.dedent(wrapped.__doc__ or "")
        if group:
            wrapped.__doc__ = f"""
{wrapped.__doc__}

Redis {module} module command documentation: {redis_command_link(command_name)}
            """
        if cache_config:
            wrapped.__doc__ += """
Supports client side caching
            """
        setattr(wrapped, "__coredis_command", command_details)
        setattr(wrapped, "__coredis_module", module)
        return wrapped

    return wrapper


class ModuleGroupRegistry(ABCMeta):
    MODULE: Optional[str]

    def __new__(
        cls, name: str, bases: Tuple[type, ...], namespace: Dict[str, object]
    ) -> ModuleGroupRegistry:
        kls = super().__new__(cls, name, bases, namespace)
        if kls.MODULE:
            MODULE_GROUPS.add(kls)
        return kls


class ModuleGroup(Generic[AnyStr], metaclass=ModuleGroupRegistry):
    #: The name of the module as reported by ``MODULES LIST``
    MODULE: ClassVar[Optional[str]] = None

    def __init__(self, client: AbstractExecutor):
        self.client = weakref.proxy(client)

    async def execute_module_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., R] = NoopCallback(),
        **options: Optional[ValueT],
    ) -> R:
        return cast(
            R,
            await self.client.execute_command(
                command, *args, callback=callback, **options
            ),
        )
