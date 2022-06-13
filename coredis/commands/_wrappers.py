from __future__ import annotations

import contextlib
import dataclasses
import functools
import random
import textwrap
from typing import TYPE_CHECKING, Any, cast

from packaging import version

from coredis.cache import AbstractCache
from coredis.commands._utils import check_version, redis_command_link
from coredis.commands.constants import CommandGroup, CommandName
from coredis.nodemanager import NodeFlag
from coredis.response._callbacks import ClusterMultiNodeCallback
from coredis.typing import (
    AsyncIterator,
    Callable,
    Coroutine,
    Dict,
    NamedTuple,
    Optional,
    P,
    R,
    ResponseType,
)

if TYPE_CHECKING:
    pass


@dataclasses.dataclass
class CacheConfig:
    key_func: Callable[..., bytes]


class CommandDetails(NamedTuple):
    command: bytes
    group: Optional[CommandGroup]
    readonly: bool
    version_introduced: Optional[version.Version]
    version_deprecated: Optional[version.Version]
    arguments: Dict[str, Dict[str, str]]
    cluster: ClusterCommandConfig
    cache_config: Optional[CacheConfig]


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


@dataclasses.dataclass
class CommandCache:
    command: bytes
    cache_config: Optional[CacheConfig]

    @contextlib.asynccontextmanager  # type: ignore
    async def __call__(
        self,
        func: Callable[P, Coroutine[Any, Any, R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> AsyncIterator[R]:
        client = args[0]
        cache = getattr(client, "cache")
        if not (self.cache_config and cache):
            yield await func(*args, **kwargs)
        else:
            assert isinstance(cache, AbstractCache)
            if not cache.healthy:
                yield await func(*args, **kwargs)
            else:
                key = self.cache_config.key_func(*args[1:], **kwargs)
                try:
                    cached = cast(
                        R,
                        cache.get(
                            self.command, key, *args[1:], *kwargs.items()  # type: ignore
                        ),
                    )
                    if not random.random() * 100.0 < min(100.0, cache.confidence):
                        actual = await func(*args, **kwargs)
                        cache.feedback(
                            self.command,
                            key,
                            *args[1:],  # type: ignore
                            *kwargs.items(),  # type: ignore
                            match=(actual == cached),
                        )
                        yield actual
                    else:
                        yield cached
                except KeyError:
                    response = await func(*args, **kwargs)
                    cache.put(
                        self.command,
                        key,
                        *args[1:],  # type: ignore
                        *kwargs.items(),  # type: ignore
                        value=cast(ResponseType, response),
                    )
                    yield response


def redis_command(
    command_name: CommandName,
    group: Optional[CommandGroup] = None,
    version_introduced: Optional[str] = None,
    version_deprecated: Optional[str] = None,
    deprecation_reason: Optional[str] = None,
    arguments: Optional[Dict[str, Dict[str, str]]] = None,
    readonly: bool = False,
    cluster: ClusterCommandConfig = ClusterCommandConfig(),
    cache_config: Optional[CacheConfig] = None,
) -> Callable[
    [Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    if not readonly and cache_config:
        raise RuntimeError()

    command_details = CommandDetails(
        command_name,
        group,
        readonly,
        version.Version(version_introduced) if version_introduced else None,
        version.Version(version_deprecated) if version_deprecated else None,
        arguments or {},
        cluster or ClusterCommandConfig(),
        cache_config,
    )

    def wrapper(
        func: Callable[P, Coroutine[Any, Any, R]]
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        command_cache = CommandCache(command_name, cache_config)

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
            async with command_cache(func, *args, **kwargs) as response:
                return response

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
        if cache_config:
            wrapped.__doc__ += """
Supports client side caching
            """
        setattr(wrapped, "__coredis_command", command_details)
        return wrapped

    return wrapper
