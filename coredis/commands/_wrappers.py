from __future__ import annotations

import contextlib
import dataclasses
import functools
import random
import textwrap
import warnings
from typing import TYPE_CHECKING, Any, cast

from packaging import version

from coredis.cache import AbstractCache, SupportsSampling
from coredis.commands._utils import check_version, redis_command_link
from coredis.commands.constants import CommandFlag, CommandGroup, CommandName, NodeFlag
from coredis.globals import COMMAND_FLAGS, READONLY_COMMANDS
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
    Set,
    add_runtime_checks,
)

if TYPE_CHECKING:
    pass


@dataclasses.dataclass
class CacheConfig:
    key_func: Callable[..., bytes]


class RedirectUsage(NamedTuple):
    reason: str
    warn: bool


@dataclasses.dataclass
class CommandDetails:
    command: bytes
    group: Optional[CommandGroup]
    version_introduced: Optional[version.Version]
    version_deprecated: Optional[version.Version]
    _arguments: Optional[Dict[str, Dict[str, str]]]
    cluster: ClusterCommandConfig
    cache_config: Optional[CacheConfig]
    flags: Set[CommandFlag]
    redirect_usage: Optional[RedirectUsage]
    arguments: Dict[str, version.Version] = dataclasses.field(
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

    @contextlib.asynccontextmanager
    async def __call__(
        self,
        func: Callable[P, Coroutine[Any, Any, R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> AsyncIterator[R]:
        from coredis.modules.base import ModuleGroup

        client = args[0]
        if isinstance(args[0], ModuleGroup):
            client = args[0].client

        cache = getattr(client, "cache")
        noreply = getattr(client, "noreply")
        if not (self.cache_config and cache) or noreply:
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
                    if isinstance(
                        cache, SupportsSampling
                    ) and not random.random() * 100.0 < min(100.0, cache.confidence):
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
    redirect_usage: Optional[RedirectUsage] = None,
    arguments: Optional[Dict[str, Dict[str, str]]] = None,
    flags: Optional[Set[CommandFlag]] = None,
    cluster: ClusterCommandConfig = ClusterCommandConfig(),
    cache_config: Optional[CacheConfig] = None,
) -> Callable[
    [Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    readonly = False
    if flags and CommandFlag.READONLY in flags:
        READONLY_COMMANDS.add(command_name)
        readonly = True

    if not readonly and cache_config:  # noqa
        raise RuntimeError(
            f"Can't decorate non readonly command {command_name} with cache config"
        )

    COMMAND_FLAGS[command_name] = flags or set()

    command_details = CommandDetails(
        command_name,
        group,
        version.Version(version_introduced) if version_introduced else None,
        version.Version(version_deprecated) if version_deprecated else None,
        arguments,
        cluster or ClusterCommandConfig(),
        cache_config,
        flags or set(),
        redirect_usage,
    )

    def wrapper(
        func: Callable[P, Coroutine[Any, Any, R]],
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        command_cache = CommandCache(command_name, cache_config)
        runtime_checkable = add_runtime_checks(func)

        @functools.wraps(func)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            from coredis.client import Redis, RedisCluster

            client = args[0]
            is_regular_client = isinstance(client, (Redis, RedisCluster))
            runtime_checking = (
                not getattr(client, "noreply", None) and is_regular_client
            )
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
            async with command_cache(callable, *args, **kwargs) as response:
                return response

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
        if cache_config:
            wrapped.__doc__ += """
.. hint:: Supports client side caching
"""
        if redirect_usage:
            if redirect_usage.warn:
                preamble = f".. warning:: Using ``{func.__name__}`` directly is not recommended."
            else:
                preamble = (
                    f".. danger:: Using ``{func.__name__}`` directly is not supported."
                )
            wrapped.__doc__ += f"""
{preamble} {redirect_usage.reason}
"""

        setattr(wrapped, "__coredis_command", command_details)
        return wrapped

    return wrapper
