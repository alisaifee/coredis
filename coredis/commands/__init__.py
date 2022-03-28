"""
coredis.commands
----------------
"""
import dataclasses
import functools
import textwrap
import warnings
from abc import ABC
from types import FunctionType
from typing import TYPE_CHECKING, cast

from packaging import version

from coredis.exceptions import CommandNotSupportedError
from coredis.typing import (
    Any,
    AnyStr,
    Callable,
    Coroutine,
    Dict,
    Generic,
    NamedTuple,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

from ..tokens import PrefixToken

if TYPE_CHECKING:
    import coredis.client

import coredis.pool
from coredis.response.callbacks import ParametrizedCallback, SimpleCallback
from coredis.typing import AbstractExecutor, ParamSpec, ValueT
from coredis.utils import NodeFlag

from .constants import CommandGroup, CommandName

R = TypeVar("R")
P = ParamSpec("P")


@dataclasses.dataclass
class ClusterCommandConfig:
    enabled: bool = True
    flag: Optional[NodeFlag] = None
    combine: Optional[Callable] = None

    @property
    def multi_node(self):
        return self.flag in [NodeFlag.ALL, NodeFlag.PRIMARIES]


class CommandDetails(NamedTuple):
    command: bytes
    readonly: bool
    version_introduced: Optional[version.Version]
    version_deprecated: Optional[version.Version]
    arguments: Dict[str, Dict[str, str]]
    cluster: ClusterCommandConfig
    response_callback: Optional[
        Union[FunctionType, SimpleCallback, ParametrizedCallback]
    ]


class CommandMixin(Generic[AnyStr], AbstractExecutor[AnyStr], ABC):
    connection_pool: "coredis.pool.ConnectionPool"


def redis_command(
    command_name: CommandName,
    group: Optional["CommandGroup"] = None,
    version_introduced: Optional[str] = None,
    version_deprecated: Optional[str] = None,
    deprecation_reason: Optional[str] = None,
    arguments: Optional[Dict[str, Dict[str, str]]] = None,
    readonly: bool = False,
    response_callback: Optional[
        Union[FunctionType, SimpleCallback, ParametrizedCallback]
    ] = None,
    cluster: ClusterCommandConfig = ClusterCommandConfig(),
) -> Callable[
    [Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    command_details = CommandDetails(
        command_name,
        readonly,
        version.Version(version_introduced) if version_introduced else None,
        version.Version(version_deprecated) if version_deprecated else None,
        arguments or {},
        cluster or ClusterCommandConfig(),
        response_callback,
    )

    def wrapper(
        func: Callable[P, Coroutine[Any, Any, R]]
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        @functools.wraps(func)
        async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
            _check_version(
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


def keys_from_command(args: Tuple[ValueT, ...]) -> Tuple[ValueT, ...]:
    if len(args) <= 1:
        return ()

    command = args[0]

    if command in {CommandName.EVAL, CommandName.EVALSHA, CommandName.FCALL}:
        numkeys = int(args[2])
        keys = args[3 : 3 + numkeys]
    elif command in {CommandName.XREAD, CommandName.XREADGROUP}:
        try:
            idx = args.index(PrefixToken.STREAMS) + 1
            keys = (args[idx],)
        except ValueError:
            keys = ()
    elif command in {CommandName.XGROUP, CommandName.XINFO}:
        keys = (args[2],)
    elif command == CommandName.OBJECT:
        keys = (args[2],)
    elif command in {
        CommandName.BLMPOP,
        CommandName.BZMPOP,
    }:
        keys = args[3 : int(args[2]) + 3 : 1]
    elif command in {
        CommandName.BZPOPMAX,
        CommandName.BRPOP,
        CommandName.BZPOPMIN,
        CommandName.BLPOP,
    }:
        keys = args[1 : len(args) - 1]
    elif command in {
        CommandName.SINTER,
        CommandName.SDIFF,
        CommandName.SSUBSCRIBE,
        CommandName.MGET,
        CommandName.PFCOUNT,
        CommandName.EXISTS,
        CommandName.SUNION,
        CommandName.DEL,
        CommandName.TOUCH,
        CommandName.WATCH,
        CommandName.SUNSUBSCRIBE,
        CommandName.UNLINK,
    }:
        keys = args[1:]
    elif command == CommandName.LCS:
        keys = args[1:3]
    elif command in {
        CommandName.LMPOP,
        CommandName.ZINTERCARD,
        CommandName.ZMPOP,
        CommandName.ZUNION,
        CommandName.ZINTER,
        CommandName.ZDIFF,
        CommandName.SINTERCARD,
    }:
        keys = args[2 : int(args[1]) + 2 : 1]
    elif command in {
        CommandName.MEMORY_USAGE,
        CommandName.XGROUP_CREATE,
        CommandName.XGROUP_DESTROY,
        CommandName.XGROUP_SETID,
        CommandName.XINFO_STREAM,
        CommandName.XINFO_GROUPS,
        CommandName.OBJECT_ENCODING,
        CommandName.OBJECT_REFCOUNT,
        CommandName.OBJECT_IDLETIME,
        CommandName.XGROUP_DELCONSUMER,
        CommandName.XGROUP_CREATECONSUMER,
        CommandName.OBJECT_FREQ,
        CommandName.XINFO_CONSUMERS,
    }:
        keys = (args[1],)
    elif command in {CommandName.MSETNX, CommandName.MSET}:
        keys = args[1:-1:2]
    elif command == CommandName.KEYS:
        return ()
    else:
        keys = (args[1],)
    return keys


def _check_version(
    instance: Any,
    command: bytes,
    function_name: str,
    min_version: Optional[version.Version],
    deprecated_version: Optional[version.Version],
    deprecation_reason: Optional[str],
):
    if not any([min_version, deprecated_version]):
        return

    client = cast("coredis.client.RedisConnection", instance)

    if getattr(client, "verify_version", False):
        server_version = getattr(client, "server_version", None)
        if not server_version:
            return
        if min_version and server_version < min_version:
            raise CommandNotSupportedError(command, client.server_version)
        if deprecated_version and server_version >= deprecated_version:
            if deprecation_reason:
                warnings.warn(deprecation_reason.strip())
            else:
                warnings.warn(
                    f"{function_name}() is deprecated since redis version {deprecated_version}. "
                )


def _redis_command_link(command):
    return f'`{str(command)} <https://redis.io/commands/{str(command).lower().replace(" ", "-")}>`_'


__all__ = (
    "CommandGroup",
    "CommandName",
    "CommandMixin",
    "redis_command",
    "ClusterCommandConfig",
    "CommandDetails",
)
