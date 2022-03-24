"""
coredis.commands
----------------
"""
import dataclasses
import enum
import functools
import warnings
from abc import ABC
from types import FunctionType
from typing import (
    TYPE_CHECKING,
    Any,
    AnyStr,
    Callable,
    Coroutine,
    Dict,
    Generic,
    NamedTuple,
    Optional,
    TypeVar,
    Union,
    cast,
)

from packaging import version

from coredis.exceptions import CommandNotSupportedError

if TYPE_CHECKING:
    import coredis.client

import coredis.pool
from coredis.response.callbacks import ParametrizedCallback, SimpleCallback
from coredis.typing import AbstractExecutor, ParamSpec
from coredis.utils import NodeFlag

R = TypeVar("R")
P = ParamSpec("P")


def check_version(
    instance: Any,
    command: str,
    function_name: str,
    min_version: Optional[version.Version],
    deprecated_version: Optional[version.Version],
    deprecation_reason: Optional[str],
):
    if not any([min_version, deprecated_version]):
        return

    client = cast("coredis.client.RedisConnection", instance)
    if client.server_version:
        if min_version and client.server_version < min_version:
            raise CommandNotSupportedError(command, client.server_version)
        if deprecated_version and client.server_version >= deprecated_version:
            if deprecation_reason:
                warnings.warn(deprecation_reason.strip())
            else:
                warnings.warn(
                    f"{function_name}() is deprecated since redis version {deprecated_version}. "
                )


@dataclasses.dataclass
class ClusterCommandConfig:
    enabled: bool = True
    pipeline: bool = True
    flag: Optional[NodeFlag] = None
    combine: Optional[Callable] = None

    @property
    def multi_node(self):
        return self.flag in [NodeFlag.ALL, NodeFlag.PRIMARIES]


class CommandDetails(NamedTuple):
    command: str
    readonly: bool
    version_introduced: Optional[version.Version]
    version_deprecated: Optional[version.Version]
    arguments: Dict[str, Dict[str, str]]
    cluster: ClusterCommandConfig
    response_callback: Optional[
        Union[FunctionType, SimpleCallback, ParametrizedCallback]
    ]


def redis_command(
    command_name: str,
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
            check_version(
                args[0],  # type: ignore
                command_name,
                func.__name__,
                command_details.version_introduced,
                command_details.version_deprecated,
                deprecation_reason,
            )
            return await func(*args, **kwargs)

        wrapped.__doc__ = f"""
        {wrapped.__doc__ or ""}
        """
        if group:
            wrapped.__doc__ += f"""
        Redis command documentation: {_redis_command_link(command_name)}
            """
        if version_introduced:
            wrapped.__doc__ += f"""
        Introduced in Redis version ``{version_introduced}``
        """

        setattr(wrapped, "__coredis_command", command_details)
        return wrapped

    return wrapper


class CommandGroup(enum.Enum):
    BITMAP = "bitmap"
    CLUSTER = "cluster"
    CONNECTION = "connection"
    GENERIC = "generic"
    GEO = "geo"
    HASH = "hash"
    HYPERLOGLOG = "hyperloglog"
    LIST = "list"
    PUBSUB = "pubsub"
    SCRIPTING = "scripting"
    SENTINEL = "sentinel"
    SERVER = "server"
    SET = "set"
    SORTED_SET = "sorted-set"
    STREAM = "stream"
    STRING = "string"
    TRANSACTIONS = "transactions"


def _redis_command_link(command):
    return (
        f'`{command} <https://redis.io/commands/{command.lower().replace(" ", "-")}>`_'
    )


class CommandMixin(Generic[AnyStr], AbstractExecutor[AnyStr], ABC):
    connection_pool: "coredis.pool.ConnectionPool"
