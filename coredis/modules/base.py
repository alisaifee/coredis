from __future__ import annotations

import functools
import textwrap
from abc import ABCMeta
from typing import TYPE_CHECKING, Any, cast

from packaging import version

from coredis.config import Config

from .._protocols import AbstractExecutor
from ..commands._utils import redis_command_link
from ..commands._wrappers import (
    CacheConfig,
    ClusterCommandConfig,
    CommandCache,
    CommandDetails,
)
from ..commands.constants import CommandFlag, CommandGroup, CommandName
from ..exceptions import CommandSyntaxError, ModuleCommandNotSupportedError
from ..globals import COMMAND_FLAGS, MODULE_GROUPS, MODULES, READONLY_COMMANDS
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
    Type,
    ValueT,
    add_runtime_checks,
)

if TYPE_CHECKING:
    import coredis.client


async def ensure_compatibility(
    client: "coredis.client.Client[Any]",
    module: str,
    command_details: CommandDetails,
    kwargs: Dict[str, Any],
) -> None:
    if (
        Config.optimized
        or not command_details.version_introduced
        or not getattr(client, "verify_version", False)
        or getattr(client, "noreply", False)
    ):
        return
    if command_details.version_introduced:
        module_version = await client.get_server_module_version(module)
        if module_version and command_details.version_introduced <= module_version:
            if command_details.arguments and set(
                command_details.arguments.keys()
            ).intersection(kwargs.keys()):
                for argument, version_introduced in command_details.arguments.items():
                    if version_introduced and version_introduced > module_version:
                        raise CommandSyntaxError(
                            {argument},
                            (
                                f"{command_details.command.decode('latin-1')} with `{argument}` "
                                f"is not supported in {module} version {module_version}"
                            ),
                        )
            return
        raise ModuleCommandNotSupportedError(
            command_details.command.decode("latin-1"),
            module,
            str(module_version) if module_version else "",
        )


def module_command(
    command_name: CommandName,
    module: "Type[Module[Any]]",
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
        version.Version(version_introduced) if version_introduced else None,
        version.Version(version_deprecated) if version_deprecated else None,
        arguments,
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
            client = cast("coredis.client.Client[Any]", mg.client)
            is_regular_client = isinstance(client, (Redis, RedisCluster))
            runtime_checking = (
                not getattr(client, "noreply", None) and is_regular_client
            )
            callable = runtime_checkable if runtime_checking else func
            await ensure_compatibility(client, module.NAME, command_details, kwargs)
            async with command_cache(callable, *args, **kwargs) as response:
                return response

        wrapped.__doc__ = textwrap.dedent(wrapped.__doc__ or "")
        if group:
            wrapped.__doc__ = f"""
{wrapped.__doc__}

{module.FULL_NAME} command documentation: {redis_command_link(command_name)}
            """
        if (
            (version_introduced and version_introduced != "1.0.0")
            or version_deprecated
            or command_details.arguments
        ):
            wrapped.__doc__ += """
Compatibility:
"""

            if version_introduced and version_introduced != "1.0.0":
                wrapped.__doc__ += f"""
- New in {module.FULL_NAME} version: `{version_introduced}`
                """
            if version_deprecated:
                wrapped.__doc__ += f"""
- Deprecated in {module.FULL_NAME} version: `{version_deprecated}`
        """
            if command_details.arguments:
                for argument, min_version in command_details.arguments.items():
                    wrapped.__doc__ += f"""
- :paramref:`{argument}`: New in {module.FULL_NAME} version `{min_version}`
                    """
        if cache_config:
            wrapped.__doc__ += """
.. hint:: Supports client side caching
"""
        setattr(wrapped, "__coredis_command", command_details)
        setattr(wrapped, "__coredis_module", module)
        return wrapped

    return wrapper


class ModuleRegistry(ABCMeta):
    """
    :meta private:
    """

    NAME: str
    FULL_NAME: str
    DESCRIPTION: str
    DOCUMENTATION_URL: str
    COMMAND_GROUPS: Dict[CommandGroup, Type[ModuleGroup[Any]]]

    def __new__(
        cls, name: str, bases: Tuple[type, ...], namespace: Dict[str, object]
    ) -> ModuleRegistry:
        if "COMMAND_GROUPS" not in namespace:
            namespace["COMMAND_GROUPS"] = {}
        kls = super().__new__(cls, name, bases, namespace)
        if getattr(kls, "NAME", None):
            MODULES[kls.NAME] = kls
            kls.__doc__ = textwrap.dedent(kls.__doc__ or "")
            kls.__doc__ += f"""
Type representation of the `{kls.FULL_NAME} <{kls.DOCUMENTATION_URL}>`__ module.
The class isn't meant to be used directly and exists as a convenient way to capture
module attributes for documentation & internal use.
            """
        return kls


class ModuleGroupRegistry(ABCMeta):
    """
    :meta private:
    """

    MODULE: Type[Module[Any]]
    COMMAND_GROUP: CommandGroup

    def __new__(
        cls, name: str, bases: Tuple[type, ...], namespace: Dict[str, object]
    ) -> ModuleGroupRegistry:
        kls = super().__new__(cls, name, bases, namespace)
        if getattr(kls, "MODULE", None):
            MODULE_GROUPS.add(kls)
            kls.MODULE.COMMAND_GROUPS[kls.COMMAND_GROUP] = cast(
                Type[ModuleGroup[Any]], kls
            )
            original_doc = textwrap.dedent(kls.__doc__ or "")
            kls.__doc__ = f"""
Container for the commands in the ``{kls.COMMAND_GROUP.value}`` command group of the
`{kls.MODULE.FULL_NAME} <{kls.MODULE.DOCUMENTATION_URL}>`__ module.

{original_doc}
            """
        return kls


class Module(Generic[AnyStr], metaclass=ModuleRegistry):
    """
    Base class for Redis module implementations.
    This class isn't meant to be used directly by the end
    user, and exists to provide a convenient way to document and group
    redis modules and the command groups that they expose.
    """

    NAME: ClassVar[str]
    """
    The name of the module as reported by ``MODULES LIST``
    """
    FULL_NAME: ClassVar[str]
    """
    The common name used to refer to the module if it differs from
    the internal name
    """
    DESCRIPTION: ClassVar[str]
    """
    A brief description of what the module does
    """
    DOCUMENTATION_URL: ClassVar[str]
    """
    A link to the module documentation
    """

    COMMAND_GROUPS: ClassVar[Dict[CommandGroup, Type[ModuleGroup[Any]]]]
    """
    Mapping of command groups that implement this module. This is auto
    populated by the :class:`ModuleGroupRegistry` metaclass.
    """


class ModuleGroup(Generic[AnyStr], metaclass=ModuleGroupRegistry):
    """
    Base class for Redis module command groups
    """

    MODULE: ClassVar[Type[Module[Any]]]
    """
    The module to which this command group belongs to
    
    :meta private: 
    """
    COMMAND_GROUP: ClassVar[CommandGroup]
    """
    The command group this class implements

    :meta private: 
    """

    def __init__(self, client: AbstractExecutor):
        self.client = client

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
