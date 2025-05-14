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
    ClusterCommandConfig,
    CommandDetails,
)
from ..commands.constants import CommandFlag, CommandGroup, CommandName
from ..commands.request import CommandRequest
from ..exceptions import CommandSyntaxError, ModuleCommandNotSupportedError
from ..globals import CACHEABLE_COMMANDS, COMMAND_FLAGS, MODULE_GROUPS, MODULES, READONLY_COMMANDS
from ..typing import (
    AnyStr,
    Callable,
    ClassVar,
    Generic,
    P,
    R,
    add_runtime_checks,
)

if TYPE_CHECKING:
    import coredis.client


def ensure_compatibility(
    client: coredis.client.Client[Any],
    module: str,
    command_details: CommandDetails,
    kwargs: dict[str, Any],
) -> None:
    if (
        Config.optimized
        or not command_details.version_introduced
        or not getattr(client, "verify_version", False)
        or not getattr(client, "server_version", None)
        or getattr(client, "noreply", False)
    ):
        return
    if command_details.version_introduced:
        module_version = client.get_server_module_version(module)
        if module_version and command_details.version_introduced <= module_version:
            if command_details.arguments and set(command_details.arguments.keys()).intersection(
                kwargs.keys()
            ):
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
    module: type[Module[Any]],
    group: CommandGroup,
    flags: set[CommandFlag] | None = None,
    cluster: ClusterCommandConfig = ClusterCommandConfig(),
    cacheable: bool | None = None,
    version_introduced: str | None = None,
    version_deprecated: str | None = None,
    arguments: dict[str, dict[str, str]] | None = None,
) -> Callable[[Callable[P, CommandRequest[R]]], Callable[P, CommandRequest[R]]]:
    command_details = CommandDetails(
        command_name,
        group,
        version.Version(version_introduced) if version_introduced else None,
        version.Version(version_deprecated) if version_deprecated else None,
        arguments,
        cluster or ClusterCommandConfig(),
        flags or set(),
        None,
    )

    def wrapper(
        func: Callable[P, CommandRequest[R]],
    ) -> Callable[P, CommandRequest[R]]:
        runtime_checkable = add_runtime_checks(func)
        if flags and CommandFlag.READONLY in flags:
            READONLY_COMMANDS.add(command_name)
        if cacheable:
            CACHEABLE_COMMANDS.add(command_name)
        COMMAND_FLAGS[command_name] = flags or set()

        @functools.wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> CommandRequest[R]:
            from coredis.client import Redis, RedisCluster

            mg = cast(ModuleGroup[bytes], args[0])
            client = cast("coredis.client.Client[Any]", mg.client)
            is_regular_client = isinstance(client, (Redis, RedisCluster))
            runtime_checking = not getattr(client, "noreply", None) and is_regular_client
            callable = runtime_checkable if runtime_checking else func
            ensure_compatibility(client, module.NAME, command_details, kwargs)
            return callable(*args, **kwargs)

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
        if cacheable:
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
    COMMAND_GROUPS: dict[CommandGroup, type[ModuleGroup[Any]]]

    def __new__(
        cls, name: str, bases: tuple[type, ...], namespace: dict[str, object]
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

    MODULE: type[Module[Any]]
    COMMAND_GROUP: CommandGroup

    def __new__(
        cls, name: str, bases: tuple[type, ...], namespace: dict[str, object]
    ) -> ModuleGroupRegistry:
        kls = super().__new__(cls, name, bases, namespace)
        if getattr(kls, "MODULE", None):
            MODULE_GROUPS.add(kls)
            kls.MODULE.COMMAND_GROUPS[kls.COMMAND_GROUP] = cast(type[ModuleGroup[Any]], kls)
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

    COMMAND_GROUPS: ClassVar[dict[CommandGroup, type[ModuleGroup[Any]]]]
    """
    Mapping of command groups that implement this module. This is auto
    populated by the :class:`ModuleGroupRegistry` metaclass.
    """


class ModuleGroup(Generic[AnyStr], metaclass=ModuleGroupRegistry):
    """
    Base class for Redis module command groups
    """

    MODULE: ClassVar[type[Module[Any]]]
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
