from __future__ import annotations

import datetime
import enum
import functools
import textwrap
from types import FunctionType
from typing import AnyStr

from packaging import version

from coredis.client import Redis
from coredis.commands import ClusterCommandConfig, CommandDetails, _check_version
from coredis.commands.constants import CommandGroup
from coredis.response.callbacks import BoolCallback, ResponseCallback
from coredis.typing import (
    Any,
    Callable,
    CommandArgList,
    Coroutine,
    Dict,
    KeyT,
    Literal,
    Optional,
    P,
    R,
    Set,
    StringT,
    Union,
)
from coredis.utils import normalized_time_seconds


def _keydb_command_link(command):
    canonical_command = str(command).lower().replace(" ", "-")
    return (
        f"`{str(command)} <https://docs.keydb.dev/docs/commands#{canonical_command}>`_"
    )


@enum.unique
class CommandName(bytes, enum.Enum):
    """
    Enum for listing all redis commands
    """

    @functools.cached_property
    def variants(self) -> Set[StringT]:
        decoded = str(self)
        return {self.value.lower(), self.value, decoded.lower(), decoded.upper()}

    def __eq__(self, other):
        """
        Since redis tokens are case insensitive allow mixed case
        Additionally allow strings to be passed in instead of
        bytes.
        """

        if other:
            if isinstance(other, CommandName):
                return self.value == other.value
            else:
                return other in self.variants

    def __hash__(self):
        return hash(self.value)

    def __str__(self):
        return self.decode("latin-1")

    EXPIREMEMBER = b"EXPIREMEMBER"
    EXPIREMEMBERAT = b"EXPIREMEMBERAT"


def keydb_command(
    command_name: CommandName,
    group: Optional[CommandGroup] = None,
    version_introduced: Optional[str] = None,
    version_deprecated: Optional[str] = None,
    deprecation_reason: Optional[str] = None,
    arguments: Optional[Dict[str, Dict[str, str]]] = None,
    readonly: bool = False,
    response_callback: Optional[
        Union[FunctionType, ResponseCallback, ResponseCallback]
    ] = None,
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

KeyDB command documentation: {_keydb_command_link(command_name)}
"""

        setattr(wrapped, "__coredis_command", command_details)
        return wrapped

    return wrapper


class KeyDB(Redis[AnyStr]):
    """
    Client for `KeyDB <https://keydb.dev>`__

    The client is mostly :class:`coredis.Redis` with a couple of extra
    commands specific to KeyDB.
    """

    @keydb_command(
        CommandName.EXPIREMEMBER, CommandGroup.GENERIC, response_callback=BoolCallback()
    )
    async def expiremember(
        self,
        key: KeyT,
        subkey: KeyT,
        delay: int,
        unit: Optional[Literal[b"s", b"ms"]] = None,
    ) -> bool:
        """
        Set a subkey's time to live in seconds (or milliseconds)
        """
        pieces: CommandArgList = [key, subkey, delay]
        if unit:
            pieces.append(unit.lower())
        return await self.execute_command(CommandName.EXPIREMEMBER, *pieces)

    @keydb_command(
        CommandName.EXPIREMEMBERAT,
        CommandGroup.GENERIC,
        response_callback=BoolCallback(),
    )
    async def expirememberat(
        self, key: KeyT, subkey: KeyT, unix_time_seconds: Union[int, datetime.datetime]
    ) -> bool:
        """
        Set the expiration for a subkey as a UNIX timestamp
        """
        pieces: CommandArgList = [
            key,
            subkey,
            normalized_time_seconds(unix_time_seconds),
        ]
        return await self.execute_command(CommandName.EXPIREMEMBERAT, *pieces)
