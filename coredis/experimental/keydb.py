from __future__ import annotations

import datetime
import functools
import textwrap

from packaging import version

from coredis._utils import CaseAndEncodingInsensitiveEnum
from coredis.client import Redis
from coredis.commands._utils import check_version, normalized_time_seconds
from coredis.commands._wrappers import ClusterCommandConfig, CommandDetails
from coredis.commands.constants import CommandGroup
from coredis.response._callbacks import BoolCallback
from coredis.typing import (
    AnyStr,
    Awaitable,
    Callable,
    CommandArgList,
    Dict,
    KeyT,
    Literal,
    Optional,
    P,
    R,
    Union,
)


def _keydb_command_link(command: CommandName) -> str:
    canonical_command = str(command).lower().replace(" ", "-")
    return (
        f"`{str(command)} <https://docs.keydb.dev/docs/commands#{canonical_command}>`_"
    )


class CommandName(CaseAndEncodingInsensitiveEnum):
    """
    Enum for listing all redis commands
    """

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
    cluster: ClusterCommandConfig = ClusterCommandConfig(),
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    command_details = CommandDetails(
        command_name,
        group,
        readonly,
        version.Version(version_introduced) if version_introduced else None,
        version.Version(version_deprecated) if version_deprecated else None,
        arguments or {},
        cluster or ClusterCommandConfig(),
    )

    def wrapper(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
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
        CommandName.EXPIREMEMBER,
        CommandGroup.GENERIC,
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
        return await self.execute_command(
            CommandName.EXPIREMEMBER, *pieces, callback=BoolCallback()
        )

    @keydb_command(
        CommandName.EXPIREMEMBERAT,
        CommandGroup.GENERIC,
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
        return await self.execute_command(
            CommandName.EXPIREMEMBERAT, *pieces, callback=BoolCallback()
        )
