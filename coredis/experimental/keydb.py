from __future__ import annotations

import datetime
import functools
import textwrap
from typing import Any

from packaging import version

from coredis._utils import CaseAndEncodingInsensitiveEnum
from coredis.client import Redis, RedisCluster
from coredis.commands import CommandMixin
from coredis.commands._utils import (
    check_version,
    normalized_milliseconds,
    normalized_time_milliseconds,
    normalized_time_seconds,
)
from coredis.commands._wrappers import ClusterCommandConfig, CommandDetails
from coredis.commands.constants import CommandGroup
from coredis.response._callbacks import (
    BoolCallback,
    BoolsCallback,
    IntCallback,
    SimpleStringCallback,
)
from coredis.typing import (
    AnyStr,
    Callable,
    CommandArgList,
    Coroutine,
    Dict,
    Iterable,
    KeyT,
    Literal,
    Optional,
    P,
    Parameters,
    R,
    StringT,
    Tuple,
    Union,
    ValueT,
)


def _keydb_command_link(command: CommandName) -> str:
    canonical_command = str(command).lower().replace(" ", "-").replace(".", "")
    return (
        f"`{str(command)} <https://docs.keydb.dev/docs/commands#{canonical_command}>`_"
    )


class CommandName(CaseAndEncodingInsensitiveEnum):
    """
    Enum for listing all keydb extension commands
    """

    BITOP = b"BITOP"
    CRON = b"KEYDB.CRON"
    EXPIREMEMBER = b"EXPIREMEMBER"
    EXPIREMEMBERAT = b"EXPIREMEMBERAT"
    PEXPIREMEMBERAT = b"PEXPIREMEMBERAT"
    HRENAME = b"KEYDB.HRENAME"
    MEXISTS = b"KEYDB.MEXISTS"
    OBJECT_LASTMODIFIED = b"OBJECT LASTMODIFIED"
    PTTL = b"PTTL"
    TTL = b"TTL"


def keydb_command(
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
        None,
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

KeyDB command documentation: {_keydb_command_link(command_name)}
"""

        setattr(wrapped, "__coredis_command", command_details)
        return wrapped

    return wrapper


class KeyDBCommands(CommandMixin[AnyStr]):
    @keydb_command(
        CommandName.BITOP,
        CommandGroup.BITMAP,
    )
    async def bitop(
        self,
        keys: Parameters[KeyT],
        operation: StringT,
        destkey: KeyT,
        value: Optional[int] = None,
    ) -> int:
        """
        Perform a bitwise operation using :paramref:`operation` between
        :paramref:`keys` and store the result in :paramref:`destkey`.
        """
        pieces: CommandArgList = [operation, destkey, *keys]
        if value is not None:
            pieces.append(value)
        return await self.execute_command(
            CommandName.BITOP, *pieces, callback=IntCallback()
        )

    @keydb_command(
        CommandName.CRON,
        CommandGroup.SCRIPTING,
    )
    async def cron(
        self,
        name: KeyT,
        repeat: bool,
        delay: Union[int, datetime.timedelta],
        script: StringT,
        keys: Parameters[KeyT],
        args: Parameters[ValueT],
        start: Optional[Union[int, datetime.datetime]] = None,
    ) -> bool:
        """
        Schedule a LUA script to run at a specified time and/or intervals.
        To cancel the cron delete the key at :paramref:`name`

        :param name: Name of the cron which will be visible in the keyspace,
         can be searched, and deleted with DEL.
        :param repeat: If the script will run only once, or if it will be repeated
         at the specified interval provided by :paramref:`delay`
        :param delay: is an integer specified in milliseconds used as the initial delay.
         If :paramref:`repeat` is ``True``, this will also be the length of the repeating timer
         which will execute the script each time the delay elapses
         (will continue to execute indefinitely).
        :param start: unix time specified as milliseconds enforcing that the script
         should only start executing then this Unix time has been reached.
         If :paramref:`delay` is greater than zero, this delay time will need to elapse prior to the
         script executing (timer begins to elapse at start time).
         If a start time is specified, the delay will always remain in reference
         intervals to that start time.
        :param script: is the body of the LUA script to execute.
        :param keys: The keys expected by the script
        :param args: The args required by the script
        """
        pieces: CommandArgList = [name]
        if repeat:
            pieces.append(b"REPEAT")
        else:
            pieces.append(b"SINGLE")
        if start is not None:
            pieces.append(normalized_time_milliseconds(start))
        pieces.append(normalized_milliseconds(delay))
        pieces.append(script)
        _keys = list(keys)
        pieces.append(len(_keys))
        pieces.extend(keys)
        pieces.extend(args)

        return await self.execute_command(
            CommandName.CRON, *pieces, callback=SimpleStringCallback()
        )

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

    @keydb_command(
        CommandName.PEXPIREMEMBERAT,
        CommandGroup.GENERIC,
    )
    async def pexpirememberat(
        self,
        key: KeyT,
        subkey: KeyT,
        unix_time_milliseconds: Union[int, datetime.datetime],
    ) -> bool:
        """
        Set the expiration for a subkey as a UNIX timestamp in milliseconds
        """
        pieces: CommandArgList = [
            key,
            subkey,
            normalized_time_milliseconds(unix_time_milliseconds),
        ]
        return await self.execute_command(
            CommandName.PEXPIREMEMBERAT, *pieces, callback=BoolCallback()
        )

    @keydb_command(CommandName.HRENAME, group=CommandGroup.HASH)
    async def hrename(
        self, key: KeyT, source_field: ValueT, destination_field: ValueT
    ) -> bool:
        """
        Rename a field :paramref:`source_field` to :paramref:`destination_field`
        in hash :paramref:`key`
        """

        return await self.execute_command(
            CommandName.HRENAME,
            key,
            source_field,
            destination_field,
            callback=BoolCallback(),
        )

    @keydb_command(CommandName.MEXISTS, readonly=True, group=CommandGroup.GENERIC)
    async def mexists(self, keys: Iterable[KeyT]) -> Tuple[bool, ...]:
        """
        Returns a tuple of bools in the same order as :paramref:`keys`
        denoting whether the keys exist
        """

        return await self.execute_command(
            CommandName.MEXISTS, *keys, callback=BoolsCallback()
        )

    @keydb_command(
        CommandName.OBJECT_LASTMODIFIED, readonly=True, group=CommandGroup.GENERIC
    )
    async def object_lastmodified(self, key: KeyT) -> int:
        """
        Returns the time elapsed (in seconds) since the key was last modified.
        This differs from idletime as it is not affected by reads of a key.

        :return: The time in seconds since the last modification
        """

        return await self.execute_command(
            CommandName.OBJECT_LASTMODIFIED, key, callback=IntCallback()
        )

    @keydb_command(CommandName.PTTL, readonly=True, group=CommandGroup.GENERIC)
    async def pttl(self, key: KeyT, subkey: Optional[ValueT] = None) -> int:
        """
        Returns the number of milliseconds until the key :paramref:`key` will expire.
        If :paramref:`subkey` is provided the response will be for the subkey.

        :return: TTL in milliseconds, or a negative value in order to signal an error
        """
        pieces: CommandArgList = [key]
        if subkey is not None:
            pieces.append(subkey)

        return await self.execute_command(
            CommandName.PTTL, *pieces, callback=IntCallback()
        )

    @keydb_command(CommandName.TTL, readonly=True, group=CommandGroup.GENERIC)
    async def ttl(self, key: KeyT, subkey: Optional[ValueT] = None) -> int:
        """
        Get the time to live for a key (or subkey) in seconds

        :return: TTL in seconds, or a negative value in order to signal an error
        """

        pieces: CommandArgList = [key]
        if subkey is not None:
            pieces.append(subkey)
        return await self.execute_command(
            CommandName.TTL, *pieces, callback=IntCallback()
        )


class KeyDB(KeyDBCommands[AnyStr], Redis[AnyStr]):
    """
    Client for `KeyDB <https://keydb.dev>`__

    The client is mostly :class:`coredis.Redis` with a couple of extra
    commands specific to KeyDB.
    """


class KeyDBCluster(KeyDBCommands[AnyStr], RedisCluster[AnyStr]):
    """
    Cluster client for `KeyDB <https://keydb.dev>`__

    The client is mostly :class:`coredis.RedisCluster` with a couple of extra
    commands specific to KeyDB.
    """
