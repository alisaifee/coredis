from __future__ import annotations

import datetime
import time
import warnings
from typing import TYPE_CHECKING, Any

from coredis.commands.constants import CommandName
from coredis.config import Config
from coredis.exceptions import CommandNotSupportedError, CommandSyntaxError
from coredis.typing import Dict, Optional, Union

if TYPE_CHECKING:
    import coredis.client
    from coredis.commands._wrappers import CommandDetails


def normalized_seconds(value: Union[int, datetime.timedelta]) -> int:
    if isinstance(value, datetime.timedelta):
        value = value.seconds + value.days * 24 * 3600

    return value


def normalized_milliseconds(value: Union[int, datetime.timedelta]) -> int:
    if isinstance(value, datetime.timedelta):
        ms = int(value.microseconds / 1000)
        value = (value.seconds + value.days * 24 * 3600) * 1000 + ms

    return value


def normalized_time_seconds(value: Union[int, datetime.datetime]) -> int:
    if isinstance(value, datetime.datetime):
        s = int(value.microsecond / 1000000)
        value = int(time.mktime(value.timetuple())) + s

    return value


def normalized_time_milliseconds(value: Union[int, datetime.datetime]) -> int:
    if isinstance(value, datetime.datetime):
        ms = int(value.microsecond / 1000)
        value = int(time.mktime(value.timetuple())) * 1000 + ms

    return value


async def check_version(
    instance: coredis.client.Client[Any],
    function_name: str,
    command_details: "CommandDetails",
    deprecation_reason: Optional[str] = None,
    kwargs: Dict[str, Any] = {},
) -> None:
    if Config.optimized or not any(
        [
            command_details.version_introduced,
            command_details.version_deprecated,
            command_details.arguments,
        ]
    ):
        return
    if getattr(instance, "verify_version", False) and not getattr(
        instance, "noreply", False
    ):
        server_version = getattr(instance, "server_version", None)
        if not server_version:
            return
        if (
            command_details.version_introduced
            and server_version < command_details.version_introduced
        ):
            raise CommandNotSupportedError(
                command_details.command.decode("latin-1"),
                str(instance.server_version),
            )
        elif command_details.arguments and set(
            command_details.arguments.keys()
        ).intersection(kwargs.keys()):
            for argument, minimum_version in command_details.arguments.items():
                if minimum_version and server_version < minimum_version:
                    raise CommandSyntaxError(
                        {argument},
                        (
                            f"{command_details.command.decode('latin-1')} with `{argument}` "
                            f"is not supported in redis version {server_version}"
                        ),
                    )
        elif (
            command_details.version_deprecated
            and server_version >= command_details.version_deprecated
        ):
            warnings.warn(
                deprecation_reason.strip()
                if deprecation_reason
                else (
                    f"{function_name}() is deprecated since redis version "
                    "{command_details.version_deprecated}."
                ),
                category=DeprecationWarning,
                stacklevel=3,
            )


def redis_command_link(command: CommandName) -> str:
    return f'`{str(command)} <https://redis.io/commands/{str(command).lower().replace(" ", "-")}>`_'
