from __future__ import annotations

import datetime
import time
import warnings
from typing import TYPE_CHECKING, Any

from packaging import version

from coredis.commands.constants import CommandName
from coredis.exceptions import CommandNotSupportedError
from coredis.typing import Optional, Union

if TYPE_CHECKING:
    import coredis.client


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


def check_version(
    instance: coredis.client.Client[Any],
    command: bytes,
    function_name: str,
    min_version: Optional[version.Version],
    deprecated_version: Optional[version.Version],
    deprecation_reason: Optional[str],
) -> None:
    if not any([min_version, deprecated_version]):
        return

    if getattr(instance, "verify_version", False):
        server_version = getattr(instance, "server_version", None)
        if not server_version:
            return
        if min_version and server_version < min_version:
            raise CommandNotSupportedError(
                command.decode("latin-1"), str(instance.server_version)
            )
        if deprecated_version and server_version >= deprecated_version:
            warnings.warn(
                deprecation_reason.strip()
                if deprecation_reason
                else f"{function_name}() is deprecated since redis version {deprecated_version}.",
                category=DeprecationWarning,
                stacklevel=3,
            )


def redis_command_link(command: CommandName) -> str:
    return f'`{str(command)} <https://redis.io/commands/{str(command).lower().replace(" ", "-")}>`_'
