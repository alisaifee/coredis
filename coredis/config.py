from __future__ import annotations

import os


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").lower() in ["1", "true", "t"]


class __Config:
    def __init__(self) -> None:
        self.__optimized: bool = False
        self.__otel_enabled: bool = False
        self.__otel_capture_command_args: bool = False
        self.__otel_capture_internal_commands: bool = False
        self.__otel_disabled_commands: list[bytes] = [
            b"PING",
        ]

    @property
    def runtime_checks(self) -> bool:
        """
        Whether runtime type checks are to be enabled.
        Can be enabled by setting the environment variable ``COREDIS_RUNTIME_CHECKS`` to ``true``
        """
        return _env_truthy("COREDIS_RUNTIME_CHECKS")

    @property
    def optimized(self) -> bool:
        """
        When ``optimized`` is ``True`` most runtime validations will be disabled.
        This can be enabled in any of the following ways:

          - By running python in optimized mode using the ``-O`` flag
          - By setting the environment variable ``COREDIS_OPTIMIZED`` to ``true``
          - By explicitly setting ``coredis.Config.optimized = True``

        """
        return not __debug__ or _env_truthy("COREDIS_OPTIMIZED") or self.__optimized

    @optimized.setter
    def optimized(self, value: bool) -> None:
        self.__optimized = value

    @property
    def otel_enabled(self) -> bool:
        """
        Whether OpenTelemetry instrumentation is enabled.

        This can be enabled by setting the environment variable ``COREDIS_OTEL_ENABLED`` to ``true``.
        """
        return _env_truthy("COREDIS_OTEL_ENABLED") or self.__otel_enabled

    @otel_enabled.setter
    def otel_enabled(self, value: bool) -> None:
        self.__otel_enabled = value

    @property
    def otel_capture_command_args(self) -> bool:
        """
        Whether span attributes will include command arguments.

        This can be enabled by setting the environment variable ``COREDIS_OTEL_CAPTURE_COMMAND_ARGS`` to ``true``.
        """
        return _env_truthy("COREDIS_OTEL_CAPTURE_COMMAND_ARGS") or self.__otel_capture_command_args

    @otel_capture_command_args.setter
    def otel_capture_command_args(self, value: bool) -> None:
        self.__otel_capture_command_args = value

    @property
    def otel_disabled_commands(self) -> list[bytes]:
        """
        The list of commands that are not included in reporting. By default
        internal commands such as HELLO & PING are not reported.

        This can be changed by setting the environment variable ``COREDIS_OTEL_DISABLED_COMMANDS`` to a comma
        separated list of command names
        """
        return (
            os.environ.get("COREDIS_OTEL_DISABLED_COMMANDS", "").encode("utf-8").split(b",")
            or self.__otel_disabled_commands
        )

    @otel_disabled_commands.setter
    def otel_disabled_commands(self, value: list[bytes]) -> None:
        self.__otel_disabled_commands = value


#: Used to configure global behaviors of the coredis library
Config = __Config()
