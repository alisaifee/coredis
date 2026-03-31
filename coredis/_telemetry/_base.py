from __future__ import annotations

import contextlib
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from coredis._utils import nativestr
from coredis.tokens import PrefixToken, PureToken
from coredis.typing import Iterator, Key

if TYPE_CHECKING:
    from coredis.commands.request import CommandRequest
    from coredis.connection import BaseConnection
    from coredis.pool import BaseConnectionPool


class TelemetryAttributeProvider(ABC):
    @abstractmethod
    def telemetry_attributes(
        self, telemetry_provider: TelemetryProvider
    ) -> dict[str, str | int]: ...


class TelemetryProvider(ABC):
    @abstractmethod
    def observe_connection(self, connection: BaseConnection) -> None: ...

    @abstractmethod
    def observe_connection_pool(self, pool: BaseConnectionPool[Any]) -> None: ...

    @abstractmethod
    def capture_connection_use_time(
        self, use_time: float, pool: BaseConnectionPool[Any]
    ) -> None: ...

    @abstractmethod
    @contextlib.contextmanager
    def capture_connection_create_time(self, pool: BaseConnectionPool[Any]) -> Iterator[None]: ...

    @abstractmethod
    @contextlib.contextmanager
    def capture_connection_wait_time(self, pool: BaseConnectionPool[Any]) -> Iterator[None]: ...

    @abstractmethod
    @contextlib.contextmanager
    def start_span(
        self,
        commands: tuple[CommandRequest[Any], ...],
        *attribute_providers: TelemetryAttributeProvider,
        name: str | None = None,
    ) -> Iterator[None]: ...

    @abstractmethod
    def update_span_attributes(self, attribute_provider: TelemetryAttributeProvider) -> None: ...

    @abstractmethod
    def emit_event(self, event: str, *, exception: Exception | None = None) -> None: ...

    def command_summary(self, command: CommandRequest[Any]) -> str:
        sanitized_args = []
        for idx, arg in enumerate(command.arguments):
            match arg:
                case Key():
                    sanitized_args.append(nativestr(arg.key))
                case PureToken() | PrefixToken():
                    sanitized_args.append(nativestr(arg))
                case _:
                    sanitized_args.append("?")
        return f"{nativestr(command.name)} {' '.join(sanitized_args)}".strip()
