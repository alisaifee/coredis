from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

from coredis.typing import Iterator

from ._base import TelemetryAttributeProvider, TelemetryProvider

if TYPE_CHECKING:
    from coredis.commands.request import CommandRequest
    from coredis.connection import BaseConnection
    from coredis.pool import BaseConnectionPool


class NoopTelemetryProvider(TelemetryProvider):
    def observe_connection(self, connection: BaseConnection) -> None:
        pass

    def observe_connection_pool(self, pool: BaseConnectionPool[Any]) -> None:
        pass

    def capture_connection_use_time(self, use_time: float, pool: BaseConnectionPool[Any]) -> None:
        pass

    @contextlib.contextmanager
    def capture_connection_create_time(self, pool: BaseConnectionPool[Any]) -> Iterator[None]:
        yield

    @contextlib.contextmanager
    def capture_connection_wait_time(self, pool: BaseConnectionPool[Any]) -> Iterator[None]:
        yield

    @contextlib.contextmanager
    def start_span(
        self,
        commands: tuple[CommandRequest[Any], ...],
        *attribute_providers: TelemetryAttributeProvider,
        name: str | None = None,
    ) -> Iterator[None]:
        yield

    def update_span_attributes(self, attribute_provider: TelemetryAttributeProvider) -> None:
        pass

    def emit_event(self, event: str, *, exception: Exception | None = None) -> None:
        pass
