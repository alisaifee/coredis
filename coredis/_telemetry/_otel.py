from __future__ import annotations

import contextlib
import functools
import time
import weakref
from typing import TYPE_CHECKING, Any

from coredis._utils import nativestr
from coredis.commands.constants import CommandName
from coredis.config import Config
from coredis.typing import ClassVar, Iterable, Iterator

from ._base import TelemetryAttributeProvider, TelemetryProvider

if TYPE_CHECKING:
    from coredis.commands.request import CommandRequest
    from coredis.connection import BaseConnection
    from coredis.pool import BaseConnectionPool

from opentelemetry import metrics, trace
from opentelemetry.metrics import (
    CallbackOptions,
    Counter,
    Histogram,
    Instrument,
    Meter,
    Observation,
)
from opentelemetry.semconv._incubating.attributes.db_attributes import (
    DB_CLIENT_CONNECTION_STATE,
)
from opentelemetry.semconv._incubating.metrics.db_metrics import (
    DB_CLIENT_CONNECTION_COUNT,
    DB_CLIENT_CONNECTION_CREATE_TIME,
    DB_CLIENT_CONNECTION_MAX,
    DB_CLIENT_CONNECTION_PENDING_REQUESTS,
    DB_CLIENT_CONNECTION_TIMEOUTS,
    DB_CLIENT_CONNECTION_USE_TIME,
    DB_CLIENT_CONNECTION_WAIT_TIME,
)
from opentelemetry.semconv.attributes.db_attributes import (
    DB_OPERATION_BATCH_SIZE,
    DB_OPERATION_NAME,
    DB_QUERY_TEXT,
    DB_STORED_PROCEDURE_NAME,
    DB_SYSTEM_NAME,
)
from opentelemetry.semconv.metrics.db_metrics import DB_CLIENT_OPERATION_DURATION
from opentelemetry.trace import SpanKind, Status, StatusCode, Tracer


class OpenTelemetryProvider(TelemetryProvider):
    meter: Meter
    tracer: Tracer
    warned_missing_deps: ClassVar[bool] = False

    def __init__(self) -> None:
        self.meter = metrics.get_meter("coredis")
        self.tracer = trace.get_tracer("coredis")
        self.instruments: dict[str, Instrument] = {}
        self.observed_connections: weakref.WeakSet[BaseConnection] = weakref.WeakSet()
        self.observed_pools: weakref.WeakSet[BaseConnectionPool[Any]] = weakref.WeakSet()
        self.pending_requests_observer = self.meter.create_observable_up_down_counter(
            DB_CLIENT_CONNECTION_PENDING_REQUESTS,
            callbacks=[self._pending_requests_callback],
        )
        self.client_connection_count_observer = self.meter.create_observable_up_down_counter(
            DB_CLIENT_CONNECTION_COUNT, callbacks=[self._connection_count_callback]
        )
        self.client_connection_max_observer = self.meter.create_observable_up_down_counter(
            DB_CLIENT_CONNECTION_MAX, callbacks=[self._connection_max_callback]
        )

    @property
    def disabled_commands(self) -> list[bytes]:
        return Config.otel_disabled_commands

    def observe_connection(self, connection: BaseConnection) -> None:
        self.observed_connections.add(connection)

    def observe_connection_pool(self, pool: BaseConnectionPool[Any]) -> None:
        self.observed_pools.add(pool)

    def capture_connection_use_time(self, use_time: float, pool: BaseConnectionPool[Any]) -> None:
        self._connection_use_time.record(use_time, self._default_attributes((), pool))

    @contextlib.contextmanager
    def capture_connection_create_time(self, pool: BaseConnectionPool[Any]) -> Iterator[None]:
        start = time.perf_counter()
        try:
            yield
        finally:
            self._connection_create_time.record(
                time.perf_counter() - start, self._default_attributes((), pool)
            )

    @contextlib.contextmanager
    def capture_connection_wait_time(self, pool: BaseConnectionPool[Any]) -> Iterator[None]:
        start = time.perf_counter()
        try:
            yield
        except TimeoutError:
            self._connection_timeouts.add(1, self._default_attributes((), pool))
            raise
        finally:
            self._connection_wait_time.record(
                time.perf_counter() - start, self._default_attributes((), pool)
            )

    @contextlib.contextmanager
    def start_span(
        self,
        commands: tuple[CommandRequest[Any], ...],
        *attribute_providers: TelemetryAttributeProvider,
        name: str | None = None,
    ) -> Iterator[None]:
        if self._command_span_enabled(commands):
            name = name or (nativestr(commands[0].name) if len(commands) == 1 else None)
            assert name
            attributes = self._default_attributes(commands, *attribute_providers)
            with self.tracer.start_as_current_span(
                name, kind=SpanKind.CLIENT, attributes=attributes
            ) as span:
                try:
                    start = time.perf_counter()
                    yield
                    with contextlib.suppress(Exception):
                        span.set_status(Status(StatusCode.OK))
                finally:
                    self._operation_duration.record(time.perf_counter() - start, attributes)
        else:
            yield

    def update_span_attributes(self, attribute_provider: TelemetryAttributeProvider) -> None:
        span = trace.get_current_span()
        if span.is_recording():
            for key, value in attribute_provider.telemetry_attributes(self).items():
                span.set_attribute(key, value)

    def emit_event(self, event: str, *, exception: Exception | None = None) -> None:
        span = trace.get_current_span()
        if span.is_recording():
            attributes = {}
            if exception:
                module = type(exception).__module__
                qualname = type(exception).__qualname__
                exception_type = (
                    f"{module}.{qualname}" if module and module != "builtins" else qualname
                )
                attributes["exception.type"] = exception_type
                if msg := str(exception):
                    attributes["exception.message"] = msg
            span.add_event(event, attributes)

    def _default_attributes(
        self,
        commands: tuple[CommandRequest[Any], ...],
        *attribute_providers: TelemetryAttributeProvider,
    ) -> dict[str, str | int]:
        attributes: dict[str, str | int] = {
            DB_SYSTEM_NAME: "redis",
        }
        if len(commands) == 1:
            attributes[DB_OPERATION_NAME] = nativestr(commands[0].name)
            if commands[0].name in {
                CommandName.EVALSHA,
                CommandName.EVALSHA_RO,
                CommandName.FCALL,
                CommandName.FCALL_RO,
            }:
                attributes[DB_STORED_PROCEDURE_NAME] = nativestr(
                    commands[0].serialized_arguments[0]
                )
            if Config.otel_capture_command_args:
                attributes[DB_QUERY_TEXT] = self.command_summary(commands[0])
        elif len(commands) > 0:
            attributes[DB_OPERATION_BATCH_SIZE] = len(commands)
            attributes[DB_OPERATION_NAME] = ",".join(
                [nativestr(command.name) for command in commands]
            )
            if Config.otel_capture_command_args:
                attributes[DB_QUERY_TEXT] = "\n".join(
                    self.command_summary(command) for command in commands
                )

        for provider in attribute_providers:
            attributes.update(provider.telemetry_attributes(self))

        return attributes

    @property
    @functools.cache
    def _operation_duration(self) -> Histogram:
        return self.meter.create_histogram(
            DB_CLIENT_OPERATION_DURATION,
            unit="s",
        )

    @property
    @functools.cache
    def _connection_create_time(self) -> Histogram:
        return self.meter.create_histogram(DB_CLIENT_CONNECTION_CREATE_TIME, unit="s")

    @property
    @functools.cache
    def _connection_use_time(self) -> Histogram:
        return self.meter.create_histogram(DB_CLIENT_CONNECTION_USE_TIME, unit="s")

    @property
    @functools.cache
    def _connection_wait_time(self) -> Histogram:
        return self.meter.create_histogram(DB_CLIENT_CONNECTION_WAIT_TIME, unit="s")

    @property
    @functools.cache
    def _connection_timeouts(self) -> Counter:
        return self.meter.create_counter(DB_CLIENT_CONNECTION_TIMEOUTS)

    def _pending_requests_callback(self, _: CallbackOptions) -> Iterable[Observation]:
        for connection in self.observed_connections:
            yield Observation(
                connection.statistics.requests_pending,
                self._default_attributes((), connection),
            )

    def _connection_count_callback(self, _: CallbackOptions) -> Iterable[Observation]:
        for pool in self.observed_pools:
            _default_attributes = self._default_attributes((), pool)
            yield Observation(
                pool.statistics.in_use_connections,
                {**_default_attributes, **{DB_CLIENT_CONNECTION_STATE: "used"}},
            )
            yield Observation(
                pool.statistics.active_connections - pool.statistics.in_use_connections,
                {**_default_attributes, **{DB_CLIENT_CONNECTION_STATE: "idle"}},
            )

    def _connection_max_callback(self, _: CallbackOptions) -> Iterable[Observation]:
        for pool in self.observed_pools:
            _default_attributes = self._default_attributes((), pool)
            yield Observation(pool.max_connections, _default_attributes)

    def _command_span_enabled(
        self,
        commands: tuple[CommandRequest[Any], ...],
    ) -> bool:
        if len(commands) == 1 and commands[0].name in self.disabled_commands:
            return False
        return True
