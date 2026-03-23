from __future__ import annotations

import contextlib
import functools
import time
import weakref
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from coredis._utils import nativestr
from coredis.config import Config
from coredis.typing import ClassVar, Iterable, Iterator

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
from opentelemetry.trace import SpanKind, Status, StatusCode, Tracer


class TelemetryAttributeProvider(ABC):
    @property
    @abstractmethod
    def telemetry_attributes(self) -> dict[str, str | int]: ...


class OtelTelemetry:
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
            "db.client.connection.pending_requests",
            callbacks=[self._pending_requests_callback],
        )
        self.pending_requests_observer = self.meter.create_observable_up_down_counter(
            "db.client.connection.count", callbacks=[self._connection_count_callback]
        )
        self.pending_requests_observer = self.meter.create_observable_up_down_counter(
            "db.client.connection.max", callbacks=[self._connection_max_callback]
        )

    @property
    @functools.cache
    def enabled(self) -> bool:
        return Config.otel_enabled

    @property
    @functools.cache
    def disabled_commands(self) -> list[bytes]:
        return Config.otel_disabled_commands

    @property
    @functools.cache
    def operation_duration(self) -> Histogram:
        return self.meter.create_histogram(
            "db.client.operation.duration",
            unit="s",
        )

    @property
    @functools.cache
    def connection_create_time(self) -> Histogram:
        return self.meter.create_histogram("db.client.connection.create_time", unit="s")

    @property
    @functools.cache
    def connection_use_time(self) -> Histogram:
        return self.meter.create_histogram("db.client.connection.use_time", unit="s")

    @property
    @functools.cache
    def connection_wait_time(self) -> Histogram:
        return self.meter.create_histogram("db.client.connection.wait_time", unit="s")

    @property
    @functools.cache
    def connection_timeouts(self) -> Counter:
        return self.meter.create_counter("db.client.connection.timeouts")

    def observe_connection(self, connection: BaseConnection) -> None:
        if self.enabled:
            self.observed_connections.add(connection)

    def observe_connection_pool(self, pool: BaseConnectionPool[Any]) -> None:
        if self.enabled:
            self.observed_pools.add(pool)

    def capture_connection_use_time(self, use_time: float, pool: BaseConnectionPool[Any]) -> None:
        if self.enabled:
            self.connection_use_time.record(use_time, self._default_attributes((), pool))

    @contextlib.contextmanager
    def capture_connection_create_time(self, pool: BaseConnectionPool[Any]) -> Iterator[None]:
        start = time.perf_counter()
        try:
            yield
        finally:
            if self.enabled:
                self.connection_create_time.record(
                    time.perf_counter() - start, self._default_attributes((), pool)
                )

    @contextlib.contextmanager
    def capture_connection_wait_time(self, pool: BaseConnectionPool[Any]) -> Iterator[None]:
        start = time.perf_counter()
        try:
            yield
        except TimeoutError:
            if self.enabled:
                self.connection_timeouts.add(1, self._default_attributes((), pool))
            raise
        finally:
            if self.enabled:
                self.connection_wait_time.record(
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
            name = name or self._generate_summary(commands)

            if not name:
                raise ValueError("Unable to generate a span name")
            attributes = self._default_attributes(commands, *attribute_providers)
            with self.tracer.start_as_current_span(
                name, kind=SpanKind.CLIENT, attributes=attributes
            ) as span:
                try:
                    start = time.perf_counter()
                    yield
                    with contextlib.suppress(Exception):
                        span.set_status(Status(StatusCode.OK))
                except Exception as exc:
                    with contextlib.suppress(Exception):
                        span.record_exception(exc)
                        span.set_status(Status(StatusCode.ERROR))
                    raise
                finally:
                    self.operation_duration.record(time.perf_counter() - start, attributes)
        else:
            yield

    def update_span_attributes(self, attribute_provider: TelemetryAttributeProvider) -> None:
        if self.enabled:
            span = trace.get_current_span()
            if span.is_recording():
                for key, value in attribute_provider.telemetry_attributes.items():
                    span.set_attribute(key, value)

    def _generate_summary(self, commands: tuple[CommandRequest[Any], ...]) -> str:
        if len(commands) == 1:
            return nativestr(commands[0].name)
        return ""

    def _default_attributes(
        self,
        commands: tuple[CommandRequest[Any], ...],
        *attribute_providers: TelemetryAttributeProvider,
    ) -> dict[str, str | int]:
        attributes: dict[str, str | int] = {
            "db.system": "redis",
        }
        if len(commands) == 1:
            attributes["db.operation.name"] = nativestr(commands[0].name)
            if Config.otel_capture_command_args:
                attributes["db.query.text"] = commands[0].summary
        elif len(commands) > 0:
            attributes["db.operation.batch.size"] = len(commands)
            attributes["db.operation.name"] = ",".join(
                [nativestr(command.name) for command in commands]
            )
            if Config.otel_capture_command_args:
                attributes["db.query.text"] = "\n".join(command.summary for command in commands)

        for provider in attribute_providers:
            attributes.update(provider.telemetry_attributes)

        return attributes

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
                {**_default_attributes, **{"db.client.connection.state": "used"}},
            )
            yield Observation(
                pool.statistics.active_connections - pool.statistics.in_use_connections,
                {**_default_attributes, **{"db.client.connection.state": "idle"}},
            )

    def _connection_max_callback(self, _: CallbackOptions) -> Iterable[Observation]:
        for pool in self.observed_pools:
            _default_attributes = self._default_attributes((), pool)
            yield Observation(pool.max_connections, _default_attributes)

    def _command_span_enabled(
        self,
        commands: tuple[CommandRequest[Any], ...],
    ) -> bool:
        if not self.enabled:
            return False
        if len(commands) == 1 and commands[0].name in self.disabled_commands:
            return False
        return True
