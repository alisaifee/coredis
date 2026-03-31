from __future__ import annotations

import contextlib
from unittest.mock import PropertyMock

import anyio
import opentelemetry
import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import StatusCode, TracerProvider, export
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

import coredis
from tests.conftest import targets

pytestmark = [
    pytest.mark.skipif(not coredis.Config.otel_enabled, reason="OpenTelemetry not enabled"),
    pytest.mark.otel,
]


class Capture(anyio.AsyncContextManagerMixin):
    def __init__(self, exporter, metric_reader):
        self.exporter = exporter
        self.metric_reader = metric_reader
        self._data_points = {}

    async def _collect(self):
        while True:
            await anyio.sleep(1e-3)
            self._drain()

    def _drain(self):
        if metric_data := self.metric_reader.get_metrics_data():
            for resource_metric in metric_data.resource_metrics:
                for scope_metrics in resource_metric.scope_metrics:
                    for metric in scope_metrics.metrics:
                        self._data_points.setdefault(metric.name, []).extend(
                            metric.data.data_points
                        )

    @contextlib.asynccontextmanager
    async def __asynccontextmanager__(self):
        async with anyio.create_task_group() as tg:
            self.clear()
            tg.start_soon(self._collect)
            yield self
            tg.cancel_scope.cancel()

    @property
    def metrics(self):
        self._drain()
        return self._data_points

    @property
    def spans(self):
        self.exporter.force_flush()
        return self.exporter.get_finished_spans()

    def clear(self):
        self.exporter.clear()
        self.metric_reader.get_metrics_data()


@pytest.fixture(scope="session")
def memory_exporter():
    return InMemorySpanExporter()


@pytest.fixture(scope="session")
def memory_metric_reader():
    return InMemoryMetricReader()


@pytest.fixture(autouse=True, scope="session")
def tracer(memory_exporter):
    span_processor = export.SimpleSpanProcessor(memory_exporter)
    provider = TracerProvider()
    provider.add_span_processor(span_processor)
    opentelemetry.trace.set_tracer_provider(provider)
    return opentelemetry.trace.get_tracer("coredis")


@pytest.fixture(autouse=True, scope="session")
def meter(memory_metric_reader):
    provider = MeterProvider(metric_readers=[memory_metric_reader])
    opentelemetry.metrics.set_meter_provider(provider)
    return provider


@pytest.fixture
async def telemetry_capture(memory_exporter, memory_metric_reader, tracer, meter):
    async with Capture(memory_exporter, memory_metric_reader) as cap:
        yield cap


class TestConfiguration:
    async def test_excluded_commands(self, redis_basic, telemetry_capture, mocker):
        mocker.patch(
            "coredis.config.__Config.otel_disabled_commands",
            new_callable=PropertyMock,
            return_value=[b"GET"],
        )
        await redis_basic.get("fubar")
        await redis_basic.set("fubar", 1)
        assert len(telemetry_capture.spans) == 1
        assert telemetry_capture.spans[0].name == "SET"

    async def test_command_attributes(self, redis_basic, telemetry_capture, mocker):
        await redis_basic.get("fubar")
        assert len(telemetry_capture.spans) == 1
        assert telemetry_capture.spans[0].name == "GET"
        assert not telemetry_capture.spans[0].attributes.get("db.query.text")

        mocker.patch(
            "coredis.config.__Config.otel_capture_command_args",
            new_callable=PropertyMock,
            return_value=True,
        )
        await redis_basic.get("fubar")
        assert telemetry_capture.spans[1].attributes.get("db.query.text") == "GET fubar"


class TestSpanAttributes:
    @pytest.fixture(autouse=True)
    def setup(self, mocker):
        mocker.patch(
            "coredis.config.__Config.otel_capture_command_args",
            new_callable=PropertyMock,
            return_value=True,
        )

    @pytest.mark.parametrize(
        "method, args, kwargs, name, query_text",
        [
            ("ping", (), {}, "PING", "PING"),
            ("get", ("fubar",), {}, "GET", "GET fubar"),
            ("set", ("fubar", 1), {}, "SET", "SET fubar ?"),
            ("set", ("fubar", 1), {"condition": coredis.PureToken.NX}, "SET", "SET fubar ? NX"),
            ("set", ("fubar", 1), {"ex": 1}, "SET", "SET fubar ? EX ?"),
        ],
    )
    async def test_query_text(
        self, redis_basic, telemetry_capture, method, args, kwargs, name, query_text
    ):
        await getattr(redis_basic, method)(*args, **kwargs)
        assert telemetry_capture.spans[0].name == name
        assert telemetry_capture.spans[0].attributes == {
            "db.system.name": "redis",
            "network.peer.hostname": "localhost",
            "network.peer.port": 6379,
            "server.address": "localhost",
            "server.port": 6379,
            "db.namespace": 0,
            "db.operation.name": name,
            "db.query.text": query_text,
        }

    async def test_stored_procedures(self, redis_basic, telemetry_capture):
        sha = await redis_basic.script_load("return 1")
        await redis_basic.function_load(
            """#!lua name=mylib

redis.register_function{function_name='echo', callback=function(k, a)
    return a[1]
end, flags={'no-writes'}}
""",
            replace=True,
        )

        await redis_basic.evalsha(sha)
        await redis_basic.evalsha_ro(sha)
        await redis_basic.fcall("echo", [], [1])
        await redis_basic.fcall_ro("echo", [], [1])

        procedure_spans = telemetry_capture.spans[2:]
        assert procedure_spans[0].attributes["db.stored_procedure.name"] == sha
        assert procedure_spans[1].attributes["db.stored_procedure.name"] == sha
        assert procedure_spans[2].attributes["db.stored_procedure.name"] == "echo"
        assert procedure_spans[3].attributes["db.stored_procedure.name"] == "echo"

    @pytest.mark.parametrize(
        "transaction, name, query_text",
        [
            (
                False,
                "PIPELINE",
                "fubar",
            ),
            (
                True,
                "MULTI",
                "fubar",
            ),
        ],
    )
    async def test_pipeline(self, redis_basic, telemetry_capture, transaction, name, query_text):
        async with redis_basic.pipeline(transaction=transaction) as pipeline:
            pipeline.set("fubar", 1)
            pipeline.get("fubar")
            pipeline.delete(["fubar"])

        assert len(telemetry_capture.spans) == 1
        assert telemetry_capture.spans[0].name == name
        assert telemetry_capture.spans[0].attributes["db.operation.name"] == "SET,GET,DEL"
        assert (
            telemetry_capture.spans[0].attributes["db.query.text"]
            == """SET fubar ?
GET fubar
DEL fubar"""
        )

    async def test_error(self, redis_basic, telemetry_capture):
        await redis_basic.set("fubar", 1)
        with pytest.raises(coredis.exceptions.RedisError):
            await redis_basic.lpush("fubar", [1, 2, 3])

        assert len(telemetry_capture.spans[1].events) == 1
        assert telemetry_capture.spans[1].status.status_code == StatusCode.ERROR
        assert telemetry_capture.spans[1].events[0].name == "exception"
        assert (
            telemetry_capture.spans[1].events[0].attributes["exception.type"]
            == "coredis.exceptions.WrongTypeError"
        )

    @pytest.mark.parametrize(
        "client_arguments",
        [
            {
                "retry_policy": coredis.retry.ConstantRetryPolicy(
                    (coredis.exceptions.WrongTypeError,), retries=2, delay=0.1
                )
            }
        ],
    )
    async def test_retry(self, redis_basic, telemetry_capture, client_arguments):
        await redis_basic.set("fubar", 1)
        with pytest.raises(coredis.exceptions.RedisError):
            await redis_basic.lpush("fubar", [1, 2, 3])

        assert len(telemetry_capture.spans[1].events) == 3
        assert telemetry_capture.spans[1].status.status_code == StatusCode.ERROR
        assert telemetry_capture.spans[1].events[0].name == "retry"
        assert (
            telemetry_capture.spans[1].events[0].attributes["exception.type"]
            == "coredis.exceptions.WrongTypeError"
        )
        assert telemetry_capture.spans[1].events[1].name == "retry"
        assert (
            telemetry_capture.spans[1].events[1].attributes["exception.type"]
            == "coredis.exceptions.WrongTypeError"
        )
        assert telemetry_capture.spans[1].events[2].name == "exception"
        assert (
            telemetry_capture.spans[1].events[2].attributes["exception.type"]
            == "coredis.exceptions.WrongTypeError"
        )


class TestMetrics:
    @pytest.fixture(autouse=True)
    def setup(self, mocker):
        pass

    @targets("redis_basic", "redis_cluster")
    async def test_basic_connection_metrics(self, telemetry_capture, client, cloner):
        async def get():
            await client.get("fubar")

        async with anyio.create_task_group() as tg:
            [tg.start_soon(get) for _ in range(100)]

        metrics = telemetry_capture.metrics
        max_pending = max(value.value for value in metrics["db.client.connection.pending_requests"])
        max_wait = max(value.sum for value in metrics["db.client.connection.wait_time"])
        max_idle = max(
            [
                value.value
                for value in metrics["db.client.connection.count"]
                if value.attributes["db.client.connection.state"] == "idle"
            ]
        )
        max_used = max(
            [
                value.value
                for value in metrics["db.client.connection.count"]
                if value.attributes["db.client.connection.state"] == "used"
            ]
        )

        assert max_pending > 10
        assert max_wait > 0
        assert max_idle > 0
        assert max_used == 0

    @pytest.mark.parametrize(
        "client_arguments",
        [{"max_connections": 1, "pool_timeout": 2}],
    )
    async def test_blocking_connection_metrics(
        self, telemetry_capture, redis_basic, cloner, client_arguments
    ):
        async def blocking():
            try:
                await redis_basic.blpop(["test"], 1)
            except TimeoutError:
                pass

        async with anyio.create_task_group() as tg:
            tg.start_soon(blocking)
            tg.start_soon(blocking)
            tg.start_soon(blocking)

        metrics = telemetry_capture.metrics

        max_wait = max(value.sum for value in metrics["db.client.connection.wait_time"])
        max_use = max(value.sum for value in metrics["db.client.connection.use_time"])
        max_create = max(value.sum for value in metrics["db.client.connection.create_time"])

        timeouts = max([value.value for value in metrics["db.client.connection.timeouts"]])
        max_idle = max(
            [
                value.value
                for value in metrics["db.client.connection.count"]
                if value.attributes["db.client.connection.state"] == "idle"
            ]
        )
        max_used = max(
            [
                value.value
                for value in metrics["db.client.connection.count"]
                if value.attributes["db.client.connection.state"] == "used"
            ]
        )

        assert max_wait >= 1
        assert max_use >= 1
        assert max_create >= 0
        assert timeouts == 1
        assert max_idle > 0
        assert max_used >= 0

    @pytest.mark.parametrize(
        "client_arguments",
        [{"max_connections": 1, "max_connections_per_node": True, "pool_timeout": 2}],
    )
    async def test_blocking_connection_metrics_cluster(
        self, telemetry_capture, redis_cluster, cloner, client_arguments
    ):
        async def blocking():
            try:
                await redis_cluster.blpop(["test"], 1)
            except TimeoutError:
                pass

        async with anyio.create_task_group() as tg:
            tg.start_soon(blocking)
            tg.start_soon(blocking)
            tg.start_soon(blocking)
        metrics = telemetry_capture.metrics
        max_wait = max(value.sum for value in metrics["db.client.connection.wait_time"])
        max_use = max(value.sum for value in metrics["db.client.connection.use_time"])
        max_create = max(value.sum for value in metrics["db.client.connection.create_time"])

        timeouts = max([value.value for value in metrics["db.client.connection.timeouts"]])
        max_idle = max(
            [
                value.value
                for value in metrics["db.client.connection.count"]
                if value.attributes["db.client.connection.state"] == "idle"
            ]
        )
        max_used = max(
            [
                value.value
                for value in metrics["db.client.connection.count"]
                if value.attributes["db.client.connection.state"] == "used"
            ]
        )

        assert max_wait >= 1
        assert max_use >= 1
        assert max_create >= 0
        assert timeouts == 1
        assert max_idle > 0
        assert max_used >= 0
