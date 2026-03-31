Observability
==============

**coredis** supports emitting telemetry traces and metrics through OpenTelemetry.

Installation
------------

Install the optional dependencies:

.. code-block:: bash

    pip install "coredis[otel]"

Enable instrumentation
----------------------

Enable OpenTelemetry instrumentation by setting :envvar:`COREDIS_OTEL_ENABLED`:

.. code-block:: bash

    export COREDIS_OTEL_ENABLED=true

or by setting ``coredis.Config.otel_enabled`` to ``True``

Example
-------

The example below configures in-memory exporters for both traces and metrics and
then executes a command so you can inspect emitted telemetry immediately.

.. code-block:: python

    import anyio
    import coredis
    from opentelemetry import metrics, trace
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    async def main() -> None:
        coredis.Config.otel_enabled = True
        coredis.Config.otel_capture_command_args = True

        span_exporter = InMemorySpanExporter()
        tracer_provider = TracerProvider()
        tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))
        trace.set_tracer_provider(tracer_provider)

        metric_reader = InMemoryMetricReader()
        metrics.set_meter_provider(MeterProvider(metric_readers=[metric_reader]))

        client = coredis.Redis(host="127.0.0.1", port=6379)
        async with client:
            await client.set("example:key", "value")
            await client.get("example:key")

        spans = span_exporter.get_finished_spans()
        metric_data = metric_reader.get_metrics_data()

        print(f"spans={len(spans)} metric_resources={len(metric_data.resource_metrics)}")

    anyio.run(main)



Traces
------

When enabled, coredis emits:

- A span for a single command execution: ``${COMMAND NAME}``
- A single aggregate span for a pipeline: ``PIPELINE``
- A single aggregate span for a transaction pipeline: ``MULTI``

Span attributes include the following recommended attributes:

``db.system.name``
  Always set to ``redis``
``db.namespace``
  The database index number associated with the connection used when performing the operation
``db.operation.name``
  Either the command name for a single command execution, or ``MULTI`` for transactions and ``PIPELINE`` for non transactional pipelines.

  Examples:

  .. code-block:: text

     SET
     GET

``db.operation.batch.size``
  The number of commands in a transaction or pipeline
``db.query.text``
  The full redis :term:`RESP` command equivalent with all user input values masked (key names are still shown)

  .. note:: If the span is created for a transaction or pipeline this attribute will contain all the commands
     in the batch.

  Examples:

  .. code-block:: text

     SET key1 ?
     GET key1


``network.peer.hostname``
  Address of the redis node where the operation was performed
``network.peer.port``
  Port of the redis node
``server.address``
  Address of the redis node where the operation was performed
``server.port``
  Port of the redis node where the operation was performed
``db.client.connection.pool.name``
  Name of the connection pool associated with the operation

Metrics
-------

When enabled, the following metrics are emitted:

``db.client.operation.duration``
  Histogram (seconds)
``db.client.connection.pending_requests``
  Up/Down Counter
``db.client.connection.count``
  Up/Down Counter
``db.client.connection.max``
  Up/Down Counter
``db.client.connection.timeouts``
  Counter
``db.client.connection.create_time``
  Histogram (seconds)
``db.client.connection.wait_time``
  Histogram (seconds)
``db.client.connection.use_time``
  Histogram (seconds)


Span events
-----------

In addition to span attributes, coredis emits span events for retry attempts.

``retry``
  Emitted when a command execution is retried by the configured retry policy.
  This event includes the following attributes:

  - ``exception.type``: Fully-qualified exception type where available.
  - ``exception.message``: Exception message if present.

Additionally, any uncaught exceptions raised while executing a command
will be emitted as standard exception span events.


Troubleshooting
---------------

- If telemetry is enabled but no data appears, verify OpenTelemetry SDK
  providers are configured in your application before issuing redis commands.
- If OpenTelemetry dependencies are missing, coredis falls back to a noop
  telemetry provider and emits a warning.

Configuration
-------------

:envvar:`COREDIS_OTEL_ENABLED` or ``coredis.Config.otel_enabled``
  Whether telemetry is enabled or not.

:envvar:`COREDIS_OTEL_CAPTURE_COMMAND_ARGS` or ``coredis.Config.otel_capture_command_args``
  If set to ``True`` the argument values for each reported command will be included in the span attributes under
  ``db.query.text``. Any user provided values other than keys  will be replaced with ``?``.

:envvar:`COREDIS_OTEL_DISABLED_COMMANDS` or ``coredis.Config.otel_disabled_commands``
  A list of command names to disable emitting spans for. When using an environment variable,
  provide a comma-separated list (for example: ``PING,GET,SET``). Command matching is case-insensitive.

.. note:: The following strings (case insensitive) are accepted as truthy values when using
   environment variables: ``true``, ``t``, ``1``
