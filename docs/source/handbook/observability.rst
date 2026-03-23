.. _handbook/observability:

Observability
==============

**coredis** can emit OpenTelemetry traces and metrics for Redis operations. This
integration is **opt-in** and uses the global OpenTelemetry tracer/meter
providers, so your application is responsible for configuring exporters/readers.

Installation
------------

Install the optional dependencies:

.. code-block:: bash

    pip install "coredis[otel]"

Enable instrumentation
----------------------

Enable OpenTelemetry emission by setting :envvar:`COREDIS_OTEL_ENABLED`:

.. code-block:: bash

    export COREDIS_OTEL_ENABLED=true

or by setting `coredis.Config.otel_enabled` to ``True``

Traces
------

When enabled, coredis emits:

- A span for a command execution: ``<CMD>``
- A single span for pipeline or transactions: ``MULTI`` (for transaction) and ``PIPELINE`` for non transactional
  batch operations.

Span attributes include the following recommended attributes:

- ``db.system``
- ``db.operation.name``
- ``db.operation.batch.size``
- ``db.query.text``
- ``network.peer.hostname``
- ``network.peer.port``
- ``server.address``
- ``server.port``
- ``db.client.connection.pool.name``

Metrics
-------

When enabled, the following metrics are emitted:

- ``db.client.request.duration`` (histogram, seconds)
- ``db.client.connection.pending_requests`` (counter)
- ``db.client.connection.connection.count`` (counter)
- ``db.client.connection.connection.max`` (counter)
- ``db.client.connection.connection.timeouts`` (counter)
- ``db.client.connection.connection.create_time`` (histogram, seconds)
- ``db.client.connection.connection.wait_time`` (histogram, seconds)
- ``db.client.connection.connection.use_time`` (histogram, seconds)

Configuration
-------------


- :envvar:`COREDIS_OTEL_CAPTURE_COMMAND_ARGS`: includes (sanitized) argument values in span attributes (default: false)
- :envvar:`COREDIS_OTEL_DISABLED_COMMANDS`: a comma separated list of commands to disable emitting spans for

