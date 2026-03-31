from __future__ import annotations

import pytest

import coredis
from coredis._telemetry._noop import NoopTelemetryProvider

try:
    import opentelemetry  # noqa

    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False

OTEL_REQUESTED = coredis.Config.otel_enabled

pytestmark = [pytest.mark.otel]


@pytest.mark.skipif(OTEL_REQUESTED, reason="OpenTelemetry enabled")
def test_noop_provider_by_default():
    assert isinstance(coredis._telemetry.get_telemetry_provider(), NoopTelemetryProvider)


@pytest.mark.skipif(not (OTEL_REQUESTED and OTEL_AVAILABLE), reason="OpenTelemetry not enabled")
def test_otel_provider_configured():
    from coredis._telemetry._otel import OpenTelemetryProvider

    assert isinstance(coredis._telemetry.get_telemetry_provider(), OpenTelemetryProvider)
