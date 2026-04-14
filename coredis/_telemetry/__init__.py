from __future__ import annotations

import warnings

from coredis.config import Config

from ._base import TelemetryAttributeProvider, TelemetryProvider
from ._noop import NoopTelemetryProvider

_noop_provider = NoopTelemetryProvider()
_otel_provider = None
_otel_unavailable = False


def get_telemetry_provider() -> TelemetryProvider:
    global _noop_provider, _otel_unavailable, _otel_provider
    if _otel_unavailable or not Config.otel_enabled:
        return _noop_provider
    try:
        if not _otel_provider:
            from ._otel import OpenTelemetryProvider

            _otel_provider = OpenTelemetryProvider()
        return _otel_provider
    except ImportError:
        warnings.warn(
            "OpenTelemetry support was configured but could not be enabled due to missing dependencies",
            stacklevel=2,
        )
        _otel_unavailable = True
        return _noop_provider


__all__ = ["TelemetryAttributeProvider", "TelemetryProvider", "get_telemetry_provider"]
