from __future__ import annotations

import warnings

from .patterns.pipeline import ClusterPipeline, Pipeline

warnings.warn(
    "coredis.pipeline has been moved to coredis.patterns.pipeline",
    DeprecationWarning,
    stacklevel=2,
)
__all__ = ["Pipeline", "ClusterPipeline"]
