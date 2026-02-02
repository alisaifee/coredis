from __future__ import annotations

import warnings

from .patterns.cache import (
    AbstractCache,
    CacheStats,
    ClusterTrackingCache,
    LRUCache,
    NodeTrackingCache,
    TrackingCache,
)

warnings.warn(
    "coredis.patterns.cache has been moved to coredis.patterns.cache",
    DeprecationWarning,
    stacklevel=2,
)
__all__ = [
    "AbstractCache",
    "CacheStats",
    "LRUCache",
    "TrackingCache",
    "NodeTrackingCache",
    "ClusterTrackingCache",
]
