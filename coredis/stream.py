from __future__ import annotations

import warnings

from coredis.patterns.streams import (
    Consumer,
    GroupConsumer,
    StreamParameters,
)

warnings.warn(
    "coredis.stream has been moved to coredis.patterns.streams",
    DeprecationWarning,
    stacklevel=2,
)
__all__ = ["Consumer", "GroupConsumer", "StreamParameters"]
