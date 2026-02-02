from __future__ import annotations

import warnings

from ..patterns.pubsub import ClusterPubSub, PubSub, ShardedPubSub, SubscriptionCallback

warnings.warn(
    "coredis.commands.pubsub has been moved to coredis.patterns.pubsub",
    DeprecationWarning,
    stacklevel=2,
)
__all__ = ["PubSub", "ClusterPubSub", "ShardedPubSub", "SubscriptionCallback"]
