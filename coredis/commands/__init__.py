"""
coredis.commands
----------------
Implementation of core redis commands and abstractions over high level
core concepts such as pubsub, scripting and functions.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from coredis.response._callbacks import NoopCallback
from coredis.typing import AnyStr, Callable, Generic, Optional, R, ValueT

# Command wrappers
from .bitfield import BitFieldOperation
from .function import Function, Library
from .monitor import Monitor
from .pubsub import ClusterPubSub, PubSub, ShardedPubSub
from .script import Script


class CommandMixin(Generic[AnyStr], ABC):
    @abstractmethod
    async def execute_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., R] = NoopCallback(),
        **options: Optional[ValueT],
    ) -> R:
        pass


__all__ = [
    "BitFieldOperation",
    "ClusterPubSub",
    "Function",
    "Library",
    "Monitor",
    "PubSub",
    "Script",
    "ShardedPubSub",
]
