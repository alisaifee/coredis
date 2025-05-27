"""
coredis.commands
----------------
Implementation of core redis commands and abstractions over high level
core concepts such as pubsub, scripting and functions.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from coredis.response._callbacks import NoopCallback
from coredis.typing import (
    AnyStr,
    Callable,
    ExecutionParameters,
    Generic,
    R,
    RedisCommandP,
    T_co,
    Unpack,
    ValueT,
)

# Command wrappers
from .bitfield import BitFieldOperation
from .function import Function, Library
from .monitor import Monitor
from .pubsub import ClusterPubSub, PubSub, ShardedPubSub
from .request import CommandRequest, CommandResponseT
from .script import Script


class CommandMixin(Generic[AnyStr], ABC):
    @abstractmethod
    async def execute_command(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **options: Unpack[ExecutionParameters],
    ) -> R:
        pass

    @abstractmethod
    def create_request(
        self,
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., T_co],
        execution_parameters: ExecutionParameters | None = None,
    ) -> CommandRequest[T_co]: ...


__all__ = [
    "CommandRequest",
    "CommandResponseT",
    "BitFieldOperation",
    "ClusterPubSub",
    "Function",
    "Library",
    "Monitor",
    "PubSub",
    "Script",
    "ShardedPubSub",
]
