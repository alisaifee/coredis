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
    Awaitable,
    Callable,
    ExecutionParameters,
    Generic,
    R,
    RedisCommandP,
    Unpack,
    ValueT,
)

# Command wrappers
from .bitfield import BitFieldOperation
from .function import Function, Library
from .pubsub import ClusterPubSub, PubSub, ShardedPubSub
from .request import CommandRequest, CommandResponseT
from .script import Script


class CommandMixin(Generic[AnyStr], ABC):
    @abstractmethod
    def execute_command(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **options: Unpack[ExecutionParameters],
    ) -> Awaitable[R]: ...

    @abstractmethod
    def create_request(
        self,
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., R],
        execution_parameters: ExecutionParameters | None = None,
    ) -> CommandRequest[R]: ...


__all__ = [
    "CommandRequest",
    "CommandResponseT",
    "BitFieldOperation",
    "ClusterPubSub",
    "Function",
    "Library",
    "PubSub",
    "Script",
    "ShardedPubSub",
]
