"""
coredis.commands
----------------
Implementation of core redis commands and abstractions over high level
core concepts such as pubsub, scripting and functions.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from coredis.typing import (
    AnyStr,
    Awaitable,
    Callable,
    ExecutionParameters,
    Generic,
    Key,
    R,
    ValueT,
)

# Command wrappers
from .bitfield import BitFieldOperation
from .function import Function, Library
from .request import CommandRequest, CommandResponseT
from .script import Script


class CommandMixin(Generic[AnyStr], ABC):
    @abstractmethod
    def execute_command(
        self,
        command: CommandRequest[R],
    ) -> Awaitable[R]: ...

    @abstractmethod
    def create_request(
        self,
        name: bytes,
        *arguments: ValueT | Key,
        callback: Callable[..., R],
        execution_parameters: ExecutionParameters | None = None,
    ) -> CommandRequest[R]: ...


__all__ = [
    "CommandRequest",
    "CommandResponseT",
    "BitFieldOperation",
    "Function",
    "Library",
    "Script",
]
