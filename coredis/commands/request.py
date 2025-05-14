from __future__ import annotations

from typing import Any

from coredis._protocols import AbstractExecutor
from coredis.typing import Awaitable, Callable, ExecutionParameters, Generator, TypeVar, ValueT

CommandResponse = TypeVar("CommandResponse", covariant=True)


class CommandRequest(Awaitable[CommandResponse]):
    response: Awaitable[CommandResponse]

    def __init__(
        self,
        client: AbstractExecutor,
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., CommandResponse],
        execution_parameters: ExecutionParameters | None = None,
    ) -> None:
        self.name = name
        self.callback = callback
        self.execution_parameters = execution_parameters or {}
        self.arguments = arguments
        self.client: AbstractExecutor = client

    def run(self) -> Awaitable[CommandResponse]:
        if not hasattr(self, "response"):
            self.response = self.client.execute_command(
                self, self.callback, **self.execution_parameters
            )
        return self.response

    def __await__(self) -> Generator[Any, Any, CommandResponse]:
        return self.run().__await__()
