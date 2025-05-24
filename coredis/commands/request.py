from __future__ import annotations

from typing import Any

from coredis._protocols import AbstractExecutor
from coredis.typing import Awaitable, Callable, ExecutionParameters, Generator, TypeVar, ValueT

TaskResponse = TypeVar("TaskResponse", covariant=True)


class CommandRequest(Awaitable[TaskResponse]):
    response: Awaitable[TaskResponse]

    def __init__(
        self,
        client: AbstractExecutor,
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., TaskResponse],
        execution_parameters: ExecutionParameters | None = None,
    ) -> None:
        self.name = name
        self.callback = callback
        self.execution_parameters = execution_parameters or {}
        self.arguments = arguments
        self.client: AbstractExecutor = client

    def run(self) -> Awaitable[TaskResponse]:
        if not hasattr(self, "response"):
            self.response = self.client.execute_command(
                self, self.callback, **self.execution_parameters
            )
        return self.response

    def __await__(self) -> Generator[Any, Any, TaskResponse]:
        return self.run().__await__()
