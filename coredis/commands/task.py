from __future__ import annotations

import asyncio
from typing import Any

from coredis._protocols import AbstractExecutor
from coredis.response._callbacks import ResponseCallback
from coredis.typing import Callable, ExecutionParameters, TypeVar, ValueT

TaskResponse = TypeVar("TaskResponse", covariant=True)
TransformedResponse = TypeVar("TransformedResponse")


class CommandTask(asyncio.Task[TaskResponse]):
    def __init__(
        self,
        client: AbstractExecutor,
        name: bytes,
        *arguments: ValueT,
        callback: ResponseCallback[Any, Any, TaskResponse],
        execution_parameters: ExecutionParameters | None = None,
    ) -> None:
        self.name = name
        self.callback = callback
        self.execution_parameters = execution_parameters or {}
        self.arguments = arguments
        self.client: AbstractExecutor = client

        super().__init__(self.run())

    async def run(self) -> TaskResponse:
        return await self.client.execute_command(self, self.callback, **self.execution_parameters)

    async def transform(
        self, transformer: Callable[[TaskResponse], TransformedResponse]
    ) -> TransformedResponse:
        return transformer(await self)
