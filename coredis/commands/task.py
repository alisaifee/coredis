from __future__ import annotations

import asyncio
from typing import Any, get_origin

from coredis._protocols import AbstractExecutor
from coredis.response._callbacks import ResponseCallback
from coredis.typing import (
    Callable,
    CustomInputT,
    ExecutionParameters,
    TypeAdapter,
    TypeVar,
    ValueT,
)

TaskResponse = TypeVar("TaskResponse", covariant=True)
TransformedResponse = TypeVar("TransformedResponse")

empty_adapter = TypeAdapter()


class CommandTask(asyncio.Task[TaskResponse]):
    def __init__(
        self,
        client: AbstractExecutor,
        name: bytes,
        *arguments: ValueT,
        callback: ResponseCallback[Any, Any, TaskResponse],
        execution_parameters: ExecutionParameters | None = None,
    ) -> None:
        self.client: AbstractExecutor = client
        self.name = name
        self.callback = callback
        self.execution_parameters = execution_parameters or {}
        self.arguments = tuple(
            self.type_adapter.to_redis_value(k) if isinstance(k, CustomInputT) else k
            for k in arguments
        )

        super().__init__(self.run())

    async def run(self) -> TaskResponse:
        return await self.client.execute_command(self, self.callback, **self.execution_parameters)

    async def transform(
        self, transformer: type[TransformedResponse] | Callable[[TaskResponse], TransformedResponse]
    ) -> TransformedResponse:
        if isinstance(transformer, type) or get_origin(transformer) is not None:
            return self.type_adapter.from_redis_value(await self, transformer)
        else:
            return transformer(await self)

    @property
    def type_adapter(self) -> TypeAdapter:
        from coredis.client import Client

        if isinstance(self.client, Client):
            return self.client.type_adapter or empty_adapter
        return empty_adapter
