from __future__ import annotations

import asyncio

from typing_extensions import runtime_checkable

from coredis.response._callbacks import NoopCallback
from coredis.typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    ExecutionParameters,
    KeyT,
    Parameters,
    Protocol,
    R,
    RedisCommandP,
    RedisValueT,
    ResponseType,
    StringT,
    TypeVar,
    Unpack,
    ValueT,
)

T_co = TypeVar("T_co", covariant=True)


if TYPE_CHECKING:
    from coredis.commands import CommandRequest


class AbstractExecutor(Protocol):
    def execute_command(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **options: Unpack[ExecutionParameters],
    ) -> Awaitable[R]: ...

    def create_request(
        self,
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., R],
        execution_parameters: ExecutionParameters | None = None,
    ) -> CommandRequest[R]: ...


@runtime_checkable
class SupportsScript(Protocol[T_co]):  # noqa
    async def evalsha(
        self,
        sha1: StringT,
        keys: Parameters[KeyT] | None = ...,
        args: Parameters[RedisValueT] | None = ...,
    ) -> ResponseType: ...

    async def evalsha_ro(
        self,
        sha1: StringT,
        keys: Parameters[KeyT] | None = ...,
        args: Parameters[RedisValueT] | None = ...,
    ) -> ResponseType: ...

    async def script_load(self, script: StringT) -> T_co: ...


@runtime_checkable
class ConnectionP(Protocol):
    decode_responses: bool
    encoding: str
    push_messages: asyncio.Queue[ResponseType]
