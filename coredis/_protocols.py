from __future__ import annotations

import asyncio
from types import TracebackType

from typing_extensions import runtime_checkable

from coredis.response._callbacks import NoopCallback
from coredis.typing import (
    Awaitable,
    Callable,
    ExecutionParameters,
    KeyT,
    Parameters,
    Protocol,
    R,
    RedisValueT,
    ResponseType,
    StringT,
    TypeVar,
    Unpack,
)

T_co = TypeVar("T_co", covariant=True)


class RedisCommandP(Protocol):
    name: bytes
    arguments: tuple[RedisValueT, ...]


class AbstractExecutor(Protocol):
    async def execute_command(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **options: Unpack[ExecutionParameters],
    ) -> R: ...


@runtime_checkable
class SupportsPipeline(Protocol):  # noqa
    async def pipeline(
        self,
        transaction: bool | None = True,
        watches: Parameters[StringT] | None = None,
    ) -> SupportsWatch: ...


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
class SupportsWatch(Protocol):  # noqa
    async def __aenter__(self) -> SupportsWatch: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Awaitable[bool | None]: ...

    async def watch(self, *keys: KeyT) -> bool: ...

    async def execute(self, raise_on_error: bool = True) -> tuple[object, ...]: ...


@runtime_checkable
class ConnectionP(Protocol):
    decode_responses: bool
    encoding: str
    push_messages: asyncio.Queue[ResponseType]
