from __future__ import annotations

import asyncio
from types import TracebackType

from typing_extensions import runtime_checkable

from coredis.typing import (
    Awaitable,
    Callable,
    KeyT,
    Optional,
    Parameters,
    Protocol,
    R,
    ResponseType,
    StringT,
    Tuple,
    Type,
    TypeVar,
    ValueT,
)

T_co = TypeVar("T_co", covariant=True)


class AbstractExecutor(Protocol):
    async def execute_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., R] = ...,
        **options: Optional[ValueT],
    ) -> R:
        ...


@runtime_checkable
class SupportsPipeline(Protocol):  # noqa
    async def pipeline(
        self,
        transaction: Optional[bool] = True,
        watches: Optional[Parameters[StringT]] = None,
    ) -> SupportsWatch:
        ...


@runtime_checkable
class SupportsScript(Protocol[T_co]):  # noqa
    async def evalsha(
        self,
        sha1: StringT,
        keys: Optional[Parameters[KeyT]] = ...,
        args: Optional[Parameters[ValueT]] = ...,
    ) -> ResponseType:
        ...

    async def evalsha_ro(
        self,
        sha1: StringT,
        keys: Optional[Parameters[KeyT]] = ...,
        args: Optional[Parameters[ValueT]] = ...,
    ) -> ResponseType:
        ...

    async def script_load(self, script: StringT) -> T_co:
        ...


@runtime_checkable
class SupportsWatch(Protocol):  # noqa
    async def __aenter__(self) -> SupportsWatch:
        ...

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Awaitable[Optional[bool]]:
        ...

    async def watch(self, *keys: KeyT) -> bool:
        ...

    async def execute(self, raise_on_error: bool = True) -> Tuple[object, ...]:
        ...


@runtime_checkable
class ConnectionP(Protocol):
    decode_responses: bool
    encoding: str
    push_messages: asyncio.Queue[ResponseType]
