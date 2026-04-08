from __future__ import annotations

from coredis.typing import (
    TYPE_CHECKING,
    Awaitable,
    Callable,
    ExecutionParameters,
    Key,
    Protocol,
    R,
    TypeVar,
    ValueT,
)

T_co = TypeVar("T_co", covariant=True)


if TYPE_CHECKING:
    from coredis.commands import CommandRequest


class AbstractExecutor(Protocol):
    def execute_command(
        self,
        command: CommandRequest[R],
    ) -> Awaitable[R]: ...

    def create_request(
        self,
        name: bytes,
        *arguments: ValueT | Key,
        callback: Callable[..., R],
        execution_parameters: ExecutionParameters | None = None,
    ) -> CommandRequest[R]: ...


class CommandResolver(Protocol):
    def __call__(self, command: CommandRequest[R]) -> Awaitable[R]: ...
