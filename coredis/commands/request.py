from __future__ import annotations

from typing import Any

from coredis._protocols import AbstractExecutor
from coredis.typing import Awaitable, Callable, ExecutionParameters, Generator, TypeVar, ValueT

#: Covariant type used for generalizing :class:`~coredis.command.CommandRequest`
CommandResponseT = TypeVar("CommandResponseT", covariant=True)


class CommandRequest(Awaitable[CommandResponseT]):
    response: Awaitable[CommandResponseT]

    def __init__(
        self,
        client: AbstractExecutor,
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., CommandResponseT],
        execution_parameters: ExecutionParameters | None = None,
    ) -> None:
        """
        The default command request object which is returned by all
        methods mirroring redis commands.

        :param client: The instance of the :class:`coredis.Redis` that
         will be used to call :meth:`~coredis.Redis.execute_command`
        :param name:  The name of the command
        :param arguments:  All arguments (in redis format) to be passed to the command
        :param callback: The callback to be used to transform the RESP response
        :param execution_parameters:  Any additional parameters to be passed to
         :meth:`coredis.Redis.execute_command`
        """
        self.name = name
        self.callback = callback
        self.execution_parameters = execution_parameters or {}
        self.arguments = arguments
        self.client: AbstractExecutor = client

    def run(self) -> Awaitable[CommandResponseT]:
        if not hasattr(self, "response"):
            self.response = self.client.execute_command(
                self, self.callback, **self.execution_parameters
            )
        return self.response

    def __await__(self) -> Generator[Any, Any, CommandResponseT]:
        return self.run().__await__()
