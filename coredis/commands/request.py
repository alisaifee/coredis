from __future__ import annotations

import functools
from typing import Any, cast

from coredis._protocols import AbstractExecutor
from coredis.typing import (
    Awaitable,
    Callable,
    ExecutionParameters,
    Generator,
    Serializable,
    TypeAdapter,
    TypeVar,
    ValueT,
)

#: Covariant type used for generalizing :class:`~coredis.command.CommandRequest`
CommandResponseT = TypeVar("CommandResponseT", covariant=True)

TransformedResponse = TypeVar("TransformedResponse")
empty_adapter = TypeAdapter()


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
        self.client: AbstractExecutor = client
        self.arguments = tuple(
            self.type_adapter.serialize(k) if isinstance(k, Serializable) else k for k in arguments
        )

    def run(self) -> Awaitable[CommandResponseT]:
        if not hasattr(self, "response"):
            self.response = self.client.execute_command(
                self, self.callback, **self.execution_parameters
            )

        return self.response

    def transform(
        self, transformer: type[TransformedResponse]
    ) -> CommandRequest[TransformedResponse]:
        """
        :param transformer: A type that was registered with the client
         using :meth:`~coredis.typing.TypeAdapter.register_deserializer`
         or decorated by :meth:`~coredis.typing.TypeAdapter.deserializer`

        :return: a command request object that when awaited will return the
         transformed response

         For example when used with a redis command::

           client = coredis.Redis(....)
           @client.type_adapter.deserializer
           def _(value: bytes) -> int:
               return int(value)

           await client.set("fubar", 1)
           raw: bytes = await client.get("fubar")
           int_value: int = await client.get("fubar").transform(int)
        """
        transform_func = functools.partial(
            self.type_adapter.deserialize,
            return_type=transformer,
        )
        return cast(type[CommandRequest[TransformedResponse]], self.__class__)(
            self.client,
            self.name,
            *self.arguments,
            callback=lambda resp, **kwargs: transform_func(self.callback(resp, **kwargs)),
            execution_parameters=self.execution_parameters,
        )

    @property
    def type_adapter(self) -> TypeAdapter:
        from coredis.client import Client

        if isinstance(self.client, Client):
            return self.client.type_adapter

        return empty_adapter

    def __await__(self) -> Generator[Any, Any, CommandResponseT]:
        return self.run().__await__()
