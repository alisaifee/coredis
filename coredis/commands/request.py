from __future__ import annotations

import copy
import functools
from types import GenericAlias
from typing import Any, cast, get_origin

from coredis._protocols import AbstractExecutor
from coredis.retry import RetryPolicy
from coredis.typing import (
    Awaitable,
    Callable,
    ExecutionParameters,
    Generator,
    Serializable,
    TypeAdapter,
    TypeIs,
    TypeVar,
    ValueT,
)

#: Covariant type used for generalizing :class:`~coredis.command.CommandRequest`
CommandResponseT = TypeVar("CommandResponseT", covariant=True)

TransformedResponse = TypeVar("TransformedResponse")
empty_adapter = TypeAdapter()


def is_type_like(obj: object) -> TypeIs[type[Any]]:
    """
    Return True if ``obj`` is type-like and should be treated as a
    deserialization target rather than a callable transformer.
    """
    return isinstance(obj, type) or isinstance(obj, GenericAlias) or get_origin(obj) is not None


class CommandRequest(Awaitable[CommandResponseT]):
    response: Awaitable[CommandResponseT]

    def __init__(
        self,
        client: AbstractExecutor,
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., CommandResponseT],
        execution_parameters: ExecutionParameters | None = None,
        **kwargs: Any,
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
        self.kwargs = kwargs

    def run(self) -> Awaitable[CommandResponseT]:
        if not hasattr(self, "response"):
            self.response = self.client.execute_command(
                self, self.callback, **self.execution_parameters
            )

        return self.response

    def retry(
        self,
        policy: RetryPolicy,
        failure_hook: Callable[..., Awaitable[Any]] | None = None,
    ) -> CommandRequest[CommandResponseT]:
        """

        :param policy: Retry policy to use
        :param failure_hook: Callable[..., Awaitable[Any]]
                      | dict[type[BaseException], Callable[..., Awaitable[None]]]
                      | None = None,
        :return: A retryable version of the command object

        Calling ``retry`` is essentially the same as explicitly using
        a retry policy when calling a redis command. For example the following two
        examples that try to push to a list upto 2 times if a :exc:`~coredis.exceptions.RedisError`
        is encountered, are equivalent.

        With :meth:`retry`::

            async with coredis.Redis() as client:
                await client.lpush("mylist", [1,2,3]).retryable(
                    coredis.retry.ConstantRetryPolicy(
                        (coredis.exceptions.RedisError,), 2, 1
                    ),
                )

        Explicitly retrying::

            retry_policy = coredis.retry.ConstantRetryPolicy(
                (coredis.exceptions.RedisError,), 2, 1
            )

            async with coredis.Redis() as client:
                await retry_policy.call_with_retries(lambda:client.lpush("mylist", [1,2,3])

        """
        return RetryableCommandRequest(
            self.client,
            self.name,
            *self.arguments,
            callback=self.callback,
            execution_parameters=self.execution_parameters,
            policy=policy,
            failure_hook=failure_hook,
        )

    def transform(
        self,
        transformer: type[TransformedResponse] | Callable[[CommandResponseT], TransformedResponse],
    ) -> CommandRequest[TransformedResponse]:
        """
        :param transformer: A type that was registered with the client
         using :meth:`~coredis.typing.TypeAdapter.register_deserializer`
         or decorated by :meth:`~coredis.typing.TypeAdapter.deserializer`
         or a callable that takes a single argument (the original response)
         and returns the transformed response.

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
           float_value: float = await client.get("fubar").transform(lambda value: float(value))
        """

        transform_func = (
            functools.partial(self.type_adapter.deserialize, return_type=transformer)
            if is_type_like(transformer)
            else transformer
        )

        return cast(type[CommandRequest[TransformedResponse]], self.__class__)(
            self.client,
            self.name,
            *self.arguments,
            callback=lambda resp, **kwargs: transform_func(self.callback(resp, **kwargs)),
            execution_parameters=self.execution_parameters,
            **self.kwargs,
        )

    @property
    def type_adapter(self) -> TypeAdapter:
        from coredis.client import Client

        if isinstance(self.client, Client):
            return self.client.type_adapter

        return empty_adapter

    def __await__(self) -> Generator[Any, Any, CommandResponseT]:
        return self.run().__await__()


class RetryableCommandRequest(CommandRequest[CommandResponseT]):
    def __init__(
        self,
        client: AbstractExecutor,
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., CommandResponseT],
        execution_parameters: ExecutionParameters | None = None,
        policy: RetryPolicy,
        failure_hook: Callable[..., Awaitable[Any]]
        | dict[type[BaseException], Callable[..., Awaitable[None]]]
        | None = None,
        **kwargs: Any,
    ) -> None:
        """
        A retryable command request object.

        :param client: The instance of the :class:`coredis.Redis` that
        will be used to call :meth:`~coredis.Redis.execute_command`
        :param name:  The name of the command
        :param arguments:  All arguments (in redis format) to be passed to the command
        :param callback: The callback to be used to transform the RESP response
        :param execution_parameters:  Any additional parameters to be passed to
        :meth:`coredis.Redis.execute_command`
        :param policy: The retry policy to use when executing the command
        :param failure_hook: if provided and is a callable it will be
         called everytime a retryable exception is encountered. If it is a mapping
         of exception types to callables, the first exception type that is a parent
         of any encountered exception will be called.
        """
        self.policy = policy
        self.failure_hook = failure_hook
        super().__init__(
            client,
            name,
            *arguments,
            callback=callback,
            execution_parameters=execution_parameters,
            policy=self.policy,
            failure_hook=self.failure_hook,
            **kwargs,
        )

    def __await__(self) -> Generator[Any, Any, CommandResponseT]:
        return self.policy.call_with_retries(
            lambda: copy.copy(self).run(), failure_hook=self.failure_hook
        ).__await__()
