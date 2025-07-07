from __future__ import annotations

import asyncio
import functools
import inspect
import sys
import textwrap
import warnings
from abc import ABCMeta
from concurrent.futures import CancelledError
from types import TracebackType
from typing import Any, cast

from deprecated.sphinx import deprecated

from coredis._utils import b, hash_slot, nativestr
from coredis.client import Client, RedisCluster
from coredis.commands import CommandRequest, CommandResponseT
from coredis.commands._key_spec import KeySpec
from coredis.commands.constants import CommandName, NodeFlag
from coredis.commands.script import Script
from coredis.connection import BaseConnection, ClusterConnection, CommandInvocation, Connection
from coredis.exceptions import (
    AskError,
    ClusterCrossSlotError,
    ClusterDownError,
    ClusterTransactionError,
    ConnectionError,
    ExecAbortError,
    MovedError,
    RedisClusterException,
    RedisError,
    ResponseError,
    TimeoutError,
    TryAgainError,
    WatchError,
)
from coredis.pool import ClusterConnectionPool, ConnectionPool
from coredis.pool.nodemanager import ManagedNode
from coredis.response._callbacks import (
    AnyStrCallback,
    AsyncPreProcessingCallback,
    BoolCallback,
    BoolsCallback,
    NoopCallback,
    SimpleStringCallback,
)
from coredis.retry import ConstantRetryPolicy, retryable
from coredis.typing import (
    AnyStr,
    Awaitable,
    Callable,
    ExecutionParameters,
    Generator,
    Iterable,
    KeyT,
    Parameters,
    ParamSpec,
    RedisCommand,
    RedisCommandP,
    RedisValueT,
    ResponseType,
    Self,
    StringT,
    T_co,
    TypeVar,
    Unpack,
    ValueT,
)

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

ERRORS_ALLOW_RETRY = (
    MovedError,
    AskError,
    TryAgainError,
)

UNWATCH_COMMANDS = {CommandName.DISCARD, CommandName.EXEC, CommandName.UNWATCH}


def wrap_pipeline_method(
    kls: PipelineMeta, func: Callable[P, Awaitable[R]]
) -> Callable[P, Awaitable[R]]:
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Awaitable[R]:
        return func(*args, **kwargs)

    wrapper.__annotations__ = wrapper.__annotations__.copy()
    wrapper.__annotations__["return"] = kls
    wrapper.__doc__ = textwrap.dedent(wrapper.__doc__ or "")
    wrapper.__doc__ = f"""
Pipeline variant of :meth:`coredis.Redis.{func.__name__}` that does not execute
immediately and instead pushes the command into a stack for batch send.

The return value can be retrieved either as part of the tuple returned by
:meth:`~{kls.__name__}.execute` or by awaiting the :class:`~coredis.commands.CommandRequest`
instance after calling :meth:`~{kls.__name__}.execute`

{wrapper.__doc__}
"""
    return wrapper


class PipelineCommandRequest(CommandRequest[CommandResponseT]):
    client: Pipeline[Any] | ClusterPipeline[Any]
    queued_response: Awaitable[bytes | str]

    def __init__(
        self,
        client: Pipeline[Any] | ClusterPipeline[Any],
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., CommandResponseT],
        execution_parameters: ExecutionParameters | None = None,
    ) -> None:
        super().__init__(
            client, name, *arguments, callback=callback, execution_parameters=execution_parameters
        )
        if (client.watching or name == CommandName.WATCH) and not client.explicit_transaction:
            self.response = client.immediate_execute_command(
                self, callback=callback, **self.execution_parameters
            )
        else:
            client.pipeline_execute_command(self)  # type: ignore[arg-type]

    async def __backward_compatibility_return(self) -> Pipeline[Any] | ClusterPipeline[Any]:
        return self.client

    def __await__(self) -> Generator[None, None, CommandResponseT]:
        if hasattr(self, "response"):
            return self.response.__await__()
        else:
            warnings.warn(
                """
Awaiting a pipeline command response before calling `execute()` on the pipeline instance 
has no effect and returns the pipeline instance itself for backward compatibility.

To add commands to a pipeline simply call the methods synchronously. The awaitable response
can be awaited after calling `execute()` to retrieve a statically typed response if required.                  
                """
            )
            return self.__backward_compatibility_return().__await__()  # type: ignore[return-value]


class ClusterPipelineCommandRequest(PipelineCommandRequest[CommandResponseT]):
    def __init__(
        self,
        client: ClusterPipeline[Any],
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., CommandResponseT],
        execution_parameters: ExecutionParameters | None = None,
    ) -> None:
        self.position: int = 0
        self.result: Any | None = None
        self.asking: bool = False
        super().__init__(
            client, name, *arguments, callback=callback, execution_parameters=execution_parameters
        )


class NodeCommands:
    def __init__(
        self,
        client: RedisCluster[AnyStr],
        connection: ClusterConnection,
        in_transaction: bool = False,
        timeout: float | None = None,
    ):
        self.client: RedisCluster[Any] = client
        self.connection = connection
        self.commands: list[ClusterPipelineCommandRequest[Any]] = []
        self.in_transaction = in_transaction
        self.timeout = timeout
        self.multi_cmd: asyncio.Future[ResponseType] | None = None
        self.exec_cmd: asyncio.Future[ResponseType] | None = None

    def extend(self, c: list[ClusterPipelineCommandRequest[Any]]) -> None:
        self.commands.extend(c)

    def append(self, c: ClusterPipelineCommandRequest[Any]) -> None:
        self.commands.append(c)

    async def write(self) -> None:
        connection = self.connection
        commands = self.commands

        # We are going to clobber the commands with the write, so go ahead
        # and ensure that nothing is sitting there from a previous run.

        for c in commands:
            c.result = None

        # build up all commands into a single request to increase network perf
        # send all the commands and catch connection and timeout errors.
        try:
            if self.in_transaction:
                self.multi_cmd = await connection.create_request(
                    CommandName.MULTI, timeout=self.timeout
                )
            requests = await connection.create_requests(
                [
                    CommandInvocation(
                        cmd.name,
                        cmd.arguments,
                        (
                            bool(cmd.execution_parameters.get("decode"))
                            if cmd.execution_parameters.get("decode")
                            else None
                        ),
                        None,
                    )
                    for cmd in commands
                ],
                timeout=self.timeout,
            )
            if self.in_transaction:
                self.exec_cmd = await connection.create_request(
                    CommandName.EXEC, timeout=self.timeout
                )
            for i, cmd in enumerate(commands):
                cmd.response = requests[i]
        except (ConnectionError, TimeoutError) as e:
            for c in commands:
                c.result = e

    async def read(self) -> None:
        connection = self.connection
        success = True
        multi_result = None
        if self.multi_cmd:
            multi_result = await self.multi_cmd
            success = multi_result in {b"OK", "OK"}
        for c in self.commands:
            if c.result is None:
                try:
                    c.result = await c.response if c.response else None
                except ExecAbortError:
                    raise
                except (ConnectionError, TimeoutError, RedisError) as e:
                    success = False
                    c.result = e
        if self.in_transaction and self.exec_cmd:
            if success:
                res = await self.exec_cmd
                if res:
                    transaction_result = cast(list[ResponseType], res)
                else:
                    raise WatchError("Watched variable changed.")
                for idx, c in enumerate(
                    [
                        _c
                        for _c in sorted(self.commands, key=lambda x: x.position)
                        if _c.name not in {CommandName.MULTI, CommandName.EXEC}
                    ]
                ):
                    if isinstance(c.callback, AsyncPreProcessingCallback):
                        await c.callback.pre_process(self.client, transaction_result[idx])
                    c.result = c.callback(
                        transaction_result[idx],
                        version=connection.protocol_version,
                    )
            elif isinstance(multi_result, BaseException):
                raise multi_result


class PipelineMeta(ABCMeta):
    RESULT_CALLBACKS: dict[str, Callable[..., Any]]
    NODES_FLAGS: dict[str, NodeFlag]

    def __new__(
        cls, name: str, bases: tuple[type, ...], namespace: dict[str, object]
    ) -> PipelineMeta:
        kls = super().__new__(cls, name, bases, namespace)

        for name, method in PipelineMeta.get_methods(kls).items():
            if getattr(method, "__coredis_command", None):
                setattr(kls, name, wrap_pipeline_method(kls, method))

        return kls

    @staticmethod
    def get_methods(kls: PipelineMeta) -> dict[str, Callable[..., Any]]:
        return dict(k for k in inspect.getmembers(kls) if inspect.isfunction(k[1]))


class ClusterPipelineMeta(PipelineMeta):
    def __new__(
        cls, name: str, bases: tuple[type, ...], namespace: dict[str, object]
    ) -> PipelineMeta:
        kls = super().__new__(cls, name, bases, namespace)
        for name, method in ClusterPipelineMeta.get_methods(kls).items():
            cmd = getattr(method, "__coredis_command", None)
            if cmd:
                if cmd.cluster.route:
                    kls.NODES_FLAGS[cmd.command] = cmd.cluster.route
                if cmd.cluster.multi_node:
                    kls.RESULT_CALLBACKS[cmd.command] = cmd.cluster.combine or (lambda r, **_: r)
                else:
                    kls.RESULT_CALLBACKS[cmd.command] = lambda response, **_: list(
                        response.values()
                    ).pop()
        return kls


class Pipeline(Client[AnyStr], metaclass=PipelineMeta):
    """Pipeline for the Redis class"""

    """
    Pipelines provide a way to transmit multiple commands to the Redis server
    in one transmission.  This is convenient for batch processing, such as
    saving all the values in a list to Redis.

    All commands executed within a pipeline are wrapped with MULTI and EXEC
    calls. This guarantees all commands executed in the pipeline will be
    executed atomically.

    Any command raising an exception does *not* halt the execution of
    subsequent commands in the pipeline. Instead, the exception is caught
    and its instance is placed into the response list returned by await pipeline.execute()
    """

    command_stack: list[PipelineCommandRequest[Any]]
    connection_pool: ConnectionPool

    def __init__(
        self,
        client: Client[AnyStr],
        transaction: bool | None,
        watches: Parameters[KeyT] | None = None,
        timeout: float | None = None,
    ) -> None:
        self.client: Client[AnyStr] = client
        self.connection_pool = client.connection_pool
        self.connection: Connection | None = None
        self._transaction = transaction
        self.watching = False
        self.watches: Parameters[KeyT] | None = watches or None
        self.command_stack = []
        self.cache = None  # not implemented.
        self.explicit_transaction = False
        self.scripts: set[Script[AnyStr]] = set()
        self.timeout = timeout
        self.type_adapter = client.type_adapter

    async def __aenter__(self) -> Pipeline[AnyStr]:
        return await self.get_instance()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.clear()

    def __await__(self) -> Generator[Any, Any, Pipeline[AnyStr]]:
        return self.get_instance().__await__()

    def __len__(self) -> int:
        return len(self.command_stack)

    def __bool__(self) -> bool:
        return True

    async def get_instance(self) -> Pipeline[AnyStr]:
        return self

    def create_request(
        self,
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., T_co],
        execution_parameters: ExecutionParameters | None = None,
    ) -> CommandRequest[T_co]:
        """
        :meta private:
        """
        return PipelineCommandRequest(
            self, name, *arguments, callback=callback, execution_parameters=execution_parameters
        )

    async def clear(self) -> None:
        """
        Empties the pipeline and resets / returns the connection
        back to the pool
        """
        self.command_stack.clear()
        self.scripts = set()
        # make sure to reset the connection state in the event that we were
        # watching something

        if self.watching and self.connection:
            try:
                # call this manually since our unwatch or
                # immediate_execute_command methods can call clear()
                request = await self.connection.create_request(CommandName.UNWATCH, decode=False)
                await request
            except ConnectionError:
                # disconnect will also remove any previous WATCHes
                self.connection.disconnect()
        # clean up the other instance attributes
        self.watching = False
        self.watches = []
        self.explicit_transaction = False
        # we can safely return the connection to the pool here since we're
        # sure we're no longer WATCHing anything

        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None

    #: :meta private:
    reset_pipeline = clear

    @deprecated(
        "The reset method in pipelines clashes with the redis ``RESET`` command. Use :meth:`clear` instead",
        "5.0.0",
    )
    def reset(self) -> CommandRequest[None]:
        """
        Empties the pipeline and resets / returns the connection
        back to the pool

        :meta private:
        """
        return self.clear()  # type: ignore

    def multi(self) -> None:
        """
        Starts a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """

        if self.explicit_transaction:
            raise RedisError("Cannot issue nested calls to MULTI")

        if self.command_stack:
            raise RedisError("Commands without an initial WATCH have already been issued")
        self.explicit_transaction = True

    def execute_command(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **options: Unpack[ExecutionParameters],
    ) -> Awaitable[R]:
        raise NotImplementedError

    async def immediate_execute_command(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **kwargs: Unpack[ExecutionParameters],
    ) -> R:
        """
        Executes a command immediately, but don't auto-retry on a
        ConnectionError if we're already WATCHing a variable. Used when
        issuing WATCH or subsequent commands retrieving their values but before
        MULTI is called.

        :meta private:
        """
        conn = self.connection
        # if this is the first call, we need a connection
        if not conn:
            conn = await self.connection_pool.get_connection()
            self.connection = conn
        try:
            request = await conn.create_request(
                command.name, *command.arguments, decode=kwargs.get("decode")
            )
            return callback(
                await request,
                version=conn.protocol_version,
            )
        except (ConnectionError, TimeoutError):
            conn.disconnect()

            # if we're not already watching, we can safely retry the command
            try:
                if not self.watching:
                    request = await conn.create_request(
                        command.name, *command.arguments, decode=kwargs.get("decode")
                    )
                    return callback(await request, version=conn.protocol_version)
                raise
            except ConnectionError:
                # the retry failed so cleanup.
                conn.disconnect()
                await self.clear()
                raise
        finally:
            if command.name in UNWATCH_COMMANDS:
                self.watching = False
            elif command.name == CommandName.WATCH:
                self.watching = True

    def pipeline_execute_command(
        self,
        command: PipelineCommandRequest[R],
    ) -> None:
        """
        Stages a command to be executed next execute() invocation

        Returns the current Pipeline object back so commands can be
        chained together, such as:

        pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')

        At some other point, you can then run: pipe.execute(),
        which will execute all commands queued in the pipe.

        :meta private:
        """
        self.command_stack.append(command)

    async def _execute_transaction(
        self,
        connection: BaseConnection,
        commands: list[PipelineCommandRequest[Any]],
        raise_on_error: bool,
    ) -> tuple[Any, ...]:
        multi_cmd = await connection.create_request(CommandName.MULTI, timeout=self.timeout)
        requests = await connection.create_requests(
            [
                CommandInvocation(
                    cmd.name,
                    cmd.arguments,
                    (
                        bool(cmd.execution_parameters.get("decode"))
                        if cmd.execution_parameters.get("decode")
                        else None
                    ),
                    None,
                )
                for cmd in commands
            ],
            timeout=self.timeout,
        )
        exec_cmd = await connection.create_request(CommandName.EXEC, timeout=self.timeout)
        for i, cmd in enumerate(commands):
            cmd.queued_response = cast(Awaitable[StringT], requests[i])

        errors: list[tuple[int, RedisError | None]] = []
        multi_failed = False

        # parse off the response for MULTI
        # NOTE: we need to handle ResponseErrors here and continue
        # so that we read all the additional command messages from
        # the socket
        try:
            await multi_cmd
        except RedisError:
            multi_failed = True
            errors.append((0, cast(RedisError, sys.exc_info()[1])))

        # and all the other commands
        for i, cmd in enumerate(commands):
            try:
                if cmd.queued_response:
                    assert (await cmd.queued_response) in {b"QUEUED", "QUEUED"}
            except RedisError:
                ex = cast(RedisError, sys.exc_info()[1])
                self.annotate_exception(ex, i + 1, cmd.name, cmd.arguments)
                errors.append((i, ex))

        response: list[ResponseType]
        try:
            response = cast(
                list[ResponseType],
                await exec_cmd if exec_cmd else None,
            )
        except (ExecAbortError, ResponseError):
            if self.explicit_transaction and not multi_failed:
                await self.immediate_execute_command(
                    RedisCommand(name=CommandName.DISCARD, arguments=()), callback=BoolCallback()
                )

            if errors and errors[0][1]:
                raise errors[0][1]
            raise

        if response is None:
            raise WatchError("Watched variable changed.")

        # put any parse errors into the response

        for i, e in errors:
            response.insert(i, cast(ResponseType, e))

        if len(response) != len(commands):
            if self.connection:
                self.connection.disconnect()
            raise ResponseError("Wrong number of response items from pipeline execution")

        # find any errors in the response and raise if necessary
        if raise_on_error:
            self.raise_first_error(commands, response)

        # We have to run response callbacks manually
        data: list[Any] = []
        for r, cmd in zip(response, commands):
            if not isinstance(r, Exception):
                if isinstance(cmd.callback, AsyncPreProcessingCallback):
                    await cmd.callback.pre_process(self.client, r)
                r = cmd.callback(r, version=connection.protocol_version, **cmd.execution_parameters)
                cmd.response = asyncio.get_running_loop().create_future()
                cmd.response.set_result(r)
            data.append(r)
        return tuple(data)

    async def _execute_pipeline(
        self,
        connection: BaseConnection,
        commands: list[PipelineCommandRequest[Any]],
        raise_on_error: bool,
    ) -> tuple[Any, ...]:
        # build up all commands into a single request to increase network perf
        requests = await connection.create_requests(
            [
                CommandInvocation(
                    cmd.name,
                    cmd.arguments,
                    (
                        bool(cmd.execution_parameters.get("decode"))
                        if cmd.execution_parameters.get("decode")
                        else None
                    ),
                    None,
                )
                for cmd in commands
            ],
            timeout=self.timeout,
        )
        for i, cmd in enumerate(commands):
            cmd.response = requests[i]

        response: list[Any] = []
        for cmd in commands:
            try:
                res = await cmd.response if cmd.response else None
                if isinstance(cmd.callback, AsyncPreProcessingCallback):
                    await cmd.callback.pre_process(self.client, res, **cmd.execution_parameters)
                resp = cmd.callback(
                    res,
                    version=connection.protocol_version,
                    **cmd.execution_parameters,
                )
                cmd.response = asyncio.get_event_loop().create_future()
                cmd.response.set_result(resp)
                response.append(resp)
            except ResponseError as re:
                cmd.response = asyncio.get_event_loop().create_future()
                cmd.response.set_exception(re)
                response.append(sys.exc_info()[1])
        if raise_on_error:
            self.raise_first_error(commands, response)

        return tuple(response)

    def raise_first_error(
        self, commands: list[PipelineCommandRequest[Any]], response: ResponseType
    ) -> None:
        assert isinstance(response, list)
        for i, r in enumerate(response):
            if isinstance(r, RedisError):
                self.annotate_exception(r, i + 1, commands[i].name, commands[i].arguments)
                raise r

    def annotate_exception(
        self,
        exception: RedisError | None,
        number: int,
        command: bytes,
        args: Iterable[RedisValueT],
    ) -> None:
        if exception:
            cmd = command.decode("latin-1")
            args = " ".join(map(str, args))
            msg = f"Command # {number} ({cmd} {args}) of pipeline caused error: {str(exception.args[0])}"
            exception.args = (msg,) + exception.args[1:]

    async def load_scripts(self) -> None:
        # make sure all scripts that are about to be run on this pipeline exist
        scripts = list(self.scripts)
        immediate = self.immediate_execute_command
        shas = [s.sha for s in scripts]
        # we can't use the normal script_* methods because they would just
        # get buffered in the pipeline.
        exists = await immediate(
            RedisCommand(CommandName.SCRIPT_EXISTS, tuple(shas)), callback=BoolsCallback()
        )

        if not all(exists):
            for s, exist in zip(scripts, exists):
                if not exist:
                    s.sha = await immediate(
                        RedisCommand(CommandName.SCRIPT_LOAD, (s.script,)),
                        callback=AnyStrCallback[AnyStr](),
                    )

    async def execute(self, raise_on_error: bool = True) -> tuple[Any, ...]:
        """Executes all the commands in the current pipeline"""
        stack = self.command_stack

        if not stack:
            return ()

        if self.scripts:
            await self.load_scripts()

        if self._transaction or self.explicit_transaction:
            exec = self._execute_transaction
        else:
            exec = self._execute_pipeline

        conn = self.connection

        if not conn:
            conn = await self.connection_pool.get_connection()
            # assign to self.connection so clear() releases the connection
            # back to the pool after we're done
            self.connection = conn

        try:
            return await exec(conn, stack, raise_on_error)
        except (ConnectionError, TimeoutError, CancelledError):
            conn.disconnect()

            # if we were watching a variable, the watch is no longer valid
            # since this connection has died. raise a WatchError, which
            # indicates the user should retry his transaction. If this is more
            # than a temporary failure, the WATCH that the user next issues
            # will fail, propegating the real ConnectionError

            if self.watching:
                raise WatchError("A ConnectionError occured on while watching one or more keys")
            # otherwise, it's safe to retry since the transaction isn't
            # predicated on any state

            return await exec(conn, stack, raise_on_error)
        finally:
            await self.clear()

    def watch(self, *keys: KeyT) -> CommandRequest[bool]:
        """
        Watches the values at ``keys`` for change. Commands issues after this call
        will be executed immediately and should be awaited. To switch back to
        pipeline buffering mode, call :meth:`multi`.
        """
        if self.explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")

        return self.create_request(CommandName.WATCH, *keys, callback=SimpleStringCallback())

    def unwatch(self) -> CommandRequest[bool]:
        """
        Removes watches from any previously specified keys and returns the pipeline
        to buffered mode.
        """
        return self.create_request(CommandName.UNWATCH, callback=SimpleStringCallback())


class ClusterPipeline(Client[AnyStr], metaclass=ClusterPipelineMeta):
    client: RedisCluster[AnyStr]
    connection_pool: ClusterConnectionPool
    command_stack: list[ClusterPipelineCommandRequest[Any]]

    RESULT_CALLBACKS: dict[str, Callable[..., Any]] = {}
    NODES_FLAGS: dict[str, NodeFlag] = {}

    def __init__(
        self,
        client: RedisCluster[AnyStr],
        transaction: bool | None = False,
        watches: Parameters[KeyT] | None = None,
        timeout: float | None = None,
    ) -> None:
        self.command_stack = []
        self.refresh_table_asap = False
        self.client = client
        self.connection_pool = client.connection_pool
        self.result_callbacks = client.result_callbacks
        self._transaction = transaction
        self._watched_node: ManagedNode | None = None
        self._watched_connection: ClusterConnection | None = None
        self.watches: Parameters[KeyT] | None = watches or None
        self.watching = False
        self.explicit_transaction = False
        self.cache = None  # not implemented.
        self.timeout = timeout
        self.type_adapter = client.type_adapter

    def create_request(
        self,
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., T_co],
        execution_parameters: ExecutionParameters | None = None,
    ) -> CommandRequest[T_co]:
        """
        :meta private:
        """
        return ClusterPipelineCommandRequest(
            self, name, *arguments, callback=callback, execution_parameters=execution_parameters
        )

    def watch(self, *keys: KeyT) -> CommandRequest[bool]:
        """
        Watches the values at ``keys`` for change. Commands issues after this call
        will be executed immediately and should be awaited. To switch back to
        pipeline buffering mode, call :meth:`multi`.
        """
        if self.explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")

        return self.create_request(CommandName.WATCH, *keys, callback=SimpleStringCallback())

    async def unwatch(self) -> bool:
        """
        Removes watches from any previously specified keys and returns the pipeline
        to buffered mode.
        """
        if self._watched_connection:
            try:
                return await self._unwatch(self._watched_connection)
            finally:
                if self._watched_connection:
                    self.connection_pool.release(self._watched_connection)
                    self.watching = False
                    self._watched_node = None
                    self._watched_connection = None
        return True

    def __del__(self) -> None:
        if self._watched_connection:
            self.connection_pool.release(self._watched_connection)

    def __len__(self) -> int:
        return len(self.command_stack)

    def __bool__(self) -> bool:
        return True

    def __await__(self) -> Generator[None, None, Self]:
        yield
        return self

    async def __aenter__(self) -> ClusterPipeline[AnyStr]:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.clear()

    def execute_command(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **options: Unpack[ExecutionParameters],
    ) -> Awaitable[R]:
        raise NotImplementedError

    def pipeline_execute_command(
        self,
        command: ClusterPipelineCommandRequest[Any],
    ) -> None:
        command.position = len(self.command_stack)
        self.command_stack.append(command)

    def raise_first_error(self) -> None:
        for c in self.command_stack:
            r = c.result

            if isinstance(r, RedisError):
                self.annotate_exception(r, c.position + 1, c.name, c.arguments)
                raise r

    def annotate_exception(
        self,
        exception: RedisError | None,
        number: int,
        command: bytes,
        args: Iterable[RedisValueT],
    ) -> None:
        if exception:
            cmd = command.decode("latin-1")
            args = " ".join(str(x) for x in args)
            msg = f"Command # {number} ({cmd} {args}) of pipeline caused error: {exception.args[0]}"
            exception.args = (msg,) + exception.args[1:]

    async def execute(self, raise_on_error: bool = True) -> tuple[object, ...]:
        """Executes all the commands in the current pipeline"""
        await self.connection_pool.initialize()

        if not self.command_stack:
            return ()

        if self._transaction or self.explicit_transaction:
            execute = self.send_cluster_transaction
        else:
            execute = self.send_cluster_commands
        try:
            return await execute(raise_on_error)
        finally:
            await self.clear()

    async def clear(self) -> None:
        """
        Empties the pipeline and resets / returns the connection
        back to the pool
        """
        self.command_stack = []

        self.scripts: set[Script[AnyStr]] = set()
        # clean up the other instance attributes
        self.watching = False
        self.explicit_transaction = False
        self._watched_node = None
        if self._watched_connection:
            self.connection_pool.release(self._watched_connection)
            self._watched_connection = None

    #: :meta private:
    reset_pipeline = clear

    @deprecated(
        "The reset method in pipelines clashes with the redis ``RESET`` command. Use :meth:`clear` instead",
        "5.0.0",
    )
    def reset(self) -> CommandRequest[None]:
        """
        Empties the pipeline and resets / returns the connection
        back to the pool

        :meta private:
        """
        return self.clear()  # type: ignore

    @retryable(policy=ConstantRetryPolicy((ClusterDownError,), 3, 0.1))
    async def send_cluster_transaction(self, raise_on_error: bool = True) -> tuple[object, ...]:
        """
        :meta private:
        """
        attempt = sorted(self.command_stack, key=lambda x: x.position)
        slots: set[int] = set()
        for c in attempt:
            slot = self._determine_slot(c.name, *c.arguments, **c.execution_parameters)
            if slot:
                slots.add(slot)

            if len(slots) > 1:
                raise ClusterTransactionError("Multiple slots involved in transaction")
        if not slots:
            raise ClusterTransactionError("No slots found for transaction")
        node = self.connection_pool.get_node_by_slot(slots.pop())

        if self._watched_node and node.name != self._watched_node.name:
            raise ClusterTransactionError("Multiple slots involved in transaction")

        conn = self._watched_connection or await self.connection_pool.get_connection_by_node(node)

        if self.watches:
            await self._watch(node, conn, self.watches)
        node_commands = NodeCommands(self.client, conn, in_transaction=True, timeout=self.timeout)
        node_commands.extend(attempt)
        self.explicit_transaction = True

        await node_commands.write()
        try:
            await node_commands.read()
        except ExecAbortError:
            if self.explicit_transaction:
                request = await conn.create_request(CommandName.DISCARD)
                await request
        # If at least one watched key is modified before the EXEC command,
        # the whole transaction aborts,
        # and EXEC returns a Null reply to notify that the transaction failed.

        if node_commands.exec_cmd and await node_commands.exec_cmd is None:
            raise WatchError
        self.connection_pool.release(conn)

        if self.watching:
            await self._unwatch(conn)

        if raise_on_error:
            self.raise_first_error()

        return tuple(
            n.result
            for n in node_commands.commands
            if n.name not in {CommandName.MULTI, CommandName.EXEC}
        )

    @retryable(policy=ConstantRetryPolicy((ClusterDownError,), 3, 0.1))
    async def send_cluster_commands(
        self, raise_on_error: bool = True, allow_redirections: bool = True
    ) -> tuple[object, ...]:
        """
        Sends a bunch of cluster commands to the redis cluster.

        `allow_redirections` If the pipeline should follow `ASK` & `MOVED` responses
        automatically. If set to false it will raise RedisClusterException.

        :meta private:
        """
        # the first time sending the commands we send all of the commands that were queued up.
        # if we have to run through it again, we only retry the commands that failed.
        attempt = sorted(self.command_stack, key=lambda x: x.position)

        protocol_version: int = 3
        # build a list of node objects based on node names we need to
        nodes: dict[str, NodeCommands] = {}
        # as we move through each command that still needs to be processed,
        # we figure out the slot number that command maps to, then from the slot determine the node.
        for c in attempt:
            # refer to our internal node -> slot table that tells us where a given
            # command should route to.
            slot = self._determine_slot(c.name, *c.arguments)
            node = self.connection_pool.get_node_by_slot(slot)

            if node.name not in nodes:
                nodes[node.name] = NodeCommands(
                    self.client,
                    await self.connection_pool.get_connection_by_node(node),
                    timeout=self.timeout,
                )

            nodes[node.name].append(c)

        # send the commands in sequence.
        # we  write to all the open sockets for each node first, before reading anything
        # this allows us to flush all the requests out across the network essentially in parallel
        # so that we can read them all in parallel as they come back.
        # we dont' multiplex on the sockets as they come available, but that shouldn't make
        # too much difference.
        node_commands = nodes.values()

        for n in node_commands:
            await n.write()

        for n in node_commands:
            await n.read()

        # release all of the redis connections we allocated earlier back into the connection pool.
        # we used to do this step as part of a try/finally block, but it is really dangerous to
        # release connections back into the pool if for some reason the socket has data still left
        # in it from a previous operation. The write and read operations already have try/catch
        # around them for all known types of errors including connection and socket level errors.
        # So if we hit an exception, something really bad happened and putting any of
        # these connections back into the pool is a very bad idea.
        # the socket might have unread buffer still sitting in it, and then the
        # next time we read from it we pass the buffered result back from a previous
        # command and every single request after to that connection will always get
        # a mismatched result. (not just theoretical, I saw this happen on production x.x).
        for n in nodes.values():
            protocol_version = n.connection.protocol_version
            self.connection_pool.release(n.connection)
        # if the response isn't an exception it is a valid response from the node
        # we're all done with that command, YAY!
        # if we have more commands to attempt, we've run into problems.
        # collect all the commands we are allowed to retry.
        # (MOVED, ASK, or connection errors or timeout errors)
        attempt = sorted(
            (c for c in attempt if isinstance(c.result, ERRORS_ALLOW_RETRY)),
            key=lambda x: x.position,
        )

        if attempt and allow_redirections:
            # RETRY MAGIC HAPPENS HERE!
            # send these remaing comamnds one at a time using `execute_command`
            # in the main client. This keeps our retry logic in one place mostly,
            # and allows us to be more confident in correctness of behavior.
            # at this point any speed gains from pipelining have been lost
            # anyway, so we might as well make the best attempt to get the correct
            # behavior.
            #
            # The client command will handle retries for each individual command
            # sequentially as we pass each one into `execute_command`. Any exceptions
            # that bubble out should only appear once all retries have been exhausted.
            #
            # If a lot of commands have failed, we'll be setting the
            # flag to rebuild the slots table from scratch. So MOVED errors should
            # correct .commandsthemselves fairly quickly.
            await self.connection_pool.nodes.increment_reinitialize_counter(len(attempt))

            for c in attempt:
                try:
                    # send each command individually like we do in the main client.
                    c.result = await self.client.execute_command(
                        RedisCommand(c.name, c.arguments), **c.execution_parameters
                    )
                except RedisError as e:
                    c.result = e

        # turn the response back into a simple flat array that corresponds
        # to the sequence of commands issued in the stack in pipeline.execute()
        response = []
        for c in sorted(self.command_stack, key=lambda x: x.position):
            r = c.result
            if not isinstance(c.result, RedisError):
                if isinstance(c.callback, AsyncPreProcessingCallback):
                    await c.callback.pre_process(self.client, c.result)
                r = c.callback(c.result, version=protocol_version)
            response.append(r)

        if raise_on_error:
            self.raise_first_error()

        return tuple(response)

    def _determine_slot(
        self, command: bytes, *args: ValueT, **options: Unpack[ExecutionParameters]
    ) -> int:
        """Figure out what slot based on command and args"""

        keys: tuple[RedisValueT, ...] = cast(
            tuple[RedisValueT, ...], options.get("keys")
        ) or KeySpec.extract_keys(command, *args)  # type: ignore

        if not keys:
            raise RedisClusterException(
                f"No way to dispatch {nativestr(command)} to Redis Cluster. Missing key"
            )
        slots = {hash_slot(b(key)) for key in keys}

        if len(slots) != 1:
            raise ClusterCrossSlotError(command=command, keys=keys)
        return slots.pop()

    def _fail_on_redirect(self, allow_redirections: bool) -> None:
        if not allow_redirections:
            raise RedisClusterException("ASK & MOVED redirection not allowed in this pipeline")

    def multi(self) -> None:
        """
        Starts a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """
        if self.explicit_transaction:
            raise RedisError("Cannot issue nested calls to MULTI")

        if self.command_stack:
            raise RedisError("Commands without an initial WATCH have already been issued")
        self.explicit_transaction = True

    async def immediate_execute_command(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **kwargs: Unpack[ExecutionParameters],
    ) -> R:
        slot = self._determine_slot(command.name, *command.arguments)
        node = self.connection_pool.get_node_by_slot(slot)
        if command.name == CommandName.WATCH:
            if self._watched_node and node.name != self._watched_node.name:
                raise ClusterTransactionError(
                    "Cannot issue a watch on a different node in the same transaction"
                )
            else:
                self._watched_node = node
            self._watched_connection = conn = (
                self._watched_connection or await self.connection_pool.get_connection_by_node(node)
            )
        else:
            conn = await self.connection_pool.get_connection_by_node(node)

        try:
            request = await conn.create_request(
                command.name, *command.arguments, decode=kwargs.get("decode")
            )

            return callback(
                await request,
                version=conn.protocol_version,
            )
        except (ConnectionError, TimeoutError):
            conn.disconnect()

            try:
                if not self.watching:
                    request = await conn.create_request(
                        command.name, *command.arguments, decode=kwargs.get("decode")
                    )
                    return callback(await request, version=conn.protocol_version)
                else:
                    raise
            except ConnectionError:
                # the retry failed so cleanup.
                conn.disconnect()
                await self.clear()
                raise
        finally:
            release = True
            if command.name in UNWATCH_COMMANDS:
                self.watching = False
            elif command.name == CommandName.WATCH:
                self.watching = True
                release = False
            if release:
                self.connection_pool.release(conn)

    def load_scripts(self) -> None:
        raise RedisClusterException("method load_scripts() is not implemented")

    async def _watch(self, node: ManagedNode, conn: BaseConnection, keys: Parameters[KeyT]) -> bool:
        for key in keys:
            slot = self._determine_slot(CommandName.WATCH, key)
            dist_node = self.connection_pool.get_node_by_slot(slot)

            if node.name != dist_node.name:
                raise ClusterTransactionError("Keys in request don't hash to the same node")

        if self.explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")
        request = await conn.create_request(CommandName.WATCH, *keys)

        return SimpleStringCallback()(
            cast(StringT, await request),
            version=conn.protocol_version,
        )

    async def _unwatch(self, conn: BaseConnection) -> bool:
        """Unwatches all previously specified keys"""
        if not self.watching:
            return True
        request = await conn.create_request(CommandName.UNWATCH, decode=False)
        res = cast(bytes, await request)
        return res == b"OK"
