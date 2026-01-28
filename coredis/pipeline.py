from __future__ import annotations

import functools
import inspect
import textwrap
from abc import ABCMeta
from concurrent.futures import CancelledError
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, cast

from anyio import AsyncContextManagerMixin, sleep

from coredis._utils import b, hash_slot, nativestr
from coredis.client import Client, RedisCluster
from coredis.commands import CommandRequest, CommandResponseT
from coredis.commands._key_spec import KeySpec
from coredis.commands.constants import CommandName, NodeFlag
from coredis.commands.request import TransformedResponse, is_type_like
from coredis.commands.script import Script
from coredis.connection import BaseConnection, CommandInvocation, Request
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
    TryAgainError,
    WatchError,
)
from coredis.pool import ClusterConnectionPool
from coredis.pool.nodemanager import ManagedNode
from coredis.response._callbacks import (
    AnyStrCallback,
    AsyncPreProcessingCallback,
    BoolsCallback,
    NoopCallback,
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
    ParamSpec,
    RedisCommand,
    RedisCommandP,
    RedisValueT,
    ResponseType,
    Self,
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

    wrapper.__doc__ = textwrap.dedent(wrapper.__doc__ or "")
    wrapper.__doc__ = f"""
.. note:: Pipeline variant of :meth:`coredis.Redis.{func.__name__}` that does not execute
  immediately and instead pushes the command into a stack for batch send.

  The return value can be retrieved either as part of the tuple returned by
  :meth:`~{kls.__name__}.execute` or by awaiting the :class:`~coredis.commands.CommandRequest`
  instance after calling :meth:`~{kls.__name__}.execute`

{wrapper.__doc__}
"""
    return wrapper


class Awaitablize(Awaitable[T]):
    __slots__ = ("_result",)

    def __init__(self, result: T) -> None:
        self._result = result

    def __await__(self) -> Generator[Any, None, T]:
        async def _coro() -> T:
            await sleep(0)  # checkpoint
            return self._result

        # create the coroutine when awaited to avoid Python warning on GC
        return _coro().__await__()


def await_result(result: T) -> Awaitable[T]:
    return Awaitablize(result)


class PipelineCommandRequest(CommandRequest[CommandResponseT]):
    """
    Command request used within a pipeline. Handles immediate execution for WATCH or
    watched commands outside explicit transactions, otherwise queues the command.
    """

    client: Pipeline[Any] | ClusterPipeline[Any]

    def __init__(
        self,
        client: Pipeline[Any] | ClusterPipeline[Any],
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., CommandResponseT],
        execution_parameters: ExecutionParameters | None = None,
        parent: CommandRequest[Any] | None = None,
    ) -> None:
        super().__init__(
            client,
            name,
            *arguments,
            callback=callback,
            execution_parameters=execution_parameters,
        )
        if not parent:
            client.pipeline_execute_command(self)  # type: ignore[arg-type]
        self.parent = parent

    def transform(
        self,
        transformer: type[TransformedResponse] | Callable[[CommandResponseT], TransformedResponse],
    ) -> CommandRequest[TransformedResponse]:
        transform_func = (
            functools.partial(
                self.type_adapter.deserialize,
                return_type=transformer,
            )
            if is_type_like(transformer)
            else transformer
        )
        return cast(type[PipelineCommandRequest[TransformedResponse]], self.__class__)(
            self.client,
            self.name,
            *self.arguments,
            callback=lambda resp, **k: transform_func(resp),
            execution_parameters=self.execution_parameters,
            parent=self,
        )

    def __await__(self) -> Generator[None, None, CommandResponseT]:
        if hasattr(self, "response"):
            return self.response.__await__()
        elif self.parent:

            async def _transformed() -> CommandResponseT:
                if (r := await self.parent) == self.client:  # type: ignore
                    return r  # type: ignore
                else:
                    return self.callback(r)

            return _transformed().__await__()
        exc = ResponseError(
            "Result not set! Either a transaction failed, or you're awaiting a pipeline command before calling execute."
        )
        if self.client._raise_on_error:
            raise exc

        async def _get_exc() -> ResponseError:
            return exc

        return _get_exc().__await__()  # type: ignore


class ClusterPipelineCommandRequest(PipelineCommandRequest[CommandResponseT]):
    """
    Command request for cluster pipelines, tracks position and result for cluster routing.
    """

    def __init__(
        self,
        client: ClusterPipeline[Any],
        name: bytes,
        *arguments: ValueT,
        callback: Callable[..., CommandResponseT],
        execution_parameters: ExecutionParameters | None = None,
        parent: CommandRequest[Any] | None = None,
    ) -> None:
        self.position: int = 0
        self.result: Any = None
        self.asking: bool = False
        super().__init__(
            client,
            name,
            *arguments,
            callback=callback,
            execution_parameters=execution_parameters,
            parent=parent,
        )


class NodeCommands(AsyncContextManagerMixin):
    """
    Helper for grouping and executing commands on a single cluster node, handling transactions if needed.
    """

    connection: BaseConnection

    def __init__(
        self,
        client: RedisCluster[AnyStr],
        node: ManagedNode,
        connection: BaseConnection | None = None,
        in_transaction: bool = False,
        timeout: float | None = None,
    ):
        self.client: RedisCluster[Any] = client
        self.node = node
        self._connection = connection
        self.commands: list[ClusterPipelineCommandRequest[Any]] = []
        self.in_transaction = in_transaction
        self.timeout = timeout
        self.multi_cmd: Request | None = None
        self.exec_cmd: Request | None = None

    def extend(self, c: list[ClusterPipelineCommandRequest[Any]]) -> None:
        self.commands.extend(c)

    def append(self, c: ClusterPipelineCommandRequest[Any]) -> None:
        self.commands.append(c)

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[None]:
        if not self._connection:
            async with self.client.connection_pool.acquire(node=self.node) as self.connection:
                yield
        else:
            self.connection = self._connection
            yield

    def write(self) -> None:
        connection = self.connection
        commands = self.commands

        # Reset results for all commands before writing.
        for c in commands:
            c.result = None

        # Batch all commands into a single request for efficiency.
        try:
            if self.in_transaction:
                self.multi_cmd = connection.create_request(CommandName.MULTI, timeout=self.timeout)
            requests = connection.create_requests(
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
                self.exec_cmd = connection.create_request(CommandName.EXEC, timeout=self.timeout)
            for i, cmd in enumerate(commands):
                cmd.response = requests[i]
        except (ConnectionError, TimeoutError) as e:
            for c in commands:
                c.result = e

    async def read(self) -> None:
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
                    )
                    c.response = await_result(c.result)
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
    """
    Pipeline for batching multiple commands to a Redis server.
    Supports transactions and command stacking.

    All commands executed within a pipeline are wrapped with MULTI and EXEC
    calls when :paramref:`transaction` is ``True``.

    Any command raising an exception does *not* halt the execution of
    subsequent commands in the pipeline. Instead, the exception is caught
    and its instance is placed into the response list returned by :meth:`execute`
    """

    QUEUED_RESPONSES = {b"QUEUED", "QUEUED"}

    def __init__(
        self,
        client: Client[AnyStr],
        transaction: bool | None,
        raise_on_error: bool = True,
        timeout: float | None = None,
    ) -> None:
        self.client: Client[AnyStr] = client
        self._connection: BaseConnection | None = None
        self._transaction = transaction
        self._raise_on_error = raise_on_error
        self.watching = False
        self.command_stack: list[PipelineCommandRequest[Any]] = []
        self.watches: list[KeyT] = []
        self.cache = None
        self.explicit_transaction = False
        self.scripts: set[Script[AnyStr]] = set()
        self.timeout = timeout
        self.type_adapter = client.type_adapter

    def __repr__(self) -> str:
        return f"{type(self).__name__}<{repr(self._connection)}>"

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        yield self
        await self._execute()
        if self._connection:
            self.client.connection_pool.release(self._connection)

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
        Clear the pipeline and reset state.
        """
        self.command_stack.clear()
        self.scripts.clear()
        # Reset connection state if we were watching something.
        if self.watches and self._connection:
            await self._connection.create_request(CommandName.UNWATCH, decode=False)
        self.watches.clear()
        self.explicit_transaction = False

    @asynccontextmanager
    async def watch(self, *keys: KeyT) -> AsyncGenerator[None]:
        """
        The given keys will be watched for changes within this context.
        """
        if self.command_stack:
            raise WatchError("Unable to add a watch after pipeline commands have been added")
        if not self._connection:
            self._connection = await self.client.connection_pool.get_connection()
        self.watches.extend(keys)
        await self.immediate_execute_command(
            RedisCommand(CommandName.WATCH, arguments=tuple(self.watches))
        )
        self.explicit_transaction = True
        yield
        await self._execute()

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
        assert self._connection
        request = self._connection.create_request(
            command.name, *command.arguments, decode=kwargs.get("decode")
        )
        return callback(await request)

    def pipeline_execute_command(
        self,
        command: PipelineCommandRequest[R],
    ) -> None:
        """
        Queue a command for execution on the next `execute()` call.

        :meta private:
        """
        self.command_stack.append(command)

    async def _execute_transaction(
        self,
        connection: BaseConnection,
        commands: list[PipelineCommandRequest[Any]],
    ) -> tuple[Any, ...]:
        requests = connection.create_requests(
            [CommandInvocation(CommandName.MULTI, (), None, None)]
            + [
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
            ]
            + [CommandInvocation(CommandName.EXEC, (), None, None)],
            timeout=self.timeout,
        )

        errors: list[tuple[int, RedisError | TimeoutError | None]] = []
        # parse off the response for MULTI
        # NOTE: we need to handle ResponseErrors here and continue
        # so that we read all the additional command messages from
        # the socket
        try:
            await requests[0]
        except (RedisError, TimeoutError) as e:
            errors.append((0, e))

        # and all the other commands
        for i, cmd in enumerate(commands):
            try:
                if (resp := await requests[i + 1]) not in self.QUEUED_RESPONSES:
                    raise Exception(
                        f"Abnormal response in pipeline for command {cmd.name!r}: {resp!r}"
                    )
            except (RedisError, TimeoutError) as e:
                self.annotate_exception(e, i + 1, cmd.name, cmd.arguments)
                errors.append((i + 1, e))

        try:
            response = cast(list[ResponseType] | None, await requests[-1])
        except (ExecAbortError, ResponseError, TimeoutError) as e:
            if errors and errors[0][1]:
                raise errors[0][1] from e
            raise

        if response is None:
            raise WatchError("Watched variable changed.")

        # put any parse errors into the response
        for i, e in errors:  # type: ignore
            response.insert(i, cast(ResponseType, e))

        if len(response) != len(commands):
            raise ResponseError("Wrong number of response items from pipeline execution")

        # find any errors in the response and raise if necessary
        if self._raise_on_error:
            self.raise_first_error(commands, response)

        # We have to run response callbacks manually
        data: list[Any] = []
        for r, cmd in zip(response, commands):
            if not isinstance(r, Exception):
                if isinstance(cmd.callback, AsyncPreProcessingCallback):
                    await cmd.callback.pre_process(self.client, r)
                r = cmd.callback(r, **cmd.execution_parameters)
                cmd.response = await_result(r)
            data.append(r)
        return tuple(data)

    async def _execute_pipeline(
        self, connection: BaseConnection, commands: list[PipelineCommandRequest[Any]]
    ) -> tuple[Any, ...]:
        # build up all commands into a single request to increase network perf
        requests = connection.create_requests(
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
                    **cmd.execution_parameters,
                )
                cmd.response = await_result(resp)
                response.append(resp)
            except (ResponseError, TimeoutError) as re:
                cmd.response = await_result(re)
                response.append(re)
        if self._raise_on_error:
            self.raise_first_error(commands, response)

        return tuple(response)

    def raise_first_error(
        self, commands: list[PipelineCommandRequest[Any]], response: ResponseType
    ) -> None:
        assert isinstance(response, list)
        for i, r in enumerate(response):
            if isinstance(r, (RedisError, TimeoutError)):
                self.annotate_exception(r, i + 1, commands[i].name, commands[i].arguments)
                raise r

    def annotate_exception(
        self,
        exception: RedisError | TimeoutError | None,
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
        shas = [s.sha for s in scripts]
        exists = await self.immediate_execute_command(
            RedisCommand(CommandName.SCRIPT_EXISTS, tuple(shas)), callback=BoolsCallback()
        )

        if not all(exists):
            for s, exist in zip(scripts, exists):
                if not exist:
                    s.sha = await self.immediate_execute_command(
                        RedisCommand(CommandName.SCRIPT_LOAD, (s.script,)),
                        callback=AnyStrCallback[AnyStr](),
                    )

    async def _execute(self) -> None:
        """
        Execute all queued commands in the pipeline.
        """
        if not self.command_stack:
            return None
        if not self._connection:
            self._connection = await self.client.connection_pool.get_connection()

        if self.scripts:
            await self.load_scripts()
        if self._transaction or self.explicit_transaction:
            exec = self._execute_transaction
        else:
            exec = self._execute_pipeline

        try:
            await exec(self._connection, self.command_stack)
        except (ConnectionError, TimeoutError, CancelledError) as e:
            # if we were watching a variable, the watch is no longer valid
            # since this connection has died. raise a WatchError, which
            # indicates the user should retry his transaction. If this is more
            # than a temporary failure, the WATCH that the user next issues
            # will fail, propegating the real ConnectionError
            if self.watches:
                raise WatchError(
                    "A connection error occured while watching one or more keys"
                ) from e
            raise
        finally:
            await self.clear()


class ClusterPipeline(Client[AnyStr], metaclass=ClusterPipelineMeta):
    """
    Pipeline for batching commands to a Redis Cluster.
    Handles routing, transactions, and error management across nodes.

    .. warning:: Unlike :class:`Pipeline`, :paramref:`transaction` is ``False`` by
       default as there is limited support for transactions in redis cluster
       (only keys in the same slot can be part of a transaction).
    """

    client: RedisCluster[AnyStr]
    connection_pool: ClusterConnectionPool
    command_stack: list[ClusterPipelineCommandRequest[Any]]

    RESULT_CALLBACKS: dict[str, Callable[..., Any]] = {}
    NODES_FLAGS: dict[str, NodeFlag] = {}

    def __init__(
        self,
        client: RedisCluster[AnyStr],
        raise_on_error: bool = True,
        transaction: bool = False,
        timeout: float | None = None,
    ) -> None:
        self.command_stack = []
        self.refresh_table_asap = False
        self.client = client
        self.connection_pool = client.connection_pool
        self.result_callbacks = client.result_callbacks
        self._raise_on_error = raise_on_error
        self._transaction = transaction
        self._watched_node: ManagedNode | None = None
        self._watched_connection: BaseConnection | None = None
        self.watches: list[KeyT] = []
        self.explicit_transaction = False
        self.cache = None
        self.scripts: set[Script[AnyStr]] = set()
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

    @asynccontextmanager
    async def watch(self, *keys: KeyT) -> AsyncGenerator[None]:
        if self.command_stack:
            raise WatchError("Unable to add a watch after pipeline commands have been added")
        try:
            self._watched_node = self.connection_pool.get_node_by_keys(list(keys))
        except RedisClusterException:
            raise ClusterTransactionError("Keys for watch don't hash to the same node")
        self.watches.extend(keys)
        async with self.connection_pool.acquire(
            node=self._watched_node
        ) as self._watched_connection:
            await self._watched_connection.create_request(CommandName.WATCH, *keys)
            self.explicit_transaction = True
            yield
            await self._execute()
            await self._watched_connection.create_request(CommandName.UNWATCH, decode=False)

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        yield self
        await self._execute()

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

            if isinstance(r, (RedisError, TimeoutError)):
                self.annotate_exception(r, c.position + 1, c.name, c.arguments)
                raise r

    def annotate_exception(
        self,
        exception: RedisError | TimeoutError | None,
        number: int,
        command: bytes,
        args: Iterable[RedisValueT],
    ) -> None:
        if exception:
            cmd = command.decode("latin-1")
            args = " ".join(str(x) for x in args)
            msg = f"Command # {number} ({cmd} {args}) of pipeline caused error: {exception.args[0]}"
            exception.args = (msg,) + exception.args[1:]

    async def _execute(self) -> tuple[object, ...]:
        """
        Execute all queued commands in the cluster pipeline. Returns a tuple of results.
        """
        await self.connection_pool.initialize()

        if not self.command_stack:
            return ()
        if self.scripts:
            await self.load_scripts()
        if self._transaction or self.explicit_transaction:
            execute = self.send_cluster_transaction
        else:
            execute = self.send_cluster_commands
        try:
            return await execute(self._raise_on_error)
        finally:
            await self.clear()

    async def clear(self) -> None:
        """
        Clear the pipeline and reset state.
        """
        self.command_stack = []
        self.scripts.clear()
        self.watches.clear()
        self.explicit_transaction = False

    @retryable(policy=ConstantRetryPolicy((ClusterDownError,), retries=3, delay=0.1))
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
        if self._watched_node and node != self._watched_node:
            raise ClusterTransactionError("Watched keys are bogus")

        node_commands = NodeCommands(
            self.client,
            node,
            in_transaction=True,
            timeout=self.timeout,
            connection=self._watched_connection,
        )
        node_commands.extend(attempt)
        self.explicit_transaction = True
        async with node_commands:
            node_commands.write()
            try:
                await node_commands.read()
            except ExecAbortError:
                if self.explicit_transaction:
                    await node_commands.connection.create_request(CommandName.DISCARD)
            # If at least one watched key is modified before EXEC, the transaction aborts and EXEC returns null.

            if node_commands.exec_cmd and await node_commands.exec_cmd is None:
                raise WatchError

            if raise_on_error:
                self.raise_first_error()

            return tuple(
                n.result
                for n in node_commands.commands
                if n.name not in {CommandName.MULTI, CommandName.EXEC}
            )

    @retryable(policy=ConstantRetryPolicy((ClusterDownError,), retries=3, delay=0.1))
    async def send_cluster_commands(
        self, raise_on_error: bool = True, allow_redirections: bool = True
    ) -> tuple[object, ...]:
        """
        Execute all queued commands in the cluster pipeline, handling redirections
        and retries as needed.

        :meta private:
        """
        # On first send, queue all commands. On retry, only failed ones.
        attempt = sorted(self.command_stack, key=lambda x: x.position)

        # Group commands by node for efficient network usage.
        nodes: dict[str, NodeCommands] = {}
        for c in attempt:
            slot = self._determine_slot(c.name, *c.arguments)
            node = self.connection_pool.get_node_by_slot(slot)
            if node.name not in nodes:
                nodes[node.name] = NodeCommands(
                    self.client,
                    node,
                    timeout=self.timeout,
                )
            nodes[node.name].append(c)

        # Write to all nodes, then read from all nodes in sequence.
        for n in nodes.values():
            async with n:
                n.write()
                await n.read()

        # Retry MOVED/ASK/connection errors one by one if allowed.
        attempt = sorted(
            (c for c in attempt if isinstance(c.result, ERRORS_ALLOW_RETRY)),
            key=lambda x: x.position,
        )
        if attempt and allow_redirections:
            await self.connection_pool.nodes.increment_reinitialize_counter(len(attempt))
            for c in attempt:
                try:
                    c.result = await self.client.execute_command(
                        RedisCommand(c.name, c.arguments), **c.execution_parameters
                    )
                except (RedisError, TimeoutError) as e:
                    c.result = e

        # Flatten results to match the original command order.
        response = []
        for c in sorted(self.command_stack, key=lambda x: x.position):
            r = c.result
            if not isinstance(c.result, (RedisError, TimeoutError)):
                if isinstance(c.callback, AsyncPreProcessingCallback):
                    await c.callback.pre_process(self.client, c.result)
                r = c.callback(c.result)
            c.response = await_result(r)
            response.append(r)
        if raise_on_error:
            self.raise_first_error()
        return tuple(response)

    def _determine_slot(
        self, command: bytes, *args: ValueT, **options: Unpack[ExecutionParameters]
    ) -> int:
        """
        Determine the hash slot for the given command and arguments.
        """
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

    async def load_scripts(self) -> None:
        shas = [s.sha for s in self.scripts]
        exists = await self.client.execute_command(
            RedisCommand(CommandName.SCRIPT_EXISTS, tuple(shas)), callback=BoolsCallback()
        )

        if not all(exists):
            for s, exist in zip(self.scripts, exists):
                if not exist:
                    s.sha = await self.client.execute_command(
                        RedisCommand(CommandName.SCRIPT_LOAD, (s.script,)),
                        callback=AnyStrCallback[AnyStr](),
                    )
