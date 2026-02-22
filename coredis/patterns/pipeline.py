from __future__ import annotations

import functools
from contextlib import asynccontextmanager
from typing import Any, cast

from anyio import AsyncContextManagerMixin
from deprecated.sphinx import versionchanged

from coredis._utils import nativestr
from coredis.client import Client, RedisCluster
from coredis.commands import CommandRequest, CommandResponseT
from coredis.commands._key_spec import KeySpec
from coredis.commands.constants import CommandName
from coredis.commands.request import TransformedResponse, is_type_like
from coredis.commands.script import Script
from coredis.connection._base import BaseConnection, CommandInvocation
from coredis.connection._request import Request
from coredis.exceptions import (
    AskError,
    ClusterCrossSlotError,
    ClusterDownError,
    ClusterTransactionError,
    ConnectionError,
    ExecAbortError,
    MovedError,
    RedisClusterError,
    RedisError,
    ResponseError,
    TryAgainError,
    WatchError,
)
from coredis.pool import ClusterConnectionPool
from coredis.response._callbacks import (
    AnyStrCallback,
    BoolsCallback,
    NoopCallback,
)
from coredis.retry import ConstantRetryPolicy, retryable
from coredis.typing import (
    AnyStr,
    AsyncGenerator,
    Awaitable,
    Callable,
    ExecutionParameters,
    Generator,
    Iterable,
    KeyT,
    ManagedNode,
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


class PipelineResult(Awaitable[T]):
    __slots__ = "_result"

    def __init__(self, result: T) -> None:
        self._result = result

    @property
    def value(self) -> T:
        if isinstance(self._result, Exception):
            raise self._result
        else:
            return self._result

    def __await__(self) -> Generator[Any, None, T]:
        async def _coro() -> T:
            return self.value

        return _coro().__await__()


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
            client._pipeline_execute_command(self)  # type: ignore[arg-type]
        self.parent = parent

    def transform(
        self,
        transformer: type[TransformedResponse] | Callable[[CommandResponseT], TransformedResponse],
    ) -> CommandRequest[TransformedResponse]:
        transform_func = cast(
            Callable[..., TransformedResponse],
            (
                functools.partial(
                    self.type_adapter.deserialize,
                    return_type=transformer,
                )
                if is_type_like(transformer)
                else transformer
            ),
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
        if hasattr(self, "_response"):
            return self._response.__await__()
        elif self.parent:

            async def _transformed() -> CommandResponseT:
                r = await self.parent  # type: ignore
                return self.callback(r)

            return _transformed().__await__()
        raise RuntimeError("You can't await a pipeline command before it completes executing")


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
        raise_on_error: bool = True,
    ):
        self.client: RedisCluster[Any] = client
        self.node = node
        self._connection = connection
        self.commands: list[ClusterPipelineCommandRequest[Any]] = []
        self.in_transaction = in_transaction
        self.timeout = timeout
        self._raise_on_error = raise_on_error
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
                cmd._response = requests[i]
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
                    c.result = await c._response if c._response else None
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
                    c.result = c.callback(
                        transaction_result[idx],
                    )
                    c._response = PipelineResult(c.result)
            elif isinstance(multi_result, BaseException):
                raise multi_result


@versionchanged(
    version="6.0.0",
    reason="Pipelines are no longer awaitable. They support the async context manager protocol and must always be used as such",
)
class Pipeline(Client[AnyStr]):
    """
    Pipeline for batching multiple commands to a Redis server.
    Supports transactions and command stacking.

    All commands executed within a pipeline are wrapped with MULTI and EXEC
    calls when :paramref:`transaction` is ``True``.

    Any command raising an exception does **not** halt the execution of
    subsequent commands in the pipeline, however the first exception encountered
    will be raised when exiting the pipeline if :paramref:`raise_on_error` is ``True``.
    If not the exception is caught and will be returned when awaiting the command that failed.
    """

    QUEUED_RESPONSES = {b"QUEUED", "QUEUED"}

    def __init__(
        self,
        client: Client[AnyStr],
        transaction: bool | None,
        raise_on_error: bool = True,
        timeout: float | None = None,
    ) -> None:
        """
        :param transaction: Whether to wrap the commands in the pipeline in a ``MULTI``, ``EXEC``
        :param raise_on_error: Whether to raise the first error encounterd in the pipeline after
         executing it
        :param timeout: Time in seconds to wait for the pipeline results to return
        """
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
        self._results: tuple[Any] | None = None

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        yield self
        await self._execute()
        if self._connection:
            self.client.connection_pool.release(self._connection)

    def __repr__(self) -> str:
        return f"{type(self).__name__}<{repr(self._connection)}>"

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

    @asynccontextmanager
    async def watch(self, *keys: KeyT) -> AsyncGenerator[None]:
        """
        The given keys will be watched for changes within this context and the
        commands stacked within the context will be automatically executed when the
        context exits.
        """
        if self.command_stack:
            raise WatchError("Unable to add a watch after pipeline commands have been added")
        if not self._connection:
            self._connection = await self.client.connection_pool.get_connection()
        self.watches.extend(keys)
        await self._immediate_execute_command(
            RedisCommand(CommandName.WATCH, arguments=tuple(self.watches))
        )
        self.explicit_transaction = True
        yield
        await self._execute()

    @property
    def results(self) -> tuple[Any, ...] | None:
        """
        The results of the pipeline execution which can be accessed
        after the pipeline has completed.
        """
        if self.command_stack:
            raise RuntimeError("Pipeline results are not available before it completes execution")
        return self._results

    def execute_command(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **options: Unpack[ExecutionParameters],
    ) -> Awaitable[R]:
        raise NotImplementedError

    async def _clear(self) -> None:
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

    async def _immediate_execute_command(
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

    def _pipeline_execute_command(
        self,
        command: PipelineCommandRequest[R],
    ) -> None:
        """
        Queue a command for execution

        :meta private:
        """
        self.command_stack.append(command)

    async def _execute_transaction(
        self,
        connection: BaseConnection,
        commands: list[PipelineCommandRequest[Any]],
    ) -> None:
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
                self._annotate_exception(e, i + 1, cmd.name, cmd.arguments)
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

        # We have to run response callbacks manually
        data: list[Any] = []
        for r, cmd in zip(response, commands):
            r = cmd.callback(r, **cmd.execution_parameters)
            cmd._response = PipelineResult(r)
            data.append(r)

        self._results = tuple(data)
        # find any errors in the response and raise if necessary
        if self._raise_on_error:
            self._raise_first_error(commands, response)

    async def _execute_pipeline(
        self, connection: BaseConnection, commands: list[PipelineCommandRequest[Any]]
    ) -> None:
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
            cmd._response = requests[i]

        response: list[Any] = []
        for cmd in commands:
            try:
                res = await cmd._response if cmd._response else None
                resp = cmd.callback(
                    res,
                    **cmd.execution_parameters,
                )
                cmd._response = PipelineResult(resp)
                response.append(resp)
            except (ResponseError, TimeoutError) as re:
                cmd._response = PipelineResult(re)
                response.append(re)
        self._results = tuple(response)
        if self._raise_on_error:
            self._raise_first_error(commands, response)

    def _raise_first_error(
        self, commands: list[PipelineCommandRequest[Any]], response: ResponseType
    ) -> None:
        assert isinstance(response, list)
        for i, r in enumerate(response):
            if isinstance(r, (RedisError, TimeoutError)):
                self._annotate_exception(r, i + 1, commands[i].name, commands[i].arguments)
                raise r

    def _annotate_exception(
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

    async def _load_scripts(self) -> None:
        # make sure all scripts that are about to be run on this pipeline exist
        scripts = list(self.scripts)
        shas = [s.sha for s in scripts]
        exists = await self._immediate_execute_command(
            RedisCommand(CommandName.SCRIPT_EXISTS, tuple(shas)), callback=BoolsCallback()
        )

        if not all(exists):
            for s, exist in zip(scripts, exists):
                if not exist:
                    s.sha = await self._immediate_execute_command(
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
            await self._load_scripts()
        if self._transaction or self.explicit_transaction:
            exec = self._execute_transaction
        else:
            exec = self._execute_pipeline

        try:
            return await exec(self._connection, self.command_stack)
        except (ConnectionError, TimeoutError) as e:
            if self.watches:
                raise WatchError(
                    "A connection error occurred while watching one or more keys"
                ) from e
            raise
        finally:
            await self._clear()


@versionchanged(
    version="6.0.0",
    reason="Cluster Pipelines are no longer awaitable. They support the async context manager protocol and must always be used as such",
)
class ClusterPipeline(Client[AnyStr]):
    """
    Pipeline for batching multiple commands to a Redis Cluster
    Supports transactions only when all keys map to the same shard, and therefore
    :paramref:`transactions` is set to ``False`` by default due to the limited scope.

    Any command raising an exception does **not** halt the execution of
    subsequent commands in the pipeline, however the first exception encountered
    will be raised when exiting the pipeline if :paramref:`raise_on_error` is ``True``.
    If not the exception is caught and will be returned when awaiting the command that failed.
    """

    client: RedisCluster[AnyStr]
    connection_pool: ClusterConnectionPool
    command_stack: list[ClusterPipelineCommandRequest[Any]]

    def __init__(
        self,
        client: RedisCluster[AnyStr],
        raise_on_error: bool = True,
        transaction: bool = False,
        timeout: float | None = None,
    ) -> None:
        """
        :param transaction: Whether to wrap the commands in the pipeline in a ``MULTI``, ``EXEC``
        :param raise_on_error: Whether to raise the first error encounterd in the pipeline after
         executing it
        :param timeout: Time in seconds to wait for the pipeline results to return
        """
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
        self._results: tuple[Any] | None = None

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        yield self
        await self._execute()

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
        """
        The given keys will be watched for changes within this context and the
        commands stacked within the context will be automatically executed when the
        context exits.
        """
        if self.command_stack:
            raise WatchError("Unable to add a watch after pipeline commands have been added")
        try:
            self._watched_node = self.connection_pool.get_node_by_keys(list(keys))
        except RedisClusterError:
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

    @property
    def results(self) -> tuple[Any, ...] | None:
        """
        The results of the pipeline execution which can be accessed
        after the pipeline has completed.
        """
        if self.command_stack:
            raise RuntimeError("Pipeline results are not available before it completes")
        return self._results

    def execute_command(
        self,
        command: RedisCommandP,
        callback: Callable[..., R] = NoopCallback(),
        **options: Unpack[ExecutionParameters],
    ) -> Awaitable[R]:
        raise NotImplementedError

    async def _clear(self) -> None:
        """
        Clear the pipeline and reset state.
        """
        self.command_stack = []
        self.scripts.clear()
        self.watches.clear()
        self.explicit_transaction = False

    def _pipeline_execute_command(
        self,
        command: ClusterPipelineCommandRequest[Any],
    ) -> None:
        command.position = len(self.command_stack)
        self.command_stack.append(command)

    def _raise_first_error(self) -> None:
        for c in self.command_stack:
            r = c.result

            if isinstance(r, (RedisError, TimeoutError)):
                self._annotate_exception(r, c.position + 1, c.name, c.arguments)
                raise r

    def _annotate_exception(
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

    async def _execute(self) -> None:
        """
        Execute all queued commands in the cluster pipeline. Returns a tuple of results.
        """
        if not self.command_stack:
            return
        if self.scripts:
            await self._load_scripts()
        if self._transaction or self.explicit_transaction:
            execute = self._send_cluster_transaction
        else:
            execute = self._send_cluster_commands
        try:
            await execute(self._raise_on_error)
        finally:
            await self._clear()

    @retryable(policy=ConstantRetryPolicy((ClusterDownError,), retries=3, delay=0.1))
    async def _send_cluster_transaction(self, raise_on_error: bool = True) -> None:
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
            raise ClusterTransactionError("Multiple slots involved in transaction")

        node_commands = NodeCommands(
            self.client,
            node,
            in_transaction=True,
            timeout=self.timeout,
            connection=self._watched_connection,
            raise_on_error=self._raise_on_error,
        )
        node_commands.extend(attempt)
        async with node_commands:
            node_commands.write()
            try:
                await node_commands.read()
            except ExecAbortError:
                await node_commands.connection.create_request(CommandName.DISCARD)

            # If at least one watched key is modified before EXEC, the transaction aborts and EXEC returns null.
            if node_commands.exec_cmd:
                exec_result = await node_commands.exec_cmd
                if exec_result is None:
                    raise WatchError("Watched variable changed.")

            self._results = tuple(
                n.result
                for n in node_commands.commands
                if n.name not in {CommandName.MULTI, CommandName.EXEC}
            )
            if raise_on_error:
                self._raise_first_error()

    @retryable(policy=ConstantRetryPolicy((ClusterDownError,), retries=3, delay=0.1))
    async def _send_cluster_commands(
        self, raise_on_error: bool = True, allow_redirections: bool = True
    ) -> None:
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
                    raise_on_error=self._raise_on_error,
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
                r = c.callback(c.result)
            c._response = PipelineResult(r)
            response.append(r)
        self._results = tuple(response)
        if raise_on_error:
            self._raise_first_error()

    def _determine_slot(
        self, command: bytes, *args: RedisValueT, **options: Unpack[ExecutionParameters]
    ) -> int:
        """
        Determine the hash slot for the given command and arguments.
        """
        keys: tuple[RedisValueT, ...] = cast(
            tuple[RedisValueT, ...], options.get("keys")
        ) or KeySpec.extract_keys(command, *args)

        if not keys:
            raise RedisClusterError(
                f"No way to dispatch {nativestr(command)} to Redis Cluster. Missing key"
            )
        slots = self.connection_pool.nodes.determine_slots(command, *args, **options)

        if len(slots) != 1:
            raise ClusterCrossSlotError(command=command, keys=keys)
        return slots.pop()

    async def _load_scripts(self) -> None:
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
