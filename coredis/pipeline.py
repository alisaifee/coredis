from __future__ import annotations

import functools
import inspect
import sys
import textwrap
from abc import ABCMeta
from concurrent.futures import CancelledError
from dataclasses import dataclass, field
from itertools import chain
from types import TracebackType
from typing import Any, cast

from wrapt import ObjectProxy  # type: ignore

from coredis._utils import b, clusterdown_wrapper, hash_slot
from coredis.client import Client
from coredis.commands._key_spec import KeySpec
from coredis.commands.constants import CommandName, NodeFlag
from coredis.commands.script import Script
from coredis.connection import BaseConnection, ClusterConnection
from coredis.exceptions import (
    AskError,
    ClusterCrossSlotError,
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
from coredis.response._callbacks import (
    AnyStrCallback,
    BoolCallback,
    BoolsCallback,
    NoopCallback,
    SimpleStringCallback,
)
from coredis.typing import (
    AnyStr,
    Callable,
    Coroutine,
    Dict,
    Generic,
    Iterable,
    KeyT,
    List,
    Node,
    Optional,
    Parameters,
    ParamSpec,
    ResponseType,
    Set,
    StringT,
    Tuple,
    Type,
    TypeVar,
    ValueT,
)

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

ERRORS_ALLOW_RETRY = (
    ConnectionError,
    TimeoutError,
    MovedError,
    AskError,
    TryAgainError,
)


def wrap_pipeline_method(
    kls: PipelineMeta, func: Callable[P, Coroutine[Any, Any, R]]
) -> Callable[P, Coroutine[Any, Any, R]]:
    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        return await func(*args, **kwargs)

    wrapper.__annotations__ = wrapper.__annotations__.copy()
    wrapper.__annotations__["return"] = kls
    wrapper.__doc__ = textwrap.dedent(wrapper.__doc__ or "")
    wrapper.__doc__ = f"""
Pipeline variant of :meth:`coredis.Redis.{func.__name__}` that does not execute
immediately and instead pushes the command into a stack for batch send
and returns the instance of :class:`{kls.__name__}` itself.

To fetch the return values call :meth:`{kls.__name__}.execute` to process the pipeline
and retrieve responses for the commands executed in the pipeline.

{wrapper.__doc__}
"""
    return wrapper


@dataclass
class PipelineCommand:
    command: bytes
    args: Tuple[ValueT, ...]
    callback: Callable[..., Any] = NoopCallback()  # type: ignore
    options: Dict[str, Optional[ValueT]] = field(default_factory=dict)


@dataclass
class ClusterPipelineCommand(PipelineCommand):
    position: int = 0
    result: Optional[Any] = None  # type: ignore
    asking: bool = False


class NodeCommands:
    def __init__(
        self,
        parse_response: Callable[..., Coroutine[Any, Any, ResponseType]],
        connection: ClusterConnection,
        in_transaction: bool = False,
    ):
        self.parse_response = parse_response
        self.connection = connection
        self.commands: List[ClusterPipelineCommand] = []
        self.in_transaction = in_transaction

    def extend(self, c: List[ClusterPipelineCommand]) -> None:
        self.commands.extend(c)

    def append(self, c: ClusterPipelineCommand) -> None:
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
            await connection.send_packed_command(
                connection.packer.pack_commands(
                    [(c.command,) + c.args for c in commands]
                )
            )
        except (ConnectionError, TimeoutError) as e:
            for c in commands:
                c.result = e

    async def read(self) -> None:
        connection = self.connection
        success = True

        for c in self.commands:
            if c.result is None:
                try:
                    if self.in_transaction:
                        cmd = "_"
                    else:
                        cmd = c.command
                    c.result = await self.parse_response(connection, cmd, **c.options)
                except ExecAbortError:
                    raise
                except (ConnectionError, TimeoutError) as e:
                    success = False
                    for c in self.commands:
                        c.result = e
                    break
                except RedisError:
                    success = False
                    c.result = sys.exc_info()[1]
                    break

        if self.in_transaction:
            transaction_result = []
            if success:
                for c in self.commands:
                    if c.command == CommandName.EXEC:
                        if c.result:
                            transaction_result = cast(List[ResponseType], c.result)
                        else:
                            raise WatchError()
                for idx, c in enumerate(
                    [
                        _c
                        for _c in sorted(self.commands, key=lambda x: x.position)
                        if _c.command not in {CommandName.MULTI, CommandName.EXEC}
                    ]
                ):
                    c.result = c.callback(
                        transaction_result[idx],
                        version=connection.protocol_version,
                        **c.options,
                    )
            elif isinstance(self.commands[0].result, BaseException):
                raise self.commands[0].result


class PipelineMeta(ABCMeta):
    RESULT_CALLBACKS: Dict[str, Callable[..., Any]]
    NODES_FLAGS: Dict[str, NodeFlag]

    def __new__(cls, name: str, bases: Tuple[type, ...], namespace: Dict[str, object]):
        kls = super().__new__(cls, name, bases, namespace)

        for name, method in PipelineMeta.get_methods(kls).items():
            if getattr(method, "__coredis_command", None):
                setattr(kls, name, wrap_pipeline_method(kls, method))

        return kls

    @staticmethod
    def get_methods(kls: PipelineMeta) -> Dict[str, Callable[..., Any]]:
        return dict(k for k in inspect.getmembers(kls) if inspect.isfunction(k[1]))


class ClusterPipelineMeta(PipelineMeta):
    def __new__(cls, name: str, bases: Tuple[type, ...], namespace: Dict[str, object]):
        kls = super().__new__(cls, name, bases, namespace)
        for name, method in ClusterPipelineMeta.get_methods(kls).items():
            if cmd := getattr(method, "__coredis_command", None):
                if cmd.cluster.route:
                    kls.NODES_FLAGS[cmd.command] = cmd.cluster.route
                if cmd.cluster.multi_node:
                    kls.RESULT_CALLBACKS[cmd.command] = cmd.cluster.combine or (
                        lambda r, **_: r
                    )
                else:
                    kls.RESULT_CALLBACKS[cmd.command] = lambda response, **_: list(
                        response.values()
                    ).pop()
        return kls


class PipelineImpl(Client[AnyStr], metaclass=PipelineMeta):
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
    and its instance is placed into the response list returned by execute().
    Code iterating over the response list should be able to deal with an
    instance of an exception as a potential value. In general, these will be
    ResponseError exceptions, such as those raised when issuing a command
    on a key of a different datatype.
    """

    command_stack: List[PipelineCommand]
    UNWATCH_COMMANDS = {CommandName.DISCARD, CommandName.EXEC, CommandName.UNWATCH}

    def __init__(
        self,
        connection_pool: ConnectionPool,
        transaction: Optional[bool],
        watches: Optional[Parameters[KeyT]] = None,
    ) -> None:
        self.connection_pool = connection_pool
        self.connection = None
        self._transaction = transaction
        self.watching = False
        self.watches: Optional[Parameters[KeyT]] = watches or None
        self.command_stack = []
        self.cache = None  # not implemented.
        self.explicit_transaction = False
        self.scripts: Set[Script[AnyStr]] = set()

    async def __aenter__(self) -> "PipelineImpl[AnyStr]":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self.reset_pipeline()

    def __len__(self) -> int:
        return len(self.command_stack)

    def __bool__(self) -> bool:
        return True

    async def reset_pipeline(self) -> None:
        self.command_stack.clear()
        self.scripts: Set[Script[AnyStr]] = set()
        # make sure to reset the connection state in the event that we were
        # watching something

        if self.watching and self.connection:
            try:
                # call this manually since our unwatch or
                # immediate_execute_command methods can call reset_pipeline()
                await self.connection.send_command(CommandName.UNWATCH)
                await self.connection.read_response()
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

    def multi(self) -> None:
        """
        Starts a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """

        if self.explicit_transaction:
            raise RedisError("Cannot issue nested calls to MULTI")

        if self.command_stack:
            raise RedisError(
                "Commands without an initial WATCH have already been issued"
            )
        self.explicit_transaction = True

    async def execute_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., Any] = NoopCallback(),  # type: ignore
        **options: Optional[ValueT],
    ) -> PipelineImpl[AnyStr]:  # type: ignore
        if (
            self.watching or command == CommandName.WATCH
        ) and not self.explicit_transaction:
            return await self.immediate_execute_command(
                command, *args, callback=callback, **options
            )  # type: ignore

        return self.pipeline_execute_command(
            command, *args, callback=callback, **options
        )

    async def immediate_execute_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., Any] = NoopCallback(),  # type: ignore
        **kwargs: Optional[ValueT],
    ) -> Any:  # type: ignore
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
            await conn.send_command(command, *args)

            return callback(
                await self.parse_response(conn, command, **kwargs),
                version=conn.protocol_version,
            )
        except (ConnectionError, TimeoutError) as e:
            conn.disconnect()

            if not conn.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            # if we're not already watching, we can safely retry the command
            try:
                if not self.watching:
                    await conn.send_command(command, *args)

                    return callback(
                        await self.parse_response(conn, command, **kwargs),
                        version=conn.protocol_version,
                    )
            except ConnectionError:
                # the retry failed so cleanup.
                conn.disconnect()
                await self.reset_pipeline()
                raise

    def pipeline_execute_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., Any],
        **options: Optional[ValueT],
    ) -> PipelineImpl[AnyStr]:
        """
        Stages a command to be executed next execute() invocation

        Returns the current Pipeline object back so commands can be
        chained together, such as:

        pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')

        At some other point, you can then run: pipe.execute(),
        which will execute all commands queued in the pipe.

        :meta private:
        """
        self.command_stack.append(
            PipelineCommand(
                command=command, args=args, options=options, callback=callback
            )
        )

        return self

    async def _execute_transaction(
        self,
        connection: BaseConnection,
        commands: List[PipelineCommand],
        raise_on_error: bool,
    ) -> Tuple[Any, ...]:

        cmds = chain(
            [PipelineCommand(command=CommandName.MULTI, args=())],
            commands,
            [PipelineCommand(command=CommandName.EXEC, args=())],
        )
        all_cmds = connection.packer.pack_commands(
            [(cmd.command,) + cmd.args for cmd in cmds]
        )
        if self.watches:
            await self.watch(*self.watches)
        await connection.send_packed_command(all_cmds)
        errors: List[Tuple[int, Optional[RedisError]]] = []
        multi_failed = False

        # parse off the response for MULTI
        # NOTE: we need to handle ResponseErrors here and continue
        # so that we read all the additional command messages from
        # the socket
        try:
            await self.parse_response(connection, b"_")
        except RedisError:
            multi_failed = True
            errors.append((0, cast(RedisError, sys.exc_info()[1])))

        # and all the other commands

        for i, cmd in enumerate(commands):
            try:
                await self.parse_response(connection, b"_")
            except RedisError:
                ex = cast(RedisError, sys.exc_info()[1])
                self.annotate_exception(ex, i + 1, cmd.command, cmd.args)
                errors.append((i, ex))

        # parse the EXEC.
        response: List[ResponseType]

        try:
            response = cast(
                List[ResponseType], await self.parse_response(connection, b"_")
            )
        except (ExecAbortError, ResponseError):
            if self.explicit_transaction and not multi_failed:
                await self.immediate_execute_command(
                    CommandName.DISCARD, callback=BoolCallback()
                )

            if errors and errors[0][1]:
                raise errors[0][1]
            raise

        if response is None:
            raise WatchError("Watched variable changed.")

        # put any parse errors into the response

        for i, e in errors:
            response.insert(i, e)

        if len(response) != len(commands):
            if self.connection:
                self.connection.disconnect()
            raise ResponseError(
                "Wrong number of response items from " "pipeline execution"
            )

        # find any errors in the response and raise if necessary

        if raise_on_error:
            self.raise_first_error(commands, response)

        # We have to run response callbacks manually
        data: List[Any] = []
        for r, cmd in zip(response, commands):
            if not isinstance(r, Exception):
                r = cmd.callback(r, version=connection.protocol_version, **cmd.options)
            data.append(r)
        return tuple(data)

    async def _execute_pipeline(
        self,
        connection: BaseConnection,
        commands: List[PipelineCommand],
        raise_on_error: bool,
    ) -> Tuple[Any, ...]:
        # build up all commands into a single request to increase network perf
        all_cmds = connection.packer.pack_commands(
            [(cmd.command,) + cmd.args for cmd in commands]
        )
        await connection.send_packed_command(all_cmds)

        response: List[Any] = []

        for cmd in commands:
            try:
                response.append(
                    cmd.callback(
                        await self.parse_response(
                            connection, cmd.command, **cmd.options
                        ),
                        **cmd.options,
                    )
                )
            except ResponseError:
                response.append(sys.exc_info()[1])

        if raise_on_error:
            self.raise_first_error(commands, response)

        return tuple(response)

    def raise_first_error(
        self, commands: List[PipelineCommand], response: ResponseType
    ) -> None:
        assert isinstance(response, list)
        for i, r in enumerate(response):
            if isinstance(r, ResponseError):
                self.annotate_exception(r, i + 1, commands[i].command, commands[i].args)
                raise r

    def annotate_exception(
        self,
        exception: Optional[RedisError],
        number: int,
        command: bytes,
        args: Iterable[ValueT],
    ) -> None:
        if exception:
            cmd = command.decode("latin-1")
            args = " ".join(map(str, args))
            msg = "Command # {} ({} {}) of pipeline caused error: {}".format(
                number,
                cmd,
                args,
                str(exception.args[0]),
            )
            exception.args = (msg,) + exception.args[1:]

    async def _parse(
        self,
        connection: BaseConnection,
        command_name: bytes,
        **options: Optional[ValueT],
    ) -> ResponseType:
        "Parses a response from the Redis server"
        decode = options.get("decode")
        return await connection.read_response(
            decode=decode if decode is None else bool(decode)
        )

    async def parse_response(
        self,
        connection: BaseConnection,
        command_name: bytes,
        **options: Optional[ValueT],
    ) -> ResponseType:
        result = await self._parse(connection, command_name, **options)

        if command_name in self.UNWATCH_COMMANDS:
            self.watching = False
        elif command_name == CommandName.WATCH:
            self.watching = True

        return result

    async def load_scripts(self):
        # make sure all scripts that are about to be run on this pipeline exist
        scripts = list(self.scripts)
        immediate = self.immediate_execute_command
        shas = [s.sha for s in scripts]
        # we can't use the normal script_* methods because they would just
        # get buffered in the pipeline.
        exists = await immediate(
            CommandName.SCRIPT_EXISTS, *shas, callback=BoolsCallback()
        )

        if not all(exists):
            for s, exist in zip(scripts, exists):
                if not exist:
                    s.sha = await immediate(
                        CommandName.SCRIPT_LOAD,
                        s.script,
                        callback=AnyStrCallback[AnyStr](),
                    )

    async def execute(self, raise_on_error: bool = True) -> Tuple[Any, ...]:
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
            # assign to self.connection so reset_pipeline() releases the connection
            # back to the pool after we're done
            self.connection = conn

        try:
            return await exec(conn, stack, raise_on_error)
        except (ConnectionError, TimeoutError, CancelledError) as e:
            conn.disconnect()

            if not conn.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            # if we were watching a variable, the watch is no longer valid
            # since this connection has died. raise a WatchError, which
            # indicates the user should retry his transaction. If this is more
            # than a temporary failure, the WATCH that the user next issues
            # will fail, propegating the real ConnectionError

            if self.watching:
                raise WatchError(
                    "A ConnectionError occured on while watching one or more keys"
                )
            # otherwise, it's safe to retry since the transaction isn't
            # predicated on any state

            return await exec(conn, stack, raise_on_error)
        finally:
            await self.reset_pipeline()

    async def watch(self, *keys: KeyT) -> bool:
        """
        Watches the values at keys ``keys``
        """

        if self.explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")

        return await self.immediate_execute_command(
            CommandName.WATCH, *keys, callback=SimpleStringCallback()
        )

    async def unwatch(self) -> bool:
        """Unwatches all previously specified keys"""

        return (
            await self.immediate_execute_command(
                CommandName.UNWATCH, callback=SimpleStringCallback()
            )
            if self.watching
            else True
        )


class ClusterPipelineImpl(Client[AnyStr], metaclass=ClusterPipelineMeta):

    connection_pool: ClusterConnectionPool
    command_stack: List[ClusterPipelineCommand]

    RESULT_CALLBACKS: Dict[str, Callable[..., Any]] = {}
    NODES_FLAGS: Dict[str, NodeFlag] = {}

    def __init__(
        self,
        connection_pool: ClusterConnectionPool,
        result_callbacks: Optional[Dict[bytes, Callable[..., Any]]] = None,
        startup_nodes: Optional[List[Node]] = None,
        transaction: Optional[bool] = False,
        watches: Optional[Parameters[KeyT]] = None,
    ) -> None:
        self.command_stack = []
        self.refresh_table_asap = False
        self.connection_pool: ClusterConnectionPool = connection_pool
        self.result_callbacks = result_callbacks
        self.startup_nodes = startup_nodes if startup_nodes else []
        self._transaction = transaction
        self.watches: Optional[Parameters[KeyT]] = watches or None
        self.watching = False
        self.explicit_transaction = False
        self.cache = None  # not implemented.

    async def watch(self, *keys: KeyT) -> bool:
        raise NotImplementedError

    async def unwatch(self) -> bool:
        raise NotImplementedError

    def __repr__(self):
        return f"{type(self).__name__}"

    def __del__(self):
        self.reset_pipeline()

    def __len__(self):
        return len(self.command_stack)

    def __bool__(self) -> bool:
        return True

    async def __aenter__(self) -> "ClusterPipelineImpl[AnyStr]":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.reset_pipeline()

    async def execute_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., Any] = NoopCallback(),  # type: ignore
        **options: Optional[ValueT],
    ) -> ClusterPipelineImpl[AnyStr]:  # type: ignore
        return self.pipeline_execute_command(
            command, *args, callback=callback, **options
        )

    def pipeline_execute_command(
        self,
        command: bytes,
        *args: ValueT,
        callback: Callable[..., Any],
        **options: Optional[ValueT],
    ) -> ClusterPipelineImpl[AnyStr]:
        self.command_stack.append(
            ClusterPipelineCommand(
                command=command,
                args=args,
                options=options,
                callback=callback,
                position=len(self.command_stack),
            )
        )

        return self

    def raise_first_error(self) -> None:
        for c in self.command_stack:
            r = c.result

            if isinstance(r, RedisError):
                self.annotate_exception(r, c.position + 1, c.command, c.args)
                raise r

    def annotate_exception(
        self,
        exception: Optional[RedisError],
        number: int,
        command: bytes,
        args: Iterable[ValueT],
    ) -> None:
        if exception:
            cmd = command.decode("latin-1")
            args = " ".join(str(x) for x in args)
            msg = "Command # {} ({} {}) of pipeline caused error: {}".format(
                number, cmd, args, exception.args[0]
            )
            exception.args = (msg,) + exception.args[1:]

    async def execute(self, raise_on_error: bool = True) -> Tuple[object, ...]:
        await self.connection_pool.initialize()

        if not self.command_stack:
            return ()

        if self._transaction:
            execute = self.send_cluster_transaction
        else:
            execute = self.send_cluster_commands
        try:
            return await execute(raise_on_error)
        finally:
            self.reset_pipeline()

    def reset_pipeline(self):
        """Empties pipeline"""
        self.command_stack = []

        self.scripts: Set[Script[AnyStr]] = set()
        # clean up the other instance attributes
        self.watching = False
        self.explicit_transaction = False

    @clusterdown_wrapper
    async def send_cluster_transaction(
        self, raise_on_error: bool = True
    ) -> Tuple[object, ...]:
        attempt = sorted(self.command_stack, key=lambda x: x.position)
        slots: Set[int] = set()
        for c in attempt:
            slot = self._determine_slot(c.command, *c.args)
            if slot:
                slots.add(slot)

            if len(slots) > 1:
                raise ClusterTransactionError("Multiple nodes involved in transaction")
        if not slots:
            raise ClusterTransactionError("No slots found for transaction")
        node = self.connection_pool.get_node_by_slot(slots.pop())
        conn = self.connection_pool.get_connection_by_node(node)

        if self.watches:
            await self._watch(node, conn, self.watches)
        node_commands = NodeCommands(self.parse_response, conn, in_transaction=True)
        node_commands.append(ClusterPipelineCommand(CommandName.MULTI, ()))
        node_commands.extend(attempt)
        node_commands.append(ClusterPipelineCommand(CommandName.EXEC, ()))
        self.explicit_transaction = True

        await node_commands.write()
        try:
            await node_commands.read()
        except ExecAbortError:
            if self.explicit_transaction:
                await conn.send_command(CommandName.DISCARD)
                await conn.read_response()

        # If at least one watched key is modified before the EXEC command,
        # the whole transaction aborts,
        # and EXEC returns a Null reply to notify that the transaction failed.

        if node_commands.commands[-1].result is None:
            raise WatchError
        self.connection_pool.release(conn)

        if self.watching:
            await self._unwatch(conn)

        if raise_on_error:
            self.raise_first_error()

        return tuple(
            n.result
            for n in node_commands.commands
            if n.command not in {CommandName.MULTI, CommandName.EXEC}
        )

    @clusterdown_wrapper
    async def send_cluster_commands(
        self, raise_on_error: bool = True, allow_redirections: bool = True
    ) -> Tuple[object, ...]:
        """
        Sends a bunch of cluster commands to the redis cluster.

        `allow_redirections` If the pipeline should follow `ASK` & `MOVED` responses
        automatically. If set to false it will raise RedisClusterException.
        """
        # the first time sending the commands we send all of the commands that were queued up.
        # if we have to run through it again, we only retry the commands that failed.
        attempt = sorted(self.command_stack, key=lambda x: x.position)

        protocol_version: int = 3
        # build a list of node objects based on node names we need to
        nodes: Dict[str, NodeCommands] = {}
        # as we move through each command that still needs to be processed,
        # we figure out the slot number that command maps to, then from the slot determine the node.
        for c in attempt:
            # refer to our internal node -> slot table that tells us where a given
            # command should route to.
            slot = self._determine_slot(c.command, *c.args)
            node = self.connection_pool.get_node_by_slot(slot)

            # little hack to make sure the node name is populated. probably could clean this up.
            self.connection_pool.nodes.set_node_name(node)

            # now that we know the name of the node ( it's just a string in the form of host:port )
            # we can build a list of commands for each node.
            node_name = node["name"]

            if node_name not in nodes:
                nodes[node_name] = NodeCommands(
                    self.parse_response,
                    self.connection_pool.get_connection_by_node(node),
                )

            nodes[node_name].append(c)

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
            await self.connection_pool.nodes.increment_reinitialize_counter(
                len(attempt)
            )

            for c in attempt:
                try:
                    # send each command individually like we do in the main client.
                    c.result = await super().execute_command(
                        c.command, *c.args, callback=c.callback, **c.options
                    )
                except RedisError as e:
                    c.result = e

        # turn the response back into a simple flat array that corresponds
        # to the sequence of commands issued in the stack in pipeline.execute()
        response = tuple(
            c.callback(c.result, version=protocol_version, **c.options)
            for c in sorted(self.command_stack, key=lambda x: x.position)
        )

        if raise_on_error:
            self.raise_first_error()

        return response

    async def parse_response(
        self,
        connection: ClusterConnection,
        command_name: bytes,
        **options: Any,
    ) -> ResponseType:
        """
        Parses a response from the Redis server
        :meta private:
        """
        decode = options.get("decode")
        return await connection.read_response(
            decode=decode if decode is None else bool(decode)
        )

    def _determine_slot(self, command: bytes, *args: ValueT) -> int:
        """Figure out what slot based on command and args"""

        keys: Tuple[ValueT, ...] = KeySpec.extract_keys((command,) + args)

        if not keys:
            raise RedisClusterException(
                f"No way to dispatch {command} to Redis Cluster. Missing key"
            )

        slots = {hash_slot(b(key)) for key in keys}

        if len(slots) != 1:
            raise ClusterCrossSlotError(command=command, keys=keys)
        return slots.pop()

    def _fail_on_redirect(self, allow_redirections: bool) -> None:
        if not allow_redirections:
            raise RedisClusterException(
                "ASK & MOVED redirection not allowed in this pipeline"
            )

    def multi(self) -> None:
        raise RedisClusterException("method multi() is not implemented")

    def immediate_execute_command(
        self,
        command: CommandName,
        *args: ValueT,
        callback: Callable[..., Any] = NoopCallback(),
        **kwargs: Optional[ValueT],
    ) -> Any:
        raise RedisClusterException(
            "method immediate_execute_command() is not implemented"
        )

    def load_scripts(self):
        raise RedisClusterException("method load_scripts() is not implemented")

    async def _watch(
        self, node: Node, conn: BaseConnection, keys: Parameters[KeyT]
    ) -> bool:
        "Watches the values at keys ``keys``"

        for key in keys:
            slot = self._determine_slot(CommandName.WATCH, key)
            dist_node = self.connection_pool.get_node_by_slot(slot)

            if node.get("name") != dist_node["name"]:
                # raise error if commands in a transaction can not hash to same node

                if len(node) > 0:
                    raise ClusterTransactionError(
                        "Keys in request don't hash to the same node"
                    )

        if self.explicit_transaction:
            raise RedisError("Cannot issue a WATCH after a MULTI")
        await conn.send_command(CommandName.WATCH, *keys)

        return SimpleStringCallback()(
            cast(StringT, await conn.read_response()), version=conn.protocol_version
        )

    async def _unwatch(self, conn: BaseConnection) -> bool:
        """Unwatches all previously specified keys"""
        await conn.send_command(CommandName.UNWATCH)
        res = cast(StringT, await conn.read_response())
        return res in {"OK", b"OK"} if self.watching else True


class Pipeline(ObjectProxy, Generic[AnyStr]):  # type: ignore
    """
    Class returned by :meth:`coredis.Redis.pipeline`

    The class exposes the redis command methods available in
    :class:`~coredis.Redis`, however each of those methods returns
    the instance itself and the results of the batched commands
    can be retrieved by calling :meth:`execute`.
    """

    __wrapped__: PipelineImpl[AnyStr]

    async def __aenter__(self) -> Pipeline[AnyStr]:
        return cast(Pipeline[AnyStr], await self.__wrapped__.__aenter__())

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self.__wrapped__.__aexit__(exc_type, exc_value, traceback)

    @classmethod
    def proxy(
        cls,
        connection_pool: ConnectionPool,
        transaction: Optional[bool] = None,
        watches: Optional[Parameters[KeyT]] = None,
    ) -> Pipeline[AnyStr]:
        return cls(
            PipelineImpl(
                connection_pool,
                transaction=transaction,
                watches=watches,
            )
        )

    def multi(self) -> None:
        """
        Starts a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with :meth:`execute`
        """
        self.__wrapped__.multi()  # Only here for documentation purposes.

    async def watch(self, *keys: KeyT) -> bool:  # noqa
        """
        Watches the values at keys ``keys``
        """
        return await self.__wrapped__.watch(
            *keys
        )  # Only here for documentation purposes.

    async def unwatch(self) -> bool:  # noqa
        """
        Unwatches all previously specified keys
        """
        return await self.__wrapped__.unwatch()  # Only here for documentation purposes.

    async def execute(self, raise_on_error: bool = True) -> Tuple[object, ...]:
        """
        Executes all the commands in the current pipeline
        and return the results of the individual batched commands
        """

        # Only here for documentation purposes.
        return await self.__wrapped__.execute(raise_on_error=raise_on_error)

    async def reset(self) -> None:
        """
        Resets the command stack and releases any connections acquired from the
        pool
        """
        await self.__wrapped__.reset_pipeline()


class ClusterPipeline(ObjectProxy, Generic[AnyStr]):  # type: ignore
    """
    Class returned by :meth:`coredis.RedisCluster.pipeline`

    The class exposes the redis command methods available in
    :class:`~coredis.Redis`, however each of those methods returns
    the instance itself and the results of the batched commands
    can be retrieved by calling :meth:`execute`.
    """

    __wrapped__: ClusterPipelineImpl[AnyStr]

    async def __aenter__(self) -> ClusterPipeline[AnyStr]:
        return cast(ClusterPipeline[AnyStr], await self.__wrapped__.__aenter__())

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self.__wrapped__.__aexit__(exc_type, exc_value, traceback)

    @classmethod
    def proxy(
        cls,
        connection_pool: ClusterConnectionPool,
        result_callbacks: Optional[Dict[bytes, Callable[..., Any]]] = None,
        startup_nodes: Optional[List[Node]] = None,
        transaction: Optional[bool] = False,
        watches: Optional[Parameters[KeyT]] = None,
    ) -> ClusterPipeline[AnyStr]:
        return cls(
            ClusterPipelineImpl(
                connection_pool,
                result_callbacks=result_callbacks,
                startup_nodes=startup_nodes,
                transaction=transaction,
                watches=watches,
            )
        )

    async def watch(self, *keys: KeyT) -> bool:  # noqa
        """
        Unsupported for Cluster
        """
        raise NotImplementedError

    async def unwatch(self) -> bool:  # noqa
        """
        Unsupported for Cluster
        """
        raise NotImplementedError

    async def execute(self, raise_on_error: bool = True) -> Tuple[object, ...]:
        """
        Executes all the commands in the current pipeline
        and return the results of the individual batched commands
        """
        # Only here for documentation purposes.
        return await self.__wrapped__.execute(raise_on_error=raise_on_error)

    def reset(self) -> None:
        """
        Resets the command stack and releases any connections acquired from the
        pool
        """
        self.__wrapped__.reset_pipeline()
