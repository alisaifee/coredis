from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Any, cast

from anyio import (
    AsyncContextManagerMixin,
    CancelScope,
    create_task_group,
)
from deprecated.sphinx import versionadded
from exceptiongroup import BaseExceptionGroup

from coredis._utils import EncodingInsensitiveDict, nativestr
from coredis.client import Client, RedisCluster
from coredis.cluster._node import ClusterNodeLocation
from coredis.commands._validators import (
    MutuallyExclusiveParametersError,
    RequiredParameterError,
)
from coredis.commands.constants import CommandName
from coredis.commands.request import CommandRequest
from coredis.connection import TCPLocation
from coredis.connection._base import BaseConnection
from coredis.connection._cluster import ClusterConnection
from coredis.exceptions import (
    AskError,
    ClusterError,
    ConnectionError,
    MovedError,
    RedisError,
)
from coredis.pool import ClusterConnectionPool
from coredis.typing import (
    AnyStr,
    AsyncGenerator,
    Generic,
    Key,
    KeyT,
    Parameters,
    ResponseType,
    Self,
    StringT,
    ValueT,
)

_TRANSPORT_ERRORS = (ConnectionError, TimeoutError)

_Row = tuple[KeyT, tuple[ValueT, ...]]


@versionadded(version="6.9.0")
class HashImport(AsyncContextManagerMixin, Generic[AnyStr]):
    """
    Write many hashes that share one field layout on a single Redis instance.

    Queue rows with :meth:`add`. :meth:`flush` writes early; anything still
    queued is written when the context exits. A pool connection is leased only
    on the first prepare/write and held until exit for discard.
    """

    def __init__(
        self,
        client: Client[AnyStr],
        fieldset: StringT,
        fields: Parameters[StringT],
    ) -> None:
        if not (fields := tuple(fields)):
            raise ValueError("fields must be non-empty")
        self.client: Client[AnyStr] = client
        self.fieldset = fieldset
        self.fields = fields
        self._rows: list[_Row] = []
        self._active = False
        self._connection: BaseConnection | None = None
        self._prepared = False

    def add(
        self,
        key: KeyT,
        values: Parameters[ValueT] | Mapping[StringT, ValueT] | None = None,
        **fields: ValueT,
    ) -> None:
        """
        Queue a hash.

        :param key: hash key
        :param values: values in the same order as the field list, or a mapping
         keyed by those field names. Omit this and pass field names as keywords
         when they are valid Python identifiers.
        """
        if values is not None and fields:
            raise MutuallyExclusiveParametersError({"values", "fields"}, None)
        if values is None and not fields:
            raise RequiredParameterError({"values", "fields"}, None)
        if values is not None:
            self._rows.append((key, self._values(values)))
        else:
            self._rows.append((key, self._values(fields)))

    def _values(self, values: Parameters[ValueT] | Mapping[Any, ValueT]) -> tuple[ValueT, ...]:
        if isinstance(values, Mapping):
            provided = EncodingInsensitiveDict(dict(values))
            wanted = EncodingInsensitiveDict({name: True for name in self.fields})
            missing = [name for name in self.fields if name not in provided]
            extra = [name for name in values if name not in wanted]
            if missing or extra:
                parts: list[str] = []
                if missing:
                    parts.append(f"missing {missing}")
                if extra:
                    parts.append(f"extra {extra}")
                raise ValueError(", ".join(parts))
            return tuple(provided[name] for name in self.fields)
        if len(values := tuple(values)) != len(self.fields):
            raise ValueError(f"expected {len(self.fields)} values, got {len(values)}")
        return values

    def _annotate(
        self,
        exc: BaseException,
        *,
        key: KeyT | None = None,
        node: ClusterNodeLocation | None = None,
        fields: bool = False,
    ) -> None:
        parts = [f"fieldset {nativestr(self.fieldset)!r}"]
        if fields:
            parts.append("fields " + ",".join(nativestr(name) for name in self.fields))
        if key is not None:
            parts.append(f"key {nativestr(key)!r}")
        if node is not None:
            parts.append(f"node {node.host}:{node.port}")
        detail = exc.args[0] if exc.args else type(exc).__name__
        exc.args = (f"HIMPORT ({'; '.join(parts)}): {detail}", *exc.args[1:])

    async def flush(self) -> None:
        """Write queued rows. Safe to call more than once."""
        if not self._active:
            raise RuntimeError("HashImport must be used as an async context manager")
        if not self._rows:
            return
        await self._write(list(self._rows))
        self._rows.clear()

    def _set_command(self, key: KeyT, values: tuple[ValueT, ...]) -> CommandRequest[ResponseType]:
        return CommandRequest(
            CommandName.HIMPORT_SET,
            Key(key),
            self.fieldset,
            *values,
            execution_parameters={},
            type_adapter=self.client.type_adapter,
        )

    async def _ensure_connection(self) -> BaseConnection:
        if self._connection is None:
            self._connection = cast(
                BaseConnection, await self.client.connection_pool.get_connection()
            )
            self._prepared = False
        return self._connection

    def _abandon_connection(self) -> None:
        connection = self._connection
        assert connection is not None
        connection.invalidate()
        self.client.connection_pool.release(connection)
        self._connection = None
        self._prepared = False

    async def _prepare(self, connection: BaseConnection) -> None:
        try:
            await connection.create_request(
                CommandName.HIMPORT_PREPARE,
                self.fieldset,
                *self.fields,
                decode=False,
                disconnect_on_cancellation=True,
            )
        except (TimeoutError, RedisError) as exc:
            self._annotate(exc, fields=True)
            raise

    async def _discard(self, connection: BaseConnection) -> None:
        await connection.create_request(
            CommandName.HIMPORT_DISCARD,
            self.fieldset,
            decode=False,
            disconnect_on_cancellation=True,
        )

    async def _write(self, rows: list[_Row]) -> None:
        connection = await self._ensure_connection()
        try:
            if not self._prepared:
                await self._prepare(connection)
                self._prepared = True
            batch = await connection.create_request_batch(
                [self._set_command(key, values) for key, values in rows]
            )
            for (key, _values), result in zip(rows, batch, strict=True):
                if isinstance(result, BaseException):
                    self._annotate(result, key=key)
                    raise result
        except _TRANSPORT_ERRORS:
            self._abandon_connection()
            raise

    async def _shutdown(self, body_error: BaseException | None) -> None:
        connection = self._connection
        if connection is None:
            return
        discard_error: BaseException | None = None
        try:
            if self._prepared and connection.usable:
                try:
                    await self._discard(connection)
                except (*_TRANSPORT_ERRORS, RedisError) as exc:
                    connection.invalidate()
                    discard_error = exc
            elif not self._prepared and connection.usable:
                connection.invalidate()
        finally:
            self.client.connection_pool.release(connection)
            self._connection = None
            self._prepared = False
        if body_error is None and discard_error is not None:
            raise discard_error

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        self._active = True
        body_error: BaseException | None = None
        try:
            yield self
            await self.flush()
        except BaseException as exc:
            body_error = exc
            raise
        finally:
            with CancelScope(shield=True):
                try:
                    await self._shutdown(body_error)
                finally:
                    self._active = False
                    self._rows.clear()


@versionadded(version="6.9.0")
class ClusterHashImport(HashImport[AnyStr]):
    """
    Write many hashes that share one field layout on a Redis Cluster.

    Same public methods as :class:`HashImport`. Rows are grouped by primary;
    each node connection is prepared once. Multi-node flushes run concurrently;
    MOVED/ASK update the layout and re-prepare on the destination.
    """

    def __init__(
        self,
        client: RedisCluster[AnyStr],
        fieldset: StringT,
        fields: Parameters[StringT],
    ) -> None:
        super().__init__(cast(Any, client), fieldset, fields)
        self.client: RedisCluster[AnyStr] = client
        self._connection_pool: ClusterConnectionPool = client.connection_pool
        self._connections: dict[TCPLocation, ClusterConnection] = {}
        self._prepared_nodes: set[TCPLocation] = set()

    def _group(self, rows: list[_Row]) -> dict[ClusterNodeLocation, list[_Row]]:
        layout = self._connection_pool.cluster_layout
        grouped: dict[ClusterNodeLocation, list[_Row]] = defaultdict(list)
        for key, values in rows:
            grouped[layout.node_for_slot(Key(key).slot, primary=True)].append((key, values))
        return grouped

    def _drop_connection(self, location: TCPLocation) -> None:
        self._prepared_nodes.discard(location)
        if connection := self._connections.pop(location, None):
            connection.invalidate()
            self._connection_pool.release(connection)

    async def _ensure_node_connection(
        self, node: ClusterNodeLocation, *, asking: bool = False
    ) -> ClusterConnection:
        location = TCPLocation(node.host, node.port)
        connection = self._connections.get(location)
        if connection is None or not connection.usable:
            if connection is not None:
                self._drop_connection(location)
            connection = await self._connection_pool.get_connection(node=node)
            self._connections[location] = connection
            self._prepared_nodes.discard(location)
        try:
            if location not in self._prepared_nodes:
                await self._prepare(connection)
                self._prepared_nodes.add(location)
            if asking:
                await connection.create_request(
                    CommandName.ASKING,
                    decode=False,
                    disconnect_on_cancellation=True,
                )
        except _TRANSPORT_ERRORS:
            self._drop_connection(location)
            raise
        return connection

    async def _ask_set(self, error: AskError, key: KeyT, values: tuple[ValueT, ...]) -> None:
        node = ClusterNodeLocation(error.host, error.port, server_type="primary")
        connection = await self._ensure_node_connection(node, asking=True)
        request = self._set_command(key, values)
        await connection.create_request(
            request.name,
            *request.serialized_arguments,
            decode=False,
            disconnect_on_cancellation=True,
        )

    async def _flush_node(self, node: ClusterNodeLocation, rows: list[_Row]) -> list[_Row]:
        connection = await self._ensure_node_connection(node)
        batch = await connection.create_request_batch(
            [self._set_command(key, values) for key, values in rows]
        )
        redirects: list[_Row] = []
        for (key, values), result in zip(rows, batch, strict=True):
            if not isinstance(result, BaseException):
                continue
            if isinstance(result, MovedError):
                self._connection_pool.cluster_layout.report_errors(node, result)
                redirects.append((key, values))
            elif isinstance(result, AskError):
                await self._ask_set(result, key, values)
            else:
                self._annotate(result, key=key, node=node)
                raise result
        return redirects

    async def _write_pass(self, pending: list[_Row]) -> list[_Row]:
        grouped = self._group(pending)
        node_results: dict[TCPLocation, list[_Row]] = {}

        async def run_node(node: ClusterNodeLocation, node_rows: list[_Row]) -> None:
            node_results[TCPLocation(node.host, node.port)] = await self._flush_node(
                node, node_rows
            )

        try:
            async with create_task_group() as tg:
                for node, node_rows in grouped.items():
                    tg.start_soon(run_node, node, node_rows)
        except BaseExceptionGroup as eg:
            raise eg.exceptions[0] from None

        return [row for more in node_results.values() for row in more]

    async def _write(self, rows: list[_Row]) -> None:
        pending = list(rows)
        for _ in range(RedisCluster.MAX_RETRIES):
            if not pending:
                return
            pending = await self._write_pass(pending)

        if pending:
            raise ClusterError("Maximum HIMPORT redirect retries exhausted.")

    async def _shutdown(self, body_error: BaseException | None) -> None:
        discard_error: BaseException | None = None
        for location, connection in list(self._connections.items()):
            try:
                if location in self._prepared_nodes and connection.usable:
                    try:
                        await self._discard(connection)
                    except (*_TRANSPORT_ERRORS, RedisError) as exc:
                        connection.invalidate()
                        if discard_error is None:
                            discard_error = exc
                elif location not in self._prepared_nodes and connection.usable:
                    connection.invalidate()
            finally:
                self._connection_pool.release(connection)
        self._connections = {}
        self._prepared_nodes = set()
        if body_error is None and discard_error is not None:
            raise discard_error
