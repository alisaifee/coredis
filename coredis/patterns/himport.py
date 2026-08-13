from __future__ import annotations

from collections import defaultdict
from contextlib import asynccontextmanager
from typing import cast

from anyio import (
    AsyncContextManagerMixin,
    CancelScope,
    create_task_group,
    get_cancelled_exc_class,
)
from deprecated.sphinx import versionadded
from exceptiongroup import BaseExceptionGroup

from coredis.client import Client, RedisCluster
from coredis.cluster._node import ClusterNodeLocation
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
    TryAgainError,
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

    def add(self, key: KeyT, values: Parameters[ValueT]) -> None:
        """
        Queue a hash.

        :param key: hash key
        :param values: values in the same order as the field list
        """
        if len(values := tuple(values)) != len(self.fields):
            raise ValueError(f"expected {len(self.fields)} values, got {len(values)}")
        self._rows.append((key, values))

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
        if self._connection is not None:
            if not self._connection.usable:
                raise RuntimeError("HashImport connection is no longer usable")
            return self._connection
        connection = cast(BaseConnection, await self.client.connection_pool.get_connection())
        self._connection = connection
        self._prepared = False
        return connection

    def _abandon_connection(self) -> None:
        connection = self._connection
        if connection is None:
            return
        connection.invalidate()
        self.client.connection_pool.release(connection)
        self._connection = None
        self._prepared = False

    async def _write(self, rows: list[_Row]) -> None:
        connection = await self._ensure_connection()
        try:
            if not self._prepared:
                await connection.create_request(
                    CommandName.HIMPORT_PREPARE,
                    self.fieldset,
                    *self.fields,
                    decode=False,
                    disconnect_on_cancellation=True,
                )
                self._prepared = True
            batch = await connection.create_request_batch(
                [self._set_command(key, values) for key, values in rows]
            )
            for result in batch:
                if isinstance(result, BaseException):
                    raise result
        except _TRANSPORT_ERRORS:
            self._abandon_connection()
            raise
        except get_cancelled_exc_class():
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
                    await connection.create_request(
                        CommandName.HIMPORT_DISCARD,
                        self.fieldset,
                        decode=False,
                        disconnect_on_cancellation=True,
                    )
                except (*_TRANSPORT_ERRORS, RedisError) as exc:
                    connection.invalidate()
                    discard_error = exc
                except get_cancelled_exc_class() as exc:
                    connection.invalidate()
                    discard_error = exc
                    raise
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
class ClusterHashImport(AsyncContextManagerMixin, Generic[AnyStr]):
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
        if not (fields := tuple(fields)):
            raise ValueError("fields must be non-empty")
        self.client: RedisCluster[AnyStr] = client
        self.fieldset = fieldset
        self.fields = fields
        self._rows: list[_Row] = []
        self._active = False
        self._connection_pool: ClusterConnectionPool = client.connection_pool
        self._connections: dict[TCPLocation, ClusterConnection] = {}
        self._prepared: set[TCPLocation] = set()

    def add(self, key: KeyT, values: Parameters[ValueT]) -> None:
        """
        Queue a hash.

        :param key: hash key
        :param values: values in the same order as the field list
        """
        if len(values := tuple(values)) != len(self.fields):
            raise ValueError(f"expected {len(self.fields)} values, got {len(values)}")
        self._rows.append((key, values))

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

    def _group(self, rows: list[_Row]) -> dict[ClusterNodeLocation, list[_Row]]:
        layout = self._connection_pool.cluster_layout
        grouped: dict[ClusterNodeLocation, list[_Row]] = defaultdict(list)
        for key, values in rows:
            grouped[layout.node_for_slot(Key(key).slot, primary=True)].append((key, values))
        return grouped

    def _redirect_node(self, host: str, port: int) -> ClusterNodeLocation:
        layout = self._connection_pool.cluster_layout
        location = TCPLocation(host, port)
        if node := layout.node_for_location(location):
            return node
        return ClusterNodeLocation(host, port, server_type="primary")

    def _drop_connection(self, location: TCPLocation, *, invalidate: bool = False) -> None:
        connection = self._connections.pop(location, None)
        self._prepared.discard(location)
        if connection is None:
            return
        if invalidate:
            connection.invalidate()
        self._connection_pool.release(connection)

    async def _ensure_node_connection(
        self, node: ClusterNodeLocation, *, asking: bool = False
    ) -> ClusterConnection:
        location = TCPLocation(node.host, node.port)
        connection = self._connections.get(location)
        if connection is None or not connection.usable:
            if connection is not None:
                self._drop_connection(location, invalidate=True)
            connection = await self._connection_pool.get_connection(node=node)
            self._connections[location] = connection
            self._prepared.discard(location)
        try:
            if location not in self._prepared:
                await connection.create_request(
                    CommandName.HIMPORT_PREPARE,
                    self.fieldset,
                    *self.fields,
                    decode=False,
                    disconnect_on_cancellation=True,
                )
                self._prepared.add(location)
            if asking:
                await connection.create_request(
                    CommandName.ASKING,
                    decode=False,
                    disconnect_on_cancellation=True,
                )
        except _TRANSPORT_ERRORS:
            self._drop_connection(location, invalidate=True)
            raise
        except get_cancelled_exc_class():
            self._drop_connection(location, invalidate=True)
            raise
        return connection

    async def _ask_set(self, error: AskError, key: KeyT, values: tuple[ValueT, ...]) -> None:
        node = self._redirect_node(error.host, error.port)
        location = TCPLocation(node.host, node.port)
        try:
            connection = await self._ensure_node_connection(node, asking=True)
            request = self._set_command(key, values)
            await connection.create_request(
                request.name,
                *request.serialized_arguments,
                decode=False,
                disconnect_on_cancellation=True,
            )
        except _TRANSPORT_ERRORS:
            self._drop_connection(location, invalidate=True)
            raise
        except get_cancelled_exc_class():
            self._drop_connection(location, invalidate=True)
            raise

    async def _flush_node(self, node: ClusterNodeLocation, rows: list[_Row]) -> list[_Row]:
        location = TCPLocation(node.host, node.port)
        try:
            connection = await self._ensure_node_connection(node)
            batch = await connection.create_request_batch(
                [self._set_command(key, values) for key, values in rows]
            )
        except _TRANSPORT_ERRORS:
            self._drop_connection(location, invalidate=True)
            return list(rows)
        except get_cancelled_exc_class():
            self._drop_connection(location, invalidate=True)
            raise

        redirects: list[_Row] = []
        for idx, ((key, values), result) in enumerate(zip(rows, batch, strict=True)):
            if not isinstance(result, BaseException):
                continue
            if isinstance(result, MovedError):
                self._connection_pool.cluster_layout.report_errors(node, result)
                redirects.append((key, values))
            elif isinstance(result, AskError):
                await self._ask_set(result, key, values)
            elif isinstance(result, TryAgainError):
                redirects.append((key, values))
            elif isinstance(result, _TRANSPORT_ERRORS):
                self._drop_connection(location, invalidate=True)
                redirects.extend(rows[idx:])
                break
            else:
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
            if len(eg.exceptions) == 1:
                raise eg.exceptions[0] from None
            raise

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
                if location in self._prepared and connection.usable:
                    try:
                        await connection.create_request(
                            CommandName.HIMPORT_DISCARD,
                            self.fieldset,
                            decode=False,
                            disconnect_on_cancellation=True,
                        )
                    except (*_TRANSPORT_ERRORS, RedisError) as exc:
                        connection.invalidate()
                        if discard_error is None:
                            discard_error = exc
                    except get_cancelled_exc_class() as exc:
                        connection.invalidate()
                        if discard_error is None:
                            discard_error = exc
                        raise
                elif location not in self._prepared and connection.usable:
                    connection.invalidate()
            finally:
                self._connection_pool.release(connection)
        self._connections = {}
        self._prepared = set()
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
