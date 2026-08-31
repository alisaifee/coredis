from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Any

from anyio import AsyncContextManagerMixin
from deprecated.sphinx import versionadded

from coredis._concurrency import gather
from coredis._telemetry import (
    TelemetryAttributeProvider,
    TelemetryProvider,
    get_telemetry_provider,
)
from coredis._utils import EncodingInsensitiveDict, nativestr
from coredis.client import Client, RedisCluster
from coredis.cluster._node import ClusterNodeLocation
from coredis.commands._validators import mutually_exclusive_parameters
from coredis.commands.constants import CommandName
from coredis.commands.request import CommandRequest
from coredis.connection import TCPLocation
from coredis.connection._base import BaseConnection
from coredis.connection._request import Request
from coredis.exceptions import (
    AskError,
    ConnectionError,
    MovedError,
    RedisError,
    TryAgainError,
)
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

REDIRECTS = (MovedError, AskError, TryAgainError)


@versionadded(version="6.9.0")
class HashImport(AsyncContextManagerMixin, TelemetryAttributeProvider, Generic[AnyStr]):
    """
    Write many hashes that share the same field names on a single Redis instance.

    Queue hashes with :meth:`add` and use as an async context manager. Queued
    hashes are written when the context exits, or earlier by calling :meth:`flush`.
    Each write borrows a connection, prepares the fieldset, sends the sets,
    and discards the fieldset.
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
        self._hashes: list[tuple[KeyT, tuple[ValueT, ...]]] = []
        self._active = False

    @mutually_exclusive_parameters("values", "fields", required=True)
    def add(
        self,
        key: KeyT,
        values: Parameters[ValueT] | Mapping[StringT, ValueT] | None = None,
        **fields: ValueT,
    ) -> None:
        """
        Queue a hash to be written on the next flush or context exit.

        :param key: hash key
        :param values: a list of values in field order, or a mapping keyed by
         field name. Omit to pass field names as keyword arguments instead.
        :return: ``None``
        """
        self._hashes.append((key, self._values(values if values is not None else fields)))

    def _values(self, values: Parameters[ValueT] | Mapping[Any, ValueT]) -> tuple[ValueT, ...]:
        if not isinstance(values, Mapping):
            if len(values := tuple(values)) != len(self.fields):
                raise ValueError(f"expected {len(self.fields)} values, got {len(values)}")
            return values
        provided = EncodingInsensitiveDict(dict(values))
        wanted = {nativestr(name) for name in self.fields}
        got = {nativestr(name) for name in values}
        if wanted != got:
            raise ValueError(f"missing {wanted - got}, extra {got - wanted}")
        return tuple(provided[name] for name in self.fields)

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
        exc.args = (f"HIMPORT ({'; '.join(parts)}): {exc.args[0]}", *exc.args[1:])

    def telemetry_attributes(self, provider: TelemetryProvider) -> dict[str, str | int]:
        return {
            "db.operation.name": "HIMPORT",
            "db.collection.name": nativestr(self.fieldset),
            "himport.field.count": len(self.fields),
        }

    async def flush(self) -> None:
        """
        Write queued hashes. Safe to call more than once.

        :return: ``None``. An empty queue is a no-op.
        """
        if not self._active:
            raise RuntimeError(f"{type(self).__name__} must be used as an async context manager")
        if not self._hashes:
            return
        await self._write(list(self._hashes))
        self._hashes.clear()

    def _set_command(self, key: KeyT, values: tuple[ValueT, ...]) -> CommandRequest[ResponseType]:
        return CommandRequest(
            CommandName.HIMPORT_SET,
            Key(key),
            self.fieldset,
            *values,
            execution_parameters={},
            type_adapter=self.client.type_adapter,
        )

    def _send_set(
        self, connection: BaseConnection, key: KeyT, values: tuple[ValueT, ...]
    ) -> Request:
        command = self._set_command(key, values)
        return connection.create_request(
            command.name,
            *command.serialized_arguments,
            decode=False,
            disconnect_on_cancellation=True,
        )

    @asynccontextmanager
    async def _prepared(
        self, *, node: ClusterNodeLocation | None = None
    ) -> AsyncGenerator[BaseConnection]:
        async with self.client.connection_pool.acquire(node=node) as connection:
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
                connection.invalidate()
                raise
            try:
                yield connection
            finally:
                if connection.usable:
                    try:
                        await connection.create_request(
                            CommandName.HIMPORT_DISCARD,
                            self.fieldset,
                            decode=False,
                            disconnect_on_cancellation=True,
                        )
                    except (ConnectionError, TimeoutError, RedisError):
                        connection.invalidate()
                        raise

    async def _write(self, hashes: list[tuple[KeyT, tuple[ValueT, ...]]]) -> None:
        with get_telemetry_provider().start_span(
            [self._set_command(key, values) for key, values in hashes],
            self.client.connection_pool,
            self,
            name="HIMPORT",
        ):
            async with self._prepared() as connection:
                for key, values in hashes:
                    try:
                        await self._send_set(connection, key, values)
                    except RedisError as exc:
                        self._annotate(exc, key=key)
                        raise

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        self._active = True
        try:
            yield self
            await self.flush()
        finally:
            self._active = False
            self._hashes.clear()


@versionadded(version="6.9.0")
class ClusterHashImport(HashImport[AnyStr]):
    """
    Write many hashes that share the same field names across a Redis Cluster.

    Works like :class:`HashImport` but writes each cluster primary in its own
    prepared context.
    """

    client: RedisCluster[AnyStr]

    async def _write_node(
        self, node: ClusterNodeLocation, hashes: list[tuple[KeyT, tuple[ValueT, ...]]]
    ) -> tuple[
        list[tuple[KeyT, tuple[ValueT, ...], MovedError | AskError | TryAgainError]],
        BaseException | None,
    ]:
        redirects: list[tuple[KeyT, tuple[ValueT, ...], MovedError | AskError | TryAgainError]] = []
        error: BaseException | None = None
        try:
            async with self._prepared(node=node) as connection:
                for key, values in hashes:
                    try:
                        await self._send_set(connection, key, values)
                    except REDIRECTS as exc:
                        redirects.append((key, values, exc))
                    except RedisError as exc:
                        self._annotate(exc, key=key, node=node)
                        error = error or exc
        except (ConnectionError, TimeoutError) as exc:
            self.client.connection_pool.cluster_layout.report_errors(node, exc)
            error = error or exc
        except RedisError as exc:
            error = error or exc
        return redirects, error

    async def _retry_set(
        self,
        node: ClusterNodeLocation,
        key: KeyT,
        values: tuple[ValueT, ...],
        exc: MovedError | AskError | TryAgainError,
    ) -> None:
        dest = node
        asking = False
        if isinstance(exc, MovedError):
            self.client.connection_pool.cluster_layout.report_errors(node, exc)
        if isinstance(exc, AskError):
            dest = self.client.connection_pool.cluster_layout.node_for_location(
                TCPLocation(exc.host, exc.port)
            ) or ClusterNodeLocation(exc.host, exc.port, server_type="primary")
            asking = not isinstance(exc, MovedError)
        try:
            async with self._prepared(node=dest) as connection:
                if asking:
                    await connection.create_request(
                        CommandName.ASKING,
                        decode=False,
                        disconnect_on_cancellation=True,
                    )
                await self._send_set(connection, key, values)
        except (ConnectionError, TimeoutError) as err:
            self.client.connection_pool.cluster_layout.report_errors(dest, err)
            raise

    async def _write(self, hashes: list[tuple[KeyT, tuple[ValueT, ...]]]) -> None:
        layout = self.client.connection_pool.cluster_layout
        with get_telemetry_provider().start_span(
            [self._set_command(key, values) for key, values in hashes],
            self.client.connection_pool,
            self,
            name="HIMPORT",
        ):
            grouped: dict[ClusterNodeLocation, list[tuple[KeyT, tuple[ValueT, ...]]]] = defaultdict(
                list
            )
            for key, values in hashes:
                grouped[layout.node_for_slot(Key(key).slot, primary=True)].append((key, values))
            nodes = list(grouped.items())
            results = await gather(
                *[self._write_node(node, node_hashes) for node, node_hashes in nodes]
            )
            error: BaseException | None = None
            for (node, _), (redirects, node_error) in zip(nodes, results, strict=True):
                error = error or node_error
                for key, values, exc in redirects:
                    try:
                        await self._retry_set(node, key, values, exc)
                    except (ConnectionError, TimeoutError, RedisError) as retry_exc:
                        self._annotate(retry_exc, key=key)
                        error = error or retry_exc
            if error is not None:
                raise error
