from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from anyio import Event, move_on_after
from exceptiongroup import BaseExceptionGroup

from coredis.commands.constants import CommandName
from coredis.exceptions import ConnectionError, MovedError, ResponseError
from coredis.patterns.himport import ClusterHashImport, HashImport
from coredis.typing import Key, TypeAdapter


def _client(pool: Any) -> MagicMock:
    client = MagicMock()
    client.connection_pool = pool
    client.type_adapter = TypeAdapter()
    return client


def _awaitable(value: Any = b"OK"):
    async def _coro() -> Any:
        return value

    return _coro()


def _awaitable_error(exc: BaseException):
    async def _coro() -> Any:
        raise exc

    return _coro()


class RecordingConnection:
    """Minimal connection stand-in for session lifecycle tests."""

    def __init__(self) -> None:
        self.usable = True
        self.invalidated = False
        self.commands: list[tuple[bytes, tuple[Any, ...]]] = []
        self.prepare_error: BaseException | None = None
        self.batch_results: list[Any] | BaseException | None = None
        self.discard_error: BaseException | None = None
        #: When set, PREPARE awaits this event (for cancel tests).
        self.prepare_gate: Event | None = None
        #: When set, DISCARD awaits this event (for cancel-during-cleanup tests).
        self.discard_gate: Event | None = None
        self.last_request_kwargs: dict[str, Any] = {}

    def invalidate(self, reason: str | None = None) -> None:
        self.invalidated = True
        self.usable = False

    def create_request(self, command: bytes, *args: Any, **kwargs: Any) -> Any:
        self.commands.append((command, args))
        self.last_request_kwargs = dict(kwargs)
        if command == CommandName.HIMPORT_PREPARE:
            if self.prepare_gate is not None:

                async def _hang_prepare() -> Any:
                    await self.prepare_gate.wait()
                    if self.prepare_error is not None:
                        raise self.prepare_error
                    return b"OK"

                return _hang_prepare()
            if self.prepare_error is not None:
                return _awaitable_error(self.prepare_error)
        if command == CommandName.HIMPORT_DISCARD:
            if self.discard_gate is not None:

                async def _hang_discard() -> Any:
                    await self.discard_gate.wait()
                    if self.discard_error is not None:
                        raise self.discard_error
                    return b"OK"

                return _hang_discard()
            if self.discard_error is not None:
                return _awaitable_error(self.discard_error)
        return _awaitable(b"OK")

    def create_request_batch(self, commands: list[Any], timeout: float | None = None) -> Any:
        names = tuple(cmd.name for cmd in commands)
        self.commands.append((b"BATCH", names))
        if isinstance(self.batch_results, BaseException):
            return _awaitable_error(self.batch_results)
        if self.batch_results is not None:
            return _awaitable(list(self.batch_results))
        return _awaitable([b"OK"] * len(commands))


def _assert_not_returned_usable(conn: RecordingConnection, released: list[Any]) -> None:
    """A fieldset-bearing socket must not re-enter the pool as usable."""
    assert conn.invalidated or not conn.usable
    for item in released:
        c = item[0] if isinstance(item, tuple) else item
        if c is conn:
            assert not c.usable


@pytest.mark.anyio
async def test_empty_session_leases_no_connection() -> None:
    pool = MagicMock()
    pool.get_connection = AsyncMock()
    pool.release = MagicMock()

    async with HashImport(_client(pool), "fs", ["name"]):
        pass

    pool.get_connection.assert_not_called()
    pool.release.assert_not_called()


@pytest.mark.anyio
async def test_queue_only_until_flush_leases_once() -> None:
    conn = RecordingConnection()
    pool = MagicMock()
    pool.get_connection = AsyncMock(return_value=conn)
    pool.release = MagicMock()

    async with HashImport(_client(pool), "fs", ["name", "email"]) as himport:
        himport.add("u:1", ["alice", "a@example.com"])
        pool.get_connection.assert_not_called()
        await himport.flush()
        pool.get_connection.assert_awaited_once()
        himport.add("u:2", ["bob", "b@example.com"])
        await himport.flush()
        assert pool.get_connection.await_count == 1

    pool.release.assert_called_once_with(conn)
    prepare = [c for c in conn.commands if c[0] == CommandName.HIMPORT_PREPARE]
    discard = [c for c in conn.commands if c[0] == CommandName.HIMPORT_DISCARD]
    assert len(prepare) == 1
    assert len(discard) == 1


@pytest.mark.anyio
async def test_prepare_timeout_invalidates_and_releases() -> None:
    conn = RecordingConnection()
    conn.prepare_error = TimeoutError("prepare timed out")
    pool = MagicMock()
    pool.get_connection = AsyncMock(return_value=conn)
    pool.release = MagicMock()

    with pytest.raises(TimeoutError, match="prepare"):
        async with HashImport(_client(pool), "fs", ["name"]) as himport:
            himport.add("u:1", ["alice"])
            await himport.flush()

    assert conn.invalidated
    # kill on timeout + shutdown may release again if connection already None
    assert pool.release.call_count >= 1
    assert all(c[0] != CommandName.HIMPORT_DISCARD for c in conn.commands)


@pytest.mark.anyio
async def test_batch_connection_error_invalidates() -> None:
    conn = RecordingConnection()
    conn.batch_results = ConnectionError("socket dead")
    pool = MagicMock()
    pool.get_connection = AsyncMock(return_value=conn)
    pool.release = MagicMock()

    with pytest.raises(ConnectionError, match="socket dead"):
        async with HashImport(_client(pool), "fs", ["name"]) as himport:
            himport.add("u:1", ["alice"])
            await himport.flush()

    assert conn.invalidated
    assert pool.release.call_count >= 1


@pytest.mark.anyio
async def test_abort_without_flush_writes_nothing_and_needs_no_discard() -> None:
    pool = MagicMock()
    pool.get_connection = AsyncMock()
    pool.release = MagicMock()

    with pytest.raises(RuntimeError, match="stop"):
        async with HashImport(_client(pool), "fs", ["name"]) as himport:
            himport.add("u:1", ["alice"])
            raise RuntimeError("stop")

    pool.get_connection.assert_not_called()


@pytest.mark.anyio
async def test_cluster_moved_reprepares_on_destination() -> None:
    from coredis.cluster._node import ClusterNodeLocation

    node_a = ClusterNodeLocation("127.0.0.1", 7000, server_type="primary")
    node_b = ClusterNodeLocation("127.0.0.1", 7001, server_type="primary")
    key = "user:{a}"
    slot = Key(key).slot

    layout = MagicMock()
    # First grouping sends to A; after MOVED report, grouping sends to B.
    layout.node_for_slot = MagicMock(side_effect=[node_a, node_b, node_b])
    layout.report_errors = MagicMock()
    layout.node_for_location = MagicMock(return_value=node_b)

    conn_a = RecordingConnection()
    conn_b = RecordingConnection()
    conn_a.batch_results = [MovedError(f"{slot} 127.0.0.1:7001")]
    conn_b.batch_results = [b"OK"]

    pool = MagicMock()
    pool.cluster_layout = layout

    async def get_connection(*, node=None, **kwargs):
        if node is node_a or (node and node.port == 7000):
            return conn_a
        return conn_b

    pool.get_connection = AsyncMock(side_effect=get_connection)
    pool.release = MagicMock()

    async with ClusterHashImport(_client(pool), "fs", ["name"]) as himport:
        himport.add(key, ["alice"])
        await himport.flush()

    layout.report_errors.assert_called()
    assert any(c[0] == CommandName.HIMPORT_PREPARE for c in conn_a.commands)
    assert any(c[0] == CommandName.HIMPORT_PREPARE for c in conn_b.commands)
    assert any(c[0] == b"BATCH" for c in conn_b.commands)
    # Both node connections discarded on exit.
    assert any(c[0] == CommandName.HIMPORT_DISCARD for c in conn_a.commands)
    assert any(c[0] == CommandName.HIMPORT_DISCARD for c in conn_b.commands)
    assert pool.release.call_count == 2


@pytest.mark.anyio
async def test_cluster_multi_node_flush_uses_task_group(monkeypatch: pytest.MonkeyPatch) -> None:
    import coredis.patterns.himport as himport_mod
    from coredis.cluster._node import ClusterNodeLocation

    node_a = ClusterNodeLocation("127.0.0.1", 7000, server_type="primary")
    node_b = ClusterNodeLocation("127.0.0.1", 7001, server_type="primary")

    layout = MagicMock()

    def node_for_slot(slot: int, primary: bool = True) -> ClusterNodeLocation:
        # Split two distinct hash tags across nodes.
        return node_a if slot == Key("u:{a}").slot else node_b

    layout.node_for_slot = MagicMock(side_effect=node_for_slot)

    conn_a = RecordingConnection()
    conn_b = RecordingConnection()
    pool = MagicMock()
    pool.cluster_layout = layout

    async def get_connection(*, node=None, **kwargs):
        return conn_a if node is node_a or (node and node.port == 7000) else conn_b

    pool.get_connection = AsyncMock(side_effect=get_connection)
    pool.release = MagicMock()

    seen: list[int] = []
    real_tg = himport_mod.create_task_group

    def tracking_tg():
        seen.append(1)
        return real_tg()

    monkeypatch.setattr(himport_mod, "create_task_group", tracking_tg)

    async with ClusterHashImport(_client(pool), "fs", ["name"]) as himport:
        himport.add("u:{a}", ["alice"])
        himport.add("u:{b}", ["bob"])
        await himport.flush()

    assert seen == [1]
    assert any(c[0] == CommandName.HIMPORT_PREPARE for c in conn_a.commands)
    assert any(c[0] == CommandName.HIMPORT_PREPARE for c in conn_b.commands)


@pytest.mark.anyio
async def test_cluster_hard_response_error_propagates() -> None:
    from coredis.cluster._node import ClusterNodeLocation

    node = ClusterNodeLocation("127.0.0.1", 7000, server_type="primary")
    layout = MagicMock()
    layout.node_for_slot = MagicMock(return_value=node)

    conn = RecordingConnection()
    conn.batch_results = [ResponseError("ERR bad")]
    pool = MagicMock()
    pool.cluster_layout = layout
    pool.get_connection = AsyncMock(return_value=conn)
    pool.release = MagicMock()

    with pytest.raises(ResponseError, match="bad"):
        async with ClusterHashImport(_client(pool), "fs", ["name"]) as himport:
            himport.add("u:1", ["alice"])
            await himport.flush()


@pytest.mark.anyio
async def test_cluster_pipeline_refuses_himport_prepare() -> None:
    from coredis.patterns.pipeline import ClusterPipeline

    client = MagicMock()
    client.type_adapter = MagicMock()
    client.connection_pool = MagicMock()
    pipe = ClusterPipeline(client)
    with pytest.raises(NotImplementedError, match="himport|HIMPORT|standalone"):
        pipe.himport_prepare("fs", ["name"])


@pytest.mark.anyio
async def test_cancel_during_prepare_invalidates_and_does_not_return_usable() -> None:
    conn = RecordingConnection()
    conn.prepare_gate = Event()  # never opened — cancel via move_on_after
    released: list[RecordingConnection] = []
    pool = MagicMock()
    pool.get_connection = AsyncMock(return_value=conn)
    pool.release = MagicMock(side_effect=lambda c: released.append(c))

    with move_on_after(0.15) as scope:
        async with HashImport(_client(pool), "fs", ["name"]) as himport:
            himport.add("u:1", ["alice"])
            await himport.flush()

    assert scope.cancel_called
    assert any(c[0] == CommandName.HIMPORT_PREPARE for c in conn.commands)
    _assert_not_returned_usable(conn, released)
    assert not any(c[0] == CommandName.HIMPORT_DISCARD for c in conn.commands)
    assert conn.last_request_kwargs.get("disconnect_on_cancellation") is True


@pytest.mark.anyio
async def test_cancel_after_prepare_still_discards_under_shield() -> None:
    conn = RecordingConnection()
    body_gate = Event()  # never opened
    released: list[RecordingConnection] = []
    pool = MagicMock()
    pool.get_connection = AsyncMock(return_value=conn)
    pool.release = MagicMock(side_effect=lambda c: released.append(c))

    with move_on_after(0.15) as scope:
        async with HashImport(_client(pool), "fs", ["name"]) as himport:
            himport.add("u:1", ["alice"])
            await himport.flush()
            await body_gate.wait()

    assert scope.cancel_called
    # Shielded shutdown must still DISCARD a prepared fieldset.
    assert any(c[0] == CommandName.HIMPORT_DISCARD for c in conn.commands)
    assert released
    assert conn is released[-1]


@pytest.mark.anyio
async def test_cluster_sibling_cancel_mid_prepare_invalidates_connection() -> None:
    from coredis.cluster._node import ClusterNodeLocation

    node_a = ClusterNodeLocation("127.0.0.1", 7000, server_type="primary")
    node_b = ClusterNodeLocation("127.0.0.1", 7001, server_type="primary")

    layout = MagicMock()

    def node_for_slot(slot: int, primary: bool = True) -> ClusterNodeLocation:
        return node_a if slot == Key("u:{a}").slot else node_b

    layout.node_for_slot = MagicMock(side_effect=node_for_slot)

    conn_a = RecordingConnection()
    conn_a.prepare_gate = Event()  # hang until sibling failure cancels
    conn_b = RecordingConnection()
    conn_b.batch_results = [ResponseError("ERR peer failed")]

    released: list[tuple[RecordingConnection, bool, bool]] = []
    pool = MagicMock()
    pool.cluster_layout = layout

    async def get_connection(*, node=None, **kwargs):
        if node is node_a or (node and node.port == 7000):
            return conn_a
        return conn_b

    pool.get_connection = AsyncMock(side_effect=get_connection)

    def release(c: RecordingConnection) -> None:
        released.append((c, c.usable, c.invalidated))

    pool.release = release

    with pytest.raises((ResponseError, BaseExceptionGroup)):
        async with ClusterHashImport(_client(pool), "fs", ["name"]) as himport:
            himport.add("u:{a}", ["alice"])
            himport.add("u:{b}", ["bob"])
            await himport.flush()

    # Hanging prepare on A must not return a usable fieldset socket.
    assert conn_a.invalidated or not conn_a.usable
    for c, usable, invalidated in released:
        if c is conn_a:
            assert invalidated or not usable
