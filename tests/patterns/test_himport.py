from __future__ import annotations

import contextlib

import pytest

from coredis import RedisCluster
from coredis._utils import hash_slot
from coredis.commands._validators import (
    MutuallyExclusiveParametersError,
    RequiredParameterError,
)
from coredis.exceptions import ClusterError, ConnectionError, ResponseError, WrongTypeError
from coredis.typing import Key
from tests.conftest import targets


@targets(
    "redis_basic",
    "redis_cluster",
)
@pytest.mark.min_server_version("8.10.0")
class TestClientHashImport:
    async def test_add_and_commit(self, client, _s):
        async with client.himport("fs", ["name", "email", "age"]) as himport:
            himport.add("u:1", ["alice", "alice@example.com", 30])
            himport.add("u:2", ["bob", "bob@example.com", 41])
        assert await client.hgetall("u:1") == {
            _s("name"): _s("alice"),
            _s("email"): _s("alice@example.com"),
            _s("age"): _s("30"),
        }
        assert await client.hgetall("u:2") == {
            _s("name"): _s("bob"),
            _s("email"): _s("bob@example.com"),
            _s("age"): _s("41"),
        }

    async def test_add_mapping_writes_field_values(self, client, _s):
        async with client.himport("fs", ["email", "name"]) as himport:
            himport.add("u:map", {"name": "alice", "email": "a@example.com"})
            himport.add("u:map-enc", {_s("name"): "carol", _s("email"): "c@example.com"})
        assert await client.hgetall("u:map") == {
            _s("name"): _s("alice"),
            _s("email"): _s("a@example.com"),
        }
        assert await client.hgetall("u:map-enc") == {
            _s("name"): _s("carol"),
            _s("email"): _s("c@example.com"),
        }

    async def test_add_keywords_writes_field_values(self, client, _s):
        async with client.himport("fs", ["email", "name"]) as himport:
            himport.add("u:kw", name="alice", email="a@example.com")
        assert await client.hgetall("u:kw") == {
            _s("name"): _s("alice"),
            _s("email"): _s("a@example.com"),
        }

    async def test_add_mapping_rejects_missing_or_extra_fields(self, client):
        async with client.himport("fs", ["name", "email"]) as himport:
            with pytest.raises(ValueError, match="missing"):
                himport.add("u:1", {"name": "alice"})
            with pytest.raises(ValueError, match="extra"):
                himport.add("u:1", {"name": "alice", "email": "a@example.com", "age": 1})

    async def test_add_rejects_values_and_keywords_together(self, client):
        async with client.himport("fs", ["name"]) as himport:
            with pytest.raises(MutuallyExclusiveParametersError):
                himport.add("u:1", ["alice"], name="alice")

    async def test_add_requires_values(self, client):
        async with client.himport("fs", ["name"]) as himport:
            with pytest.raises(RequiredParameterError):
                himport.add("u:1")

    async def test_value_count_mismatch(self, client):
        async with client.himport("fs", ["name", "email"]) as himport:
            with pytest.raises(ValueError, match="expected 2 values, got 1"):
                himport.add("u:1", ["alice"])

    async def test_empty_fields(self, client):
        with pytest.raises(ValueError, match="non-empty"):
            client.himport("fs", [])

    async def test_empty_session_is_noop(self, client, _s):
        leased_before = client.connection_pool.statistics.in_use_connections
        async with client.himport("fs", ["name"]):
            pass
        assert await client.exists(["u:1"]) == 0
        assert client.connection_pool.statistics.in_use_connections <= leased_before

    async def test_flush_writes_before_exit(self, client, _s):
        async with client.himport("fs", ["name"]) as himport:
            himport.add("u:1", ["alice"])
            assert await client.exists(["u:1"]) == 0
            await himport.flush()
            assert await client.hget("u:1", "name") == _s("alice")
            himport.add("u:2", ["bob"])
        assert await client.hget("u:2", "name") == _s("bob")

    async def test_flush_requires_context(self, client):
        himport = client.himport("fs", ["name"])
        himport.add("u:1", ["alice"])
        with pytest.raises(RuntimeError, match="context manager"):
            await himport.flush()

    async def test_aborts_on_body_error(self, client, _s):
        with pytest.raises(RuntimeError, match="stop"):
            async with client.himport("fs", ["name"]) as himport:
                himport.add("u:1", ["alice"])
                raise RuntimeError("stop")
        assert await client.exists(["u:1"]) == 0

    async def test_abort_after_flush_keeps_written_rows(self, client, _s):
        with pytest.raises(RuntimeError, match="stop"):
            async with client.himport("fs", ["name"]) as himport:
                himport.add("u:1", ["alice"])
                await himport.flush()
                himport.add("u:2", ["bob"])
                raise RuntimeError("stop")
        assert await client.hget("u:1", "name") == _s("alice")
        assert await client.exists(["u:2"]) == 0

    async def test_abort_does_not_leak_fieldset(self, client, _s):
        with pytest.raises(RuntimeError, match="stop"):
            async with client.himport("fs", ["name"]) as himport:
                himport.add("u:1", ["alice"])
                raise RuntimeError("stop")
        async with client.himport("fs", ["name", "email"]) as himport:
            himport.add("u:2", ["bob", "b@example.com"])
        assert await client.hgetall("u:2") == {
            _s("name"): _s("bob"),
            _s("email"): _s("b@example.com"),
        }
        assert await client.exists(["u:1"]) == 0

    async def test_abort_clears_queue_for_reuse(self, client, _s):
        himport = client.himport("fs", ["name"])
        with pytest.raises(RuntimeError, match="stop"):
            async with himport:
                himport.add("u:1", ["alice"])
                raise RuntimeError("stop")
        async with himport:
            himport.add("u:2", ["bob"])
        assert await client.exists(["u:1"]) == 0
        assert await client.hget("u:2", "name") == _s("bob")

    async def test_overwrite_replaces_whole_hash(self, client, _s):
        await client.hset("u:ow", {"extra": "keep-me", "name": "old"})
        async with client.himport("fs", ["name", "email"]) as himport:
            himport.add("u:ow", ["alice", "a@example.com"])
        assert await client.hgetall("u:ow") == {
            _s("name"): _s("alice"),
            _s("email"): _s("a@example.com"),
        }

    async def test_different_slots(self, client, _s):
        async with client.himport("fs", ["name"]) as himport:
            himport.add("u:{a}", ["alice"])
            himport.add("u:{b}", ["bob"])
        assert await client.hget("u:{a}", "name") == _s("alice")
        assert await client.hget("u:{b}", "name") == _s("bob")

    async def test_wrongtype_names_key(self, client, _s):
        await client.set("u:str", "not-a-hash")
        with pytest.raises((ResponseError, WrongTypeError), match="u:str"):
            async with client.himport("fs", ["name"]) as himport:
                himport.add("u:str", ["alice"])

    async def test_duplicate_prepare_fields_name_fieldset(self, client):
        with pytest.raises(ResponseError, match=r"fieldset 'fs'"):
            async with client.himport("fs", ["name", "name"]) as himport:
                himport.add("u:1", ["alice", "alice"])

    async def test_imported_hash_uses_template_encoding(self, client, _s):
        async with client.himport("fs", ["name", "email"]) as himport:
            himport.add("u:enc", ["alice", "a@example.com"])
        assert await client.object_encoding("u:enc") == _s("template-listpack")

    async def _kill_held(self, client, himport):
        if (connection := himport._connection) is not None:
            assert connection.client_id is not None
            await client.client_kill(identifier=connection.client_id)
            return
        location, connection = next(iter(himport._connections.items()))
        assert connection.client_id is not None
        node = client.connection_pool.cluster_layout.node_for_location(location)
        assert node is not None
        async with node.as_client(**client.connection_pool.connection_kwargs) as node_client:
            await node_client.client_kill(identifier=connection.client_id)

    @pytest.mark.nocluster
    async def test_connection_loss_fails_write(self, client):
        with pytest.raises(ConnectionError):
            async with client.himport("fs", ["name"]) as himport:
                himport.add("u:kill-w", ["alice"])
                await himport.flush()
                await self._kill_held(client, himport)
                himport.add("u:kill-w2", ["bob"])

    @pytest.mark.clusteronly
    async def test_reprepares_after_dropped_node_connection(self, client, _s):
        async with client.himport("fs", ["name"]) as himport:
            himport.add("hi:{k}1", ["alice"])
            await himport.flush()
            await self._kill_held(client, himport)
            himport.add("hi:{k}2", ["bob"])
        assert await client.hget("hi:{k}1", "name") == _s("alice")
        assert await client.hget("hi:{k}2", "name") == _s("bob")

    @pytest.mark.clusteronly
    async def test_ask_writes_on_importing_node(self, client, _s):
        key = "hi:ask"
        slot = hash_slot(b"hi:ask")
        layout = client.connection_pool.cluster_layout
        source = layout.node_for_slot(slot, primary=True)
        dest = next(
            node
            for node in layout.primaries
            if (node.host, node.port) != (source.host, source.port)
        )
        kwargs = client.connection_pool.connection_kwargs
        try:
            async with source.as_client(**kwargs) as src:
                await src.cluster_setslot(slot, migrating=dest.node_id)
            async with dest.as_client(**kwargs) as dst:
                await dst.cluster_setslot(slot, importing=source.node_id)
            async with client.himport("fs", ["name"]) as himport:
                himport.add(key, ["alice"])
            assert await client.hget(key, "name") == _s("alice")
        finally:
            with contextlib.suppress(Exception):
                await client.delete([key])
            async with dest.as_client(**kwargs) as dst:
                with contextlib.suppress(Exception):
                    await dst.cluster_setslot(slot, stable=True)
            async with source.as_client(**kwargs) as src:
                with contextlib.suppress(Exception):
                    await src.cluster_setslot(slot, stable=True)

    @pytest.mark.clusteronly
    async def test_moved_follows_new_slot_owner(self, client, _s):
        key = "hi:moved"
        slot = hash_slot(b"hi:moved")
        layout = client.connection_pool.cluster_layout
        source = layout.node_for_slot(slot, primary=True)
        dest = next(
            node
            for node in layout.primaries
            if (node.host, node.port) != (source.host, source.port)
        )
        kwargs = client.connection_pool.connection_kwargs
        try:
            for node in layout.primaries:
                async with node.as_client(**kwargs) as node_client:
                    await node_client.cluster_setslot(slot, node=dest.node_id)
            async with client.himport("fs", ["name"]) as himport:
                himport.add(key, ["alice"])
            assert await client.hget(key, "name") == _s("alice")
        finally:
            with contextlib.suppress(Exception):
                await client.delete([key])
            for node in layout.primaries:
                async with node.as_client(**kwargs) as node_client:
                    with contextlib.suppress(Exception):
                        await node_client.cluster_setslot(slot, node=source.node_id)
            await layout._refresh()

    async def _timed_client(self, client, cloner, stream_timeout=0.08):
        if isinstance(client, RedisCluster):
            timed = await cloner(client, stream_timeout=stream_timeout)
        else:
            timed = await cloner(client, connection_kwargs={"stream_timeout": stream_timeout})
        return timed

    async def _pause(self, client, key: str, ms: int = 400) -> None:
        if isinstance(client, RedisCluster):
            node = client.connection_pool.cluster_layout.node_for_slot(
                hash_slot(key.encode()), primary=True
            )
            async with node.as_client(**client.connection_pool.connection_kwargs) as node_client:
                await node_client.client_pause(ms)
            return
        await client.client_pause(ms)

    async def _unpause(self, client) -> None:
        if isinstance(client, RedisCluster):
            for node in client.connection_pool.cluster_layout.primaries:
                async with node.as_client(
                    **client.connection_pool.connection_kwargs
                ) as node_client:
                    with contextlib.suppress(Exception):
                        await node_client.client_unpause()
            return
        with contextlib.suppress(Exception):
            await client.client_unpause()

    async def test_discard_times_out_after_flush(self, client, cloner):
        timed = await self._timed_client(client, cloner)
        try:
            async with timed:
                await timed.ping()
                with pytest.raises(TimeoutError):
                    async with timed.himport("fs", ["name"]) as himport:
                        himport.add("hi:dto", ["alice"])
                        await himport.flush()
                        await self._pause(client, "hi:dto")
        finally:
            await self._unpause(client)

    async def test_prepare_times_out(self, client, cloner):
        timed = await self._timed_client(client, cloner)
        try:
            async with timed:
                await timed.ping()
                await self._pause(client, "hi:pto")
                with pytest.raises(TimeoutError):
                    async with timed.himport("fs", ["name"]) as himport:
                        himport.add("hi:pto", ["alice"])
        finally:
            await self._unpause(client)

    @pytest.mark.nocluster
    async def test_set_times_out_after_prepare(self, client, cloner):
        timed = await self._timed_client(client, cloner)
        try:
            async with timed:
                await timed.ping()
                with pytest.raises(TimeoutError):
                    async with timed.himport("fs", ["name"]) as himport:
                        himport.add("hi:{sto}1", ["alice"])
                        await himport.flush()
                        himport.add("hi:{sto}2", ["bob"])
                        await self._pause(client, "hi:{sto}2")
                        await himport.flush()
        finally:
            await self._unpause(client)

    @pytest.mark.clusteronly
    async def test_wrongtype_on_two_primaries(self, client):
        layout = client.connection_pool.cluster_layout
        keys: dict[tuple[str, int], str] = {}
        index = 0
        while len(keys) < 2:
            key = f"hi:wt:{index}"
            node = layout.node_for_slot(Key(key).slot, primary=True)
            keys.setdefault((node.host, node.port), key)
            index += 1
        first, second = keys.values()
        await client.set(first, "x")
        await client.set(second, "y")
        with pytest.raises((ResponseError, WrongTypeError)):
            async with client.himport("fs", ["name"]) as himport:
                himport.add(first, ["a"])
                himport.add(second, ["b"])

    @pytest.mark.clusteronly
    async def test_redirect_retries_exhausted(self, client):
        original = RedisCluster.MAX_RETRIES
        RedisCluster.MAX_RETRIES = 0
        try:
            with pytest.raises(ClusterError, match="redirect retries"):
                async with client.himport("fs", ["name"]) as himport:
                    himport.add("hi:retry", ["alice"])
        finally:
            RedisCluster.MAX_RETRIES = original
