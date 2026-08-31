from __future__ import annotations

import contextlib

import pytest

import coredis
from coredis import RedisCluster
from coredis._utils import hash_slot
from coredis.commands._validators import (
    MutuallyExclusiveParametersError,
    RequiredParameterError,
)
from coredis.exceptions import ResponseError, WrongTypeError
from coredis.tokens import PureToken
from coredis.typing import Key
from tests.conftest import targets


@pytest.fixture
def keys_on_two_primaries(client):
    layout = client.connection_pool.cluster_layout
    keys: dict[tuple[str, int], str] = {}
    index = 0
    while len(keys) < 2:
        key = f"hi:wt:{index}"
        node = layout.node_for_slot(Key(key).slot, primary=True)
        keys.setdefault((node.host, node.port), key)
        index += 1
    return tuple(keys.values())


@pytest.fixture
def same_node_keys(client):
    if isinstance(client, RedisCluster):
        layout = client.connection_pool.cluster_layout
        node = next(iter(layout.primaries))
        keys: list[str] = []
        index = 0
        while len(keys) < 2:
            key = f"hi:same:{index}"
            owner = layout.node_for_slot(Key(key).slot, primary=True)
            if (owner.host, owner.port) == (node.host, node.port):
                keys.append(key)
            index += 1
        return tuple(keys)
    return "hi:{sto}1", "hi:{sto}2"


@pytest.fixture
async def stale_cluster(client, cloner, mocker):
    cluster_slots = coredis.Redis.cluster_slots

    async def swapped(self, *args, **kwargs):
        values = await cluster_slots(self, *args, **kwargs)
        for nodes in values.values():
            if len(nodes) > 1:
                nodes[0]["port"], nodes[1]["port"] = nodes[1]["port"], nodes[0]["port"]
        return values

    mocker.patch.object(coredis.Redis, "cluster_slots", new=swapped)
    clone = await cloner(client)
    async with clone:
        yield clone


@pytest.fixture
async def timeout_client(client, cloner):
    if isinstance(client, RedisCluster):
        clone = await cloner(client, stream_timeout=0.2)
    else:
        clone = await cloner(client, connection_kwargs={"stream_timeout": 0.2})
    async with clone:
        await clone.ping()
        yield clone


@pytest.fixture
async def client_pause(client):
    async def pause(key: str, ms: int = 400) -> None:
        if isinstance(client, RedisCluster):
            node = client.connection_pool.cluster_layout.node_for_slot(Key(key).slot, primary=True)
            async with node.as_client(**client.connection_pool.connection_kwargs) as node_client:
                await node_client.client_pause(ms)
            return
        await client.client_pause(ms)

    yield pause
    if isinstance(client, RedisCluster):
        for primary in client.primaries:
            async with primary:
                await primary.client_unpause()
        return
    await client.client_unpause()


@pytest.fixture
def drop_other_clients(client):
    async def drop() -> None:
        if isinstance(client, RedisCluster):
            for primary in client.primaries:
                async with primary:
                    await primary.client_kill(type_=PureToken.NORMAL, skipme=True)
            return
        await client.client_kill(type_=PureToken.NORMAL, skipme=True)

    return drop


@pytest.fixture
async def importing_keys(client):
    migrating = "hi:ask"
    layout = client.connection_pool.cluster_layout
    slot = hash_slot(migrating.encode())
    source = layout.node_for_slot(slot, primary=True)
    dest = next(
        node for node in layout.primaries if (node.host, node.port) != (source.host, source.port)
    )
    index = 0
    while True:
        local = f"hi:askd:{index}"
        owner = layout.node_for_slot(Key(local).slot, primary=True)
        if (owner.host, owner.port) == (dest.host, dest.port):
            break
        index += 1
    kwargs = client.connection_pool.connection_kwargs
    async with source.as_client(**kwargs) as src:
        await src.cluster_setslot(slot, migrating=dest.node_id)
    async with dest.as_client(**kwargs) as dst:
        await dst.cluster_setslot(slot, importing=source.node_id)
    try:
        yield migrating, local
    finally:
        with contextlib.suppress(Exception):
            await client.delete([migrating, local])
        async with dest.as_client(**kwargs) as dst:
            with contextlib.suppress(Exception):
                await dst.cluster_setslot(slot, stable=True)
        async with source.as_client(**kwargs) as src:
            with contextlib.suppress(Exception):
                await src.cluster_setslot(slot, stable=True)


@targets(
    "redis_basic",
    "redis_cluster",
)
@pytest.mark.min_server_version("8.10.0")
class TestClientHashImport:
    @pytest.mark.parametrize(
        ("args", "kwargs"),
        [
            ((["a@example.com", "alice"],), {}),
            (({"name": "alice", "email": "a@example.com"},), {}),
            ((), {"name": "alice", "email": "a@example.com"}),
        ],
        ids=["values", "mapping", "keywords"],
    )
    async def test_add_writes_fields(self, client, _s, args, kwargs):
        async with client.himport("fs", ["email", "name"]) as himport:
            himport.add("u:1", *args, **kwargs)
        assert await client.hgetall("u:1") == {
            _s("name"): _s("alice"),
            _s("email"): _s("a@example.com"),
        }

    async def test_add_mapping_encoded_keys(self, client, _s):
        async with client.himport("fs", ["email", "name"]) as himport:
            himport.add("u:enc", {_s("name"): "carol", _s("email"): "c@example.com"})
        assert await client.hgetall("u:enc") == {
            _s("name"): _s("carol"),
            _s("email"): _s("c@example.com"),
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

    async def test_abort_after_flush_keeps_written_hashes(self, client, _s):
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

    async def test_reprepares_after_dropped_connection(self, client, _s, drop_other_clients):
        async with client.himport("fs", ["name"]) as himport:
            himport.add("u:kill-w", ["alice"])
            await himport.flush()
            await drop_other_clients()
            himport.add("u:kill-w2", ["bob"])
        assert await client.hget("u:kill-w", "name") == _s("alice")
        assert await client.hget("u:kill-w2", "name") == _s("bob")

    async def test_prepare_times_out(self, timeout_client, client_pause):
        await client_pause("hi:pto")
        with pytest.raises(TimeoutError):
            async with timeout_client.himport("fs", ["name"]) as himport:
                himport.add("hi:pto", ["alice"])

    async def test_set_times_out_after_prepare(self, timeout_client, client_pause, same_node_keys):
        first, second = same_node_keys
        with pytest.raises(TimeoutError):
            async with timeout_client.himport("fs", ["name"]) as himport:
                himport.add(first, ["alice"])
                await himport.flush()
                himport.add(second, ["bob"])
                await client_pause(second)
                await himport.flush()


@targets("redis_cluster")
@pytest.mark.min_server_version("8.10.0")
class TestClusterHashImport:
    async def test_moved_follows_stale_slot_map(self, client, _s, stale_cluster):
        async with stale_cluster.himport("fs", ["name"]) as himport:
            himport.add("u:{a}", ["alice"])
            himport.add("u:{b}", ["bob"])
        assert await client.hget("u:{a}", "name") == _s("alice")
        assert await client.hget("u:{b}", "name") == _s("bob")

    async def test_ask_writes_on_importing_node(self, client, _s, importing_keys):
        migrating, local = importing_keys
        async with client.himport("fs", ["name"]) as himport:
            himport.add(migrating, ["alice"])
            himport.add(local, ["bob"])
        assert await client.hget(migrating, "name") == _s("alice")
        assert await client.hget(local, "name") == _s("bob")

    async def test_wrongtype_on_two_primaries(self, client, _s, keys_on_two_primaries):
        first, second = keys_on_two_primaries
        await client.set(first, "x")
        await client.set(second, "y")
        with pytest.raises((ResponseError, WrongTypeError)):
            async with client.himport("fs", ["name"]) as himport:
                himport.add(first, ["a"])
                himport.add(second, ["b"])
        assert await client.get(first) == _s("x")
        assert await client.get(second) == _s("y")

    async def test_wrongtype_on_one_primary_writes_the_other(
        self, client, _s, keys_on_two_primaries
    ):
        bad, good = keys_on_two_primaries
        await client.set(bad, "x")
        with pytest.raises((ResponseError, WrongTypeError), match=bad):
            async with client.himport("fs", ["name"]) as himport:
                himport.add(bad, ["a"])
                himport.add(good, ["bob"])
        assert await client.get(bad) == _s("x")
        assert await client.hget(good, "name") == _s("bob")

    async def test_wrongtype_and_good_hash_same_primary(self, client, _s, same_node_keys):
        bad, good = same_node_keys
        await client.set(bad, "x")
        with pytest.raises((ResponseError, WrongTypeError), match=bad):
            async with client.himport("fs", ["name"]) as himport:
                himport.add(bad, ["a"])
                himport.add(good, ["bob"])
        assert await client.get(bad) == _s("x")
        assert await client.hget(good, "name") == _s("bob")

    async def test_moved_wrongtype_follows_new_owner(self, client, stale_cluster):
        await client.set("u:str", "not-a-hash")
        with pytest.raises((ResponseError, WrongTypeError)):
            async with stale_cluster.himport("fs", ["name"]) as himport:
                himport.add("u:str", ["alice"])
