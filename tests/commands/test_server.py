from __future__ import annotations

import datetime

import anyio
import pytest
from pytest import approx

from coredis import PureToken
from coredis.exceptions import ReadOnlyError, ResponseError
from coredis.tokens import PrefixToken
from tests.conftest import targets


@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_cluster",
    "valkey",
)
class TestServer:
    async def slowlog(self, client, _s):
        current_config = await client.config_get(["*"])
        old_slower_than_value = current_config[_s("slowlog-log-slower-than")]
        old_max_length_value = current_config[_s("slowlog-max-len")]
        await client.config_set({"slowlog-log-slower-than": 0})
        await client.config_set({"slowlog-max-len": 128})

        return old_slower_than_value, old_max_length_value

    async def cleanup(self, client, old_slower_than_value, old_max_legnth_value):
        await client.config_set({"slowlog-log-slower-than": old_slower_than_value})
        await client.config_set({"slowlog-max-len": old_max_legnth_value})

    async def test_command_count(self, client, _s):
        assert await client.command_count() > 100  # :D

    async def test_command_docs(self, client, _s):
        docs = await client.command_docs("geosearch")
        assert _s("summary") in docs[_s("geosearch")]
        assert _s("arguments") in docs[_s("geosearch")]
        docs = await client.command_docs("get", "set")
        assert {_s("get"), _s("set")} & set(docs.keys())

    async def test_commands_get(self, client, _s):
        commands = await client.command()
        assert commands["get"]
        assert commands["set"]
        assert commands["get"]["name"] == _s("get")
        assert commands["get"]["arity"] == 2

    async def test_command_info(self, client, _s):
        commands = await client.command_info("get")
        assert list(commands.keys()) == ["get"]
        assert commands["get"]["name"] == _s("get")
        assert commands["get"]["arity"] == 2

    async def test_command_list(self, client, _s):
        assert _s("get") in await client.command_list()
        assert _s("acl|getuser") in await client.command_list(aclcat="admin")
        assert _s("zrevrange") in await client.command_list(pattern="zrev*")
        assert set() == await client.command_list(module="doesnotexist")

    async def test_command_getkeys(self, client, _s):
        assert await client.command_getkeys("MSET", ["a", 1, "b", 2]) == (
            _s("a"),
            _s("b"),
        )

    async def test_command_getkeysandflags(self, client, _s):
        assert await client.command_getkeysandflags("MSET", ["a", 1, "b", 2]) == {
            _s("a"): {_s("OW"), _s("update")},
            _s("b"): {_s("OW"), _s("update")},
        }

    @pytest.mark.nocluster
    async def test_config_get(self, client, _s):
        data = await client.config_get(["*"])
        assert _s("maxmemory") in data
        assert data[_s("maxmemory")].isdigit()

    async def test_config_resetstat(self, client, _s):
        await client.ping()
        prior_commands_processed = int((await client.info())["total_commands_processed"])
        assert prior_commands_processed >= 1
        await client.config_resetstat()
        reset_commands_processed = int((await client.info())["total_commands_processed"])
        assert reset_commands_processed < prior_commands_processed

    @pytest.mark.nocluster
    async def test_config_rewrite(self, client):
        with pytest.raises(ResponseError, match="The server is running without a config file"):
            await client.config_rewrite()

    @pytest.mark.nocluster
    async def test_dbsize(self, client, _s):
        await client.set("a", "foo")
        await client.set("b", "bar")
        assert await client.dbsize() == 2

    async def test_debug_object(self, client):
        await client.set("fubar", 1)
        object_info = await client.debug_object("fubar")
        assert object_info["type"] == "Value"
        assert object_info["encoding"] == "int"

    @pytest.mark.parametrize(
        "mode",
        [
            None,
            PureToken.SYNC,
            PureToken.ASYNC,
        ],
    )
    @pytest.mark.nocluster
    async def test_flushall(self, client, cloner, _s, mode):
        await client.set("a", "foo")
        await client.set("b", "bar")
        db1 = await cloner(client, connection_kwargs={"db": 1})
        async with db1:
            await db1.set("a", "foo")
            await db1.set("b", "bar")
            assert len(await client.keys()) == 2
            assert len(await db1.keys()) == 2
            assert await client.flushall(mode)
            assert len(await client.keys()) == 0
            assert len(await db1.keys()) == 0

    @pytest.mark.parametrize(
        "mode",
        [
            None,
            PureToken.SYNC,
            PureToken.ASYNC,
        ],
    )
    async def test_flushdb(self, client, _s, mode):
        await client.set("a", "foo")
        await client.set("b", "bar")
        assert len(await client.keys()) == 2
        assert await client.flushdb(mode)
        assert len(await client.keys()) == 0

    @pytest.mark.nocluster
    async def test_slowlog_get(self, client, _s):
        sl_v, length_v = await self.slowlog(client, _s)
        await client.slowlog_reset()
        unicode_string = "3456abcd3421"
        await client.get(unicode_string)
        slowlog = await client.slowlog_get()
        commands = [log.command for log in slowlog]

        get_command = [_s("GET"), _s(unicode_string)]
        assert get_command in commands
        assert [_s("SLOWLOG"), _s("RESET")] in commands
        # the order should be ['GET <uni string>', 'SLOWLOG RESET'],
        # but if other clients are executing commands at the same time, there
        # could be commands, before, between, or after, so just check that
        # the two we care about are in the appropriate ordeclient.
        assert commands.index(get_command) < commands.index([_s("SLOWLOG"), _s("RESET")])

        # make sure other attributes are typed correctly
        assert isinstance(slowlog[0].start_time, int)
        assert isinstance(slowlog[0].duration, int)
        await self.cleanup(client, sl_v, length_v)

    @pytest.mark.nocluster
    async def test_slowlog_get_limit(self, client, _s):
        sl_v, length_v = await self.slowlog(client, _s)
        assert await client.slowlog_reset()
        await client.get("foo")
        await client.get("bar")
        slowlog = await client.slowlog_get(1)
        commands = [log.command for log in slowlog]
        assert [_s("GET"), _s("foo")] not in commands
        assert [_s("GET"), _s("bar")] in commands
        await self.cleanup(client, sl_v, length_v)

    @pytest.mark.nocluster
    async def test_slowlog_length(self, client, _s):
        sl_v, length_v = await self.slowlog(client, _s)
        await client.get("foo")
        assert isinstance(await client.slowlog_len(), int)
        await self.cleanup(client, sl_v, length_v)

    @pytest.mark.nocluster
    async def test_time(self, client, _s):
        t = await client.time()
        assert isinstance(t, datetime.datetime)

    @pytest.mark.nocluster
    async def test_info(self, client, _s):
        await client.set("a", "foo")
        await client.set("b", "bar")
        info = await client.info()
        assert isinstance(info, dict)
        assert info["db0"]["keys"] == 2
        keyspace = await client.info("keyspace")
        assert {"db0"} == keyspace.keys()

    @pytest.mark.clusteronly
    async def test_info_cluster(self, client, _s):
        await client.set("a", "foo")
        await client.set("b", "bar")
        info = await client.info()
        assert isinstance(info, dict)
        assert info[_s("redis_mode")] == _s("cluster")

    @pytest.mark.nocluster
    async def test_lastsave(self, client, _s):
        assert isinstance(await client.lastsave(), datetime.datetime)

    @pytest.mark.nocluster
    async def test_lolwut(self, client, _s):
        lolwut = await client.lolwut(5)
        assert "ver." in lolwut

    @pytest.mark.nocluster
    async def test_memory_doctor(self, client, _s):
        assert _s("Sam") in (await client.memory_doctor())

    @pytest.mark.nocluster
    async def test_memory_malloc_stats(self, client, _s):
        assert _s("jemalloc") in (await client.memory_malloc_stats())

    async def test_memory_purge(self, client, _s):
        assert await client.memory_purge() is True

    @pytest.mark.nocluster
    async def test_memory_stats(self, client, _s):
        stats = await client.memory_stats()
        assert stats[_s("keys.count")] == 0

    async def test_memory_usage(self, client, _s):
        await client.set("key", str(bytearray([0] * 1024)))
        assert (await client.memory_usage(_s("key"))) > 1024
        assert (await client.memory_usage(_s("key"), samples=1)) > 1024

    @pytest.mark.nocluster
    async def test_latency_doctor(self, client, _s):
        assert await client.latency_doctor()

    @pytest.mark.nocluster
    @pytest.mark.novalkey
    async def test_latency_all(self, client, _s):
        await client.create_request(b"debug", "sleep", 0.05, callback=lambda v: v)
        history = await client.latency_history("command")
        assert len(history) >= 1
        await client.latency_reset()

        await client.create_request(b"debug", "sleep", 0.05, callback=lambda v: v)
        history = await client.latency_history("command")
        assert len(history) == 1
        assert history[0][1] == approx(50, 60)
        latest = await client.latency_latest()
        assert latest[_s("command")][1] == approx(50, 60)
        assert latest[_s("command")][2] == approx(50, 60)

    @pytest.mark.nocluster
    @pytest.mark.novalkey
    async def test_latency_graph(self, client, _s):
        await client.create_request(b"debug", "sleep", 0.05, callback=lambda v: v)
        graph = await client.latency_graph("command")
        assert _s("command - high") in graph

    @pytest.mark.nocluster
    async def test_latency_histogram(self, client, _s):
        await client.set("a", 1)
        await client.set("a", 1)
        await client.set("a", 1)
        await client.get("a")
        await client.get("a")
        histogram = await client.latency_histogram()
        assert _s("set") in histogram
        assert _s("get") in histogram

    @pytest.mark.nocluster
    async def test_role(self, client, _s):
        role_info = await client.role()
        assert role_info.role == "master"

    @pytest.mark.nocluster
    async def test_save(self, client):
        assert await client.save()
        assert (
            (await client.lastsave()) - datetime.datetime.now(datetime.timezone.utc)
        ) < datetime.timedelta(minutes=1)

    @pytest.mark.nocluster
    async def test_replicaof(self, client, _s):
        assert await client.replicaof()
        try:
            await client.replicaof("nowhere", 6666)
            with pytest.raises(ReadOnlyError):
                await client.set("fubar", 1)
        finally:
            # reset to replica of self
            await client.replicaof()

    @pytest.mark.nocluster
    async def test_slaveof(self, client, _s):
        with pytest.warns(DeprecationWarning):
            assert await client.slaveof()
        try:
            with pytest.warns(DeprecationWarning):
                await client.slaveof("nowhere", 6666)
            with pytest.raises(ReadOnlyError):
                await client.set("fubar", 1)
        finally:
            # reset to replica of self
            with pytest.warns(DeprecationWarning):
                await client.slaveof()

    @pytest.mark.nocluster
    async def test_swapdb(self, client, _s):
        await client.set("fubar", 1)
        with pytest.warns(UserWarning):
            await client.select(1)
        await client.set("fubar", 2)
        with pytest.warns(UserWarning):
            await client.select(0)
        assert await client.get("fubar") == _s(1)
        assert await client.swapdb(0, 1)
        assert await client.get("fubar") == _s(2)

    @pytest.mark.nocluster
    @pytest.mark.max_server_version("7.1")
    async def test_quit(self, client):
        assert await client.quit()
        await anyio.sleep(0.1)
        assert not client.connection_pool.peek_available().usable


async def test_failover(fake_redis):
    fake_redis.responses[b"FAILOVER"] = {
        (PrefixToken.TO, "target", 6379): b"OK",
        (
            PrefixToken.TO,
            "target",
            6379,
            PureToken.FORCE,
        ): b"OK",
        (PureToken.ABORT,): b"OK",
        (PrefixToken.TO, "target", 6379, PrefixToken.TIMEOUT, 1000): b"OK",
    }
    assert await fake_redis.failover("target", 6379)
    assert await fake_redis.failover("target", 6379, force=True)
    assert await fake_redis.failover(abort=True)
    assert await fake_redis.failover("target", 6379, timeout=datetime.timedelta(seconds=1))


@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_cluster",
    "valkey",
)
@pytest.mark.min_server_version("8.10.0")
class TestBackup:
    #: ``BACKUP`` acts on a single server's on disk files.
    pytestmark = pytest.mark.nocluster

    @pytest.fixture(autouse=True)
    async def idle_backup_state(self, client, _s):
        # A backup left running would leak into the next test, so return the
        # server to idle whatever the test did.
        yield
        with client.ignore_replies():
            await client.backup_abort()
            await client.backup_cleanup()

    async def wait_for_state(self, client, _s, state):
        # A freshly started backup passes through a snapshot phase before it is
        # ready to be sealed.
        for _ in range(100):
            status = await client.backup_status()
            if status[_s("state")] == _s(state):
                return status
            await anyio.sleep(0.1)
        raise AssertionError(f"backup did not reach {state}: {status}")

    async def test_backup_lifecycle(self, client, _s):
        # Nothing pinned while the server is idle.
        assert (await client.backup_status())[_s("state")] == _s("idle")
        assert await client.backup_list() == []

        assert await client.backup_start() is True
        status = await self.wait_for_state(client, _s, "incrementing")
        assert status[_s("start_time")] > 0
        # The base rdb is pinned as soon as the snapshot completes.
        assert len(await client.backup_list()) >= 1

        assert await client.backup_seal() is True
        status = await client.backup_status()
        assert status[_s("state")] == _s("sealed")
        assert status[_s("end_time")] > 0
        # A sealed backup pins the base rdb, the incremental aof and the manifest.
        assert len(await client.backup_list()) >= 3

        # CLEANUP discards the sealed files and returns the server to idle.
        assert await client.backup_cleanup() is True
        assert (await client.backup_status())[_s("state")] == _s("idle")
        assert await client.backup_list() == []

    async def test_backup_start_twice(self, client, _s):
        await client.backup_start()
        # Only one backup may be in flight at a time.
        with pytest.raises(ResponseError):
            await client.backup_start()

    async def test_backup_abort(self, client, _s):
        await client.backup_start()
        assert await client.backup_abort() is True
        # An aborted backup is reported as failed, with the reason retained ...
        status = await client.backup_status()
        assert status[_s("state")] == _s("failed")
        assert status[_s("error")] == _s("aborted by user")
        # ... until CLEANUP discards it.
        assert await client.backup_cleanup() is True
        assert (await client.backup_status())[_s("state")] == _s("idle")
        # Aborting when nothing is running is an error.
        with pytest.raises(ResponseError):
            await client.backup_abort()


@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_cluster",
    "valkey",
)
@pytest.mark.min_server_version("8.10.0")
class TestSlowlogArgumentCount:
    pytestmark = pytest.mark.nocluster

    async def test_slowlog_get_argument_count(self, client, _s):
        current_config = await client.config_get(["*"])
        old_slower_than = current_config[_s("slowlog-log-slower-than")]
        old_max_length = current_config[_s("slowlog-max-len")]
        await client.config_set({"slowlog-log-slower-than": 0})
        await client.config_set({"slowlog-max-len": 128})
        try:
            await client.slowlog_reset()
            await client.get("argument-count")
            slowlog = await client.slowlog_get()
            get_entry = [
                entry for entry in slowlog if entry.command == [_s("GET"), _s("argument-count")]
            ].pop()
            # 8.10 appends the total argument count of the logged command.
            assert get_entry.argument_count == 2
        finally:
            await client.config_set({"slowlog-log-slower-than": old_slower_than})
            await client.config_set({"slowlog-max-len": old_max_length})
