from __future__ import annotations

import datetime
import time

import pytest

from coredis import PureToken
from coredis.exceptions import DataError, NoKeyError, ResponseError
from tests.conftest import targets


@targets(
    "redis_basic",
    "redis_basic_blocking",
    "redis_basic_raw",
    "redis_basic_resp2",
    "redis_basic_raw_resp2",
    "redis_cluster",
    "redis_cluster_raw",
    "redis_cached",
    "redis_cached_resp2",
    "redis_cluster_cached",
    "keydb",
)
@pytest.mark.asyncio()
class TestGeneric:
    async def test_sort_basic(self, client, _s):
        await client.rpush("a", ["3", "2", "1", "4"])
        assert await client.sort("a") == (_s("1"), _s("2"), _s("3"), _s("4"))

    @pytest.mark.clusteronly
    @pytest.mark.min_server_version("7.0.0")
    async def test_sort_ro(self, client, cloner, _s):
        clone = await cloner(client, connection_kwargs={"readonly": True})
        await client.rpush("a{fu}", ["3", "2", "1", "4"])
        await client.set("score{fu}:1", "8")
        await client.set("score{fu}:2", "3")
        await client.set("score{fu}:3", "5")
        assert await clone.sort_ro("a{fu}") == (_s("1"), _s("2"), _s("3"), _s("4"))
        assert await clone.sort_ro("a{fu}", offset=1, count=2) == (_s("2"), _s("3"))
        assert await clone.sort_ro(
            "a{fu}", order=PureToken.DESC, offset=1, count=2
        ) == (_s("3"), _s("2"))
        assert await clone.sort_ro("a{fu}", alpha=True, offset=1, count=2) == (
            _s("2"),
            _s("3"),
        )
        with pytest.raises(ResponseError, match="denied in Cluster mode"):
            await clone.sort_ro("a{fu}", by="score{fu}:*")
        with pytest.raises(ResponseError, match="denied in Cluster mode"):
            await client.sort_ro("a{fu}", ["score:*"])

    async def test_sort_limited(self, client, _s):
        await client.rpush("a", ["3", "2", "1", "4"])
        assert await client.sort("a", offset=1, count=2) == (_s("2"), _s("3"))

    @pytest.mark.nocluster
    async def test_sort_by(self, client, _s):
        await client.set("score:1", "8")
        await client.set("score:2", "3")
        await client.set("score:3", "5")
        await client.rpush("a", ["3", "2", "1"])
        assert await client.sort("a", by="score:*") == (_s("2"), _s("3"), _s("1"))

    @pytest.mark.nocluster
    async def test_sort_get(self, client, _s):
        await client.set("user:1", "u1")
        await client.set("user:2", "u2")
        await client.set("user:3", "u3")
        await client.rpush("a", ["2", "3", "1"])
        assert await client.sort("a", ["user:*"]) == (_s("u1"), _s("u2"), _s("u3"))

    @pytest.mark.nocluster
    async def test_sort_get_multi(self, client, _s):
        await client.set("user:1", "u1")
        await client.set("user:2", "u2")
        await client.set("user:3", "u3")
        await client.rpush("a", ["2", "3", "1"])
        assert await client.sort("a", gets=("user:*", "#")) == (
            _s("u1"),
            _s("1"),
            _s("u2"),
            _s("2"),
            _s("u3"),
            _s("3"),
        )

    @pytest.mark.nocluster
    async def test_sort_three_gets(self, client, _s):
        await client.set("user:1", "u1")
        await client.set("user:2", "u2")
        await client.set("user:3", "u3")
        await client.set("door:1", "d1")
        await client.set("door:2", "d2")
        await client.set("door:3", "d3")
        await client.rpush("a", ["2", "3", "1"])
        assert await client.sort("a", gets=["user:*", "door:*", "#"]) == (
            _s("u1"),
            _s("d1"),
            _s("1"),
            _s("u2"),
            _s("d2"),
            _s("2"),
            _s("u3"),
            _s("d3"),
            _s("3"),
        )

    async def test_sort_desc(self, client, _s):
        await client.rpush("a", ["2", "3", "1"])
        assert await client.sort("a", order=PureToken.DESC) == (
            _s("3"),
            _s("2"),
            _s("1"),
        )

    async def test_sort_alpha(self, client, _s):
        await client.rpush("a", ["e", "c", "b", "d", "a"])
        assert await client.sort("a", alpha=True) == (
            _s("a"),
            _s("b"),
            _s("c"),
            _s("d"),
            _s("e"),
        )

    async def test_sort_store(self, client, _s):
        await client.rpush("a{foo}", ["2", "3", "1"])
        assert await client.sort("a{foo}", store="sorted_values{foo}") == 3
        assert await client.lrange("sorted_values{foo}", 0, -1) == [
            _s("1"),
            _s("2"),
            _s("3"),
        ]

    @pytest.mark.nocluster
    async def test_sort_all_options(self, client, _s):
        await client.set("user:1:username", "zeus")
        await client.set("user:2:username", "titan")
        await client.set("user:3:username", "hermes")
        await client.set("user:4:username", "hercules")
        await client.set("user:5:username", "apollo")
        await client.set("user:6:username", "athena")
        await client.set("user:7:username", "hades")
        await client.set("user:8:username", "dionysus")

        await client.set("user:1:favorite_drink", "yuengling")
        await client.set("user:2:favorite_drink", "rum")
        await client.set("user:3:favorite_drink", "vodka")
        await client.set("user:4:favorite_drink", "milk")
        await client.set("user:5:favorite_drink", "pinot noir")
        await client.set("user:6:favorite_drink", "water")
        await client.set("user:7:favorite_drink", "gin")
        await client.set("user:8:favorite_drink", "apple juice")

        await client.rpush("gods", ["5", "8", "3", "1", "2", "7", "6", "4"])
        num = await client.sort(
            "gods",
            offset=2,
            count=4,
            by="user:*:username",
            gets=["user:*:favorite_drink"],
            order=PureToken.DESC,
            alpha=True,
            store="sorted",
        )
        assert num == 4
        assert await client.lrange("sorted", 0, 10) == [
            _s("vodka"),
            _s("milk"),
            _s("gin"),
            _s("apple juice"),
        ]

    async def test_delete(self, client, _s):
        assert await client.delete(["a"]) == 0
        await client.set("a", "foo")
        assert await client.delete(["a"]) == 1

    async def test_delete_with_multiple_keys(self, client, _s):
        await client.set("a{foo}", "foo")
        await client.set("b{foo}", "bar")
        assert await client.delete(["a{foo}", "b{foo}"]) == 2
        assert await client.get("a{foo}") is None
        assert await client.get("b{foo}") is None

    async def test_dump_and_restore_with_ttl(self, client, _s):
        await client.set("a", "foo")
        dumped = await client.dump("a")
        await client.delete(["a"])
        assert await client.restore("a", datetime.timedelta(milliseconds=500), dumped)
        assert await client.pttl("a") < 1000
        await client.delete(["a"])
        assert await client.restore("a", datetime.timedelta(milliseconds=1500), dumped)
        assert await client.pttl("a") > 1000
        await client.delete(["a"])
        assert await client.restore(
            "a",
            datetime.datetime.utcnow()
            + datetime.timedelta(minutes=1, milliseconds=1000),
            dumped,
            absttl=True,
        )
        assert await client.pttl("a") > 60 * 1000

    async def test_dump_and_restore_with_freq(self, client, _s):
        await client.config_set({"maxmemory-policy": "allkeys-lfu"})
        await client.set("a", "foo")
        freq = await client.object_freq("a")
        dumped = await client.dump("a")
        await client.delete(["a"])
        await client.restore("a", 0, dumped, freq=freq)
        assert await client.get("a") == _s("foo")
        freq_now = await client.object_freq("a")
        assert freq + 1 == freq_now

    async def test_dump_and_restore_with_idle_time(self, client, _s):
        await client.set("a", "foo")
        idle = await client.object_idletime("a")
        assert idle <= 1
        dumped = await client.dump("a")
        assert await client.delete(["a"]) == 1
        assert await client.restore("a", 0, dumped, idletime=2)
        new_idle = await client.object_idletime("a")
        assert new_idle >= 1

    async def test_dump_and_restore_and_replace(self, client, _s):
        await client.set("a", "bar")
        dumped = await client.dump("a")
        with pytest.raises(ResponseError):
            await client.restore("a", 0, dumped)

        await client.restore("a", 0, dumped, replace=True)
        assert await client.get("a") == _s("bar")

    @pytest.mark.nocluster
    async def test_migrate_single_key_with_auth(self, client, redis_auth, _s):
        auth_connection = await redis_auth.connection_pool.get_connection()
        await client.set("a", "1")

        with pytest.raises(DataError):
            await client.migrate("172.17.0.1", auth_connection.port, 0, 100)

        assert not await client.migrate("172.17.0.1", auth_connection.port, 0, 100, "b")
        assert await client.migrate(
            "172.17.0.1", auth_connection.port, 0, 100, "a", auth="sekret"
        )
        assert await redis_auth.get("a") == "1"
        await client.set("b", "2")
        assert await client.migrate(
            "172.17.0.1",
            auth_connection.port,
            0,
            100,
            "b",
            username="default",
            password="sekret",
        )
        assert await redis_auth.get("b") == "2"
        assert not await client.get("a")
        assert not await client.get("b")

        await client.set("c", "3")
        assert await client.migrate(
            "172.17.0.1",
            auth_connection.port,
            0,
            100,
            "c",
            username="default",
            password="sekret",
            copy=True,
        )
        assert await client.get("c") == _s(3)
        assert await redis_auth.get("c") == "3"
        await client.set("c", 4)
        assert await client.migrate(
            "172.17.0.1",
            auth_connection.port,
            0,
            100,
            "c",
            username="default",
            password="sekret",
            copy=True,
            replace=True,
        )
        assert await redis_auth.get("c") == "4"

        with pytest.raises(ResponseError, match="BUSYKEY"):
            await client.migrate(
                "172.17.0.1",
                auth_connection.port,
                0,
                100,
                "c",
                username="default",
                password="sekret",
                copy=True,
            )
        await redis_auth.flushall()
        with pytest.raises(ResponseError, match="WRONGPASS"):
            await client.migrate(
                "172.17.0.1",
                auth_connection.port,
                0,
                100,
                "c",
                auth="Sekrets",
            )

    @pytest.mark.nocluster
    async def test_migrate_multiple_keys_with_auth(self, client, redis_auth, _s):
        auth_connection = await redis_auth.connection_pool.get_connection()
        await client.set("a", "1")
        await client.set("c", "2")
        assert not await client.migrate(
            "172.17.0.1", auth_connection.port, 0, 100, "d", "b"
        )
        assert await client.migrate(
            "172.17.0.1", auth_connection.port, 0, 100, "a", "c", auth="sekret"
        )

        assert await redis_auth.get("a") == "1"
        assert await redis_auth.get("c") == "2"

    @pytest.mark.nocluster
    async def test_move(self, client, cloner, _s):
        clone = await cloner(client, connection_kwargs={"db": 1})
        await client.set("foo", 1)
        assert await client.move("foo", 1)
        assert not await client.get("foo")
        assert await clone.get("foo") == _s(1)

    @pytest.mark.min_server_version("6.2.0")
    async def test_copy(self, client, _s):
        await client.set("a{foo}", "foo")
        await client.set("c{foo}", "bar")
        assert await client.copy("x{foo}", "y{foo}") is False
        assert True == (await client.copy(_s("a{foo}"), "b{foo}"))
        assert await client.get("b{foo}") == _s("foo")
        assert False == (await client.copy(_s("a{foo}"), "c{foo}", replace=False))
        assert await client.get("c{foo}") == _s("bar")
        assert True == (await client.copy(_s("a{foo}"), "c{foo}", replace=True))
        assert await client.get("c{foo}") == _s("foo")

    @pytest.mark.min_server_version("6.2.0")
    @pytest.mark.nocluster
    async def test_copy_different_db(self, client, cloner, _s):
        clone = await cloner(client, connection_kwargs={"db": 1})
        await client.set("foo", 1)
        assert await client.copy("foo", "bar", db=1)
        assert not await client.get("bar")
        assert await clone.get("bar") == _s(1)

    @pytest.mark.max_server_version("6.2.0")
    async def test_object_encoding(self, client, _s):
        await client.set("a", "foo")
        await client.hset("b", {"foo": "1"})
        assert await client.object_encoding("a") == _s("embstr")
        assert await client.object_encoding("b") == _s("ziplist")

    @pytest.mark.min_server_version("7.0.0")
    async def test_object_encoding_listpack(self, client, _s):
        await client.set("a", "foo")
        await client.hset("b", {"foo": "1"})
        assert await client.object_encoding("a") == _s("embstr")
        assert await client.object_encoding("b") == _s("listpack")

    async def test_object_freq(self, client, _s):
        await client.set("a", "foo")
        with pytest.raises(ResponseError):
            await client.object_freq("a"),
        await client.config_set({"maxmemory-policy": "allkeys-lfu"})
        assert isinstance(await client.object_freq("a"), int)

    async def test_object_idletime(self, client, _s):
        await client.set("a", "foo")
        assert isinstance(await client.object_idletime("a"), int)
        await client.config_set({"maxmemory-policy": "allkeys-lfu"})
        with pytest.raises(ResponseError):
            await client.object_idletime("a"),

    async def test_object_refcount(self, client, _s):
        await client.set("a", "foo")
        assert await client.object_refcount("a") == 1

    async def test_exists(self, client, _s):
        assert not await client.exists(["a{foo}"])
        await client.set("a{foo}", "foo")
        assert await client.exists(["a{foo}"]) == 1
        await client.set("b{foo}", "foo")
        assert await client.exists(["a{foo}", "b{foo}", "c{foo}"]) == 2

    async def test_expire(self, client, _s):
        assert not await client.expire("a", 10)
        await client.set("a", "foo")
        assert await client.expire("a", 10)
        assert 0 < await client.ttl("a") <= 10
        assert await client.persist("a")
        assert await client.ttl("a") == -1

    @pytest.mark.min_server_version("7.0.0")
    async def test_expire_conditional(self, client, _s):
        await client.set("a", "foo")
        assert await client.expire("a", 10, PureToken.NX)
        assert not await client.expire("a", 10, PureToken.NX)
        assert await client.expire("a", 20, PureToken.XX)
        assert not await client.expire("a", 19, PureToken.GT)
        assert await client.expire("a", 19, PureToken.LT)

    async def test_expireat_datetime(self, client, redis_server_time):
        expire_at = await redis_server_time(client) + datetime.timedelta(minutes=1)
        await client.set("a", "foo")
        assert await client.expireat("a", expire_at)
        assert 0 < await client.ttl("a") <= 61

    async def test_expireat_no_key(self, client, redis_server_time):
        expire_at = await redis_server_time(client) + datetime.timedelta(minutes=1)
        assert not await client.expireat("a", expire_at)

    async def test_expireat_unixtime(self, client, redis_server_time):
        expire_at = await redis_server_time(client) + datetime.timedelta(minutes=1)
        await client.set("a", "foo")
        expire_at_seconds = int(time.mktime(expire_at.timetuple()))
        assert await client.expireat("a", expire_at_seconds)
        assert 0 < await client.ttl("a") <= 61

    @pytest.mark.min_server_version("7.0.0")
    async def test_expireat_conditional(self, client, _s):
        at = datetime.datetime.utcnow()
        await client.set("a", "foo")
        assert await client.expireat(
            "a", at + datetime.timedelta(seconds=10), PureToken.NX
        )
        assert not await client.expireat(
            "a", at + datetime.timedelta(seconds=10), PureToken.NX
        )
        assert await client.expireat(
            "a", at + datetime.timedelta(seconds=20), PureToken.XX
        )
        assert not await client.expireat(
            "a", at + datetime.timedelta(seconds=19), PureToken.GT
        )
        assert await client.expireat(
            "a", at + datetime.timedelta(seconds=19), PureToken.LT
        )

    @pytest.mark.min_server_version("7.0.0")
    async def test_expiretime(self, client, _s):
        now = datetime.datetime.utcnow()
        await client.set("a", "foo")
        await client.set("b", "foo")

        with pytest.raises(NoKeyError):
            await client.expiretime("c")
        with pytest.raises(DataError):
            await client.expiretime("b")
        set_time = datetime.datetime(now.year + 1, now.month, 1, 0, 0, 1, 1000)
        await client.pexpireat("a", set_time)
        expire_time = await client.expiretime("a")
        assert set_time.replace(microsecond=0) == expire_time

    async def test_keys(self, client, _s):
        assert await client.keys() == set()
        keys_with_underscores = {"test_a", "test_b"}
        keys = keys_with_underscores | {"testc"}

        for key in keys:
            await client.set(key, "1")
        assert await client.keys(pattern=_s("test_*")) == {
            _s(k) for k in keys_with_underscores
        }
        assert await client.keys(pattern=_s("test*")) == {_s(k) for k in keys}

    async def test_pexpire(self, client, _s):
        assert not await client.pexpire("a", 60000)
        await client.set("a", "foo")
        assert await client.pexpire("a", 60000)
        assert 0 < await client.pttl("a") <= 60000
        assert await client.persist("a")
        assert await client.pttl("a") < 0

    @pytest.mark.min_server_version("7.0.0")
    async def test_pexpire_conditional(self, client, _s):
        await client.set("a", "foo")
        assert await client.pexpire("a", 10000, PureToken.NX)
        assert not await client.pexpire("a", 100000, PureToken.NX)
        assert await client.pexpire("a", 20000, PureToken.XX)
        assert not await client.pexpire("a", 19000, PureToken.GT)
        assert await client.pexpire("a", 19000, PureToken.LT)

    async def test_pexpireat_datetime(self, client, redis_server_time):
        expire_at = await redis_server_time(client) + datetime.timedelta(minutes=1)
        await client.set("a", "foo")
        assert await client.pexpireat("a", expire_at)
        assert 0 < await client.pttl("a") <= 61000

    async def test_pexpireat_no_key(self, client, redis_server_time):
        expire_at = await redis_server_time(client) + datetime.timedelta(minutes=1)
        assert not await client.pexpireat("a", expire_at)

    async def test_pexpireat_unixtime(self, client, redis_server_time):
        expire_at = await redis_server_time(client) + datetime.timedelta(minutes=1)
        await client.set("a", "foo")
        expire_at_seconds = int(time.mktime(expire_at.timetuple())) * 1000
        assert await client.pexpireat("a", expire_at_seconds)
        assert 0 < await client.pttl("a") <= 61000

    @pytest.mark.min_server_version("7.0.0")
    async def test_pexpireat_conditional(self, client, _s):
        at = datetime.datetime.utcnow()
        await client.set("a", "foo")
        assert await client.pexpireat(
            "a", at + datetime.timedelta(seconds=10), PureToken.NX
        )
        assert not await client.pexpireat(
            "a", at + datetime.timedelta(seconds=10), PureToken.NX
        )
        assert await client.pexpireat(
            "a", at + datetime.timedelta(seconds=20), PureToken.XX
        )
        assert not await client.pexpireat(
            "a", at + datetime.timedelta(seconds=19), PureToken.GT
        )
        assert await client.pexpireat(
            "a", at + datetime.timedelta(seconds=19), PureToken.LT
        )

    @pytest.mark.min_server_version("7.0.0")
    async def test_pexpiretime(self, client, _s):
        now = datetime.datetime.utcnow()
        await client.set("a", "foo")
        await client.set("b", "foo")

        with pytest.raises(NoKeyError):
            await client.expiretime("c")
        with pytest.raises(DataError):
            await client.expiretime("b")
        set_time = datetime.datetime(now.year + 1, now.month, 1, 0, 0, 1, 1000)
        await client.pexpireat("a", set_time)
        expire_time = await client.pexpiretime("a")
        assert set_time == expire_time

    @pytest.mark.flaky
    async def test_randomkey(self, client, _s):
        assert await client.randomkey() is None

        for key in ("a", "b", "c"):
            await client.set(key, "1")
        assert await client.randomkey() in (_s("a"), _s("b"), _s("c"))

    async def test_rename(self, client, _s):
        await client.set("a{foo}", "1")
        assert await client.rename("a{foo}", "b{foo}")
        assert await client.get("a{foo}") is None
        assert await client.get("b{foo}") == _s("1")

    async def test_renamenx(self, client, _s):
        await client.set("a{foo}", "1")
        await client.set("b{foo}", "2")
        assert not await client.renamenx("a{foo}", "b{foo}")
        assert await client.get("a{foo}") == _s("1")
        assert await client.get("b{foo}") == _s("2")

    async def test_type(self, client, _s):
        assert await client.type("a") == _s("none")
        await client.set("a", "1")
        assert await client.type("a") == _s("string")
        await client.delete(["a"])
        await client.lpush("a", ["1"])
        assert await client.type("a") == _s("list")
        await client.delete(["a"])
        await client.sadd("a", ["1"])
        assert await client.type("a") == _s("set")
        await client.delete(["a"])
        await client.zadd("a", {"1": "1"})
        assert await client.type("a") == _s("zset")

    async def test_touch(self, client, _s):
        keys = ["a{foo}", "b{foo}", "c{foo}", "d{foo}"]

        for index, key in enumerate(keys):
            await client.set(key, str(index))
        assert await client.touch(keys) == len(keys)

    async def test_unlink(self, client, _s):
        keys = ["a{foo}", "b{foo}", "c{foo}", "d{foo}"]

        for index, key in enumerate(keys):
            await client.set(key, str(index))
        await client.unlink(keys)

        for key in keys:
            assert await client.get(key) is None

    @pytest.mark.nocluster
    async def test_scan(self, client, _s):
        await client.set("a", "1")
        await client.set("b", "2")
        await client.set("c", "3")
        await client.hset("d", {"a": "1"})
        cursor, keys = await client.scan()
        assert cursor == 0
        assert set(keys) == {_s("a"), _s("b"), _s("c"), _s("d")}
        _, keys = await client.scan(match="a")
        assert set(keys) == {_s("a")}
        _, keys = await client.scan(count=1)
        assert len(keys) <= 4
        _, keys = await client.scan(type_="hash")
        assert set(keys) == {_s("d")}

    async def test_scan_iter(self, client, _s):
        await client.set("a", "1")
        await client.set("b", "2")
        await client.set("c", "3")
        keys = set()
        async for key in client.scan_iter():
            keys.add(key)
        assert keys == {_s("a"), _s("b"), _s("c")}
        async for key in client.scan_iter(match="a"):
            assert key == _s("a")
