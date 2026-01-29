from __future__ import annotations

import pytest

from coredis._concurrency import gather
from coredis.exceptions import (
    AuthorizationError,
    ClusterCrossSlotError,
    ClusterTransactionError,
    RedisClusterException,
    ResponseError,
    WatchError,
)
from coredis.pipeline import ClusterPipeline
from tests.conftest import targets


@targets("redis_cluster")
class TestPipeline:
    async def test_empty_pipeline(self, client):
        async with client.pipeline():
            pass

    async def test_pipeline_simple(self, client):
        async with client.pipeline() as pipe:
            a = pipe.set("a", "a1")
            b = pipe.get("a")
            c = pipe.zadd("z", dict(z1=1))
            d = pipe.zadd("z", dict(z2=4))
            e = pipe.zincrby("z", "z1", 1)
            f = pipe.zrange("z", 0, 5, withscores=True)
        assert await gather(a, b, c, d, e, f) == (
            True,
            "a1",
            True,
            True,
            2.0,
            (("z1", 2.0), ("z2", 4)),
        )

    async def test_pipeline_no_transaction(self, client):
        async with client.pipeline(transaction=False) as pipe:
            a = pipe.set("a", "a1")
            b = pipe.set("b", "b1")
            c = pipe.set("c", "c1")
        assert await gather(a, b, c) == (
            True,
            True,
            True,
        )
        assert await client.get("a") == "a1"
        assert await client.get("b") == "b1"
        assert await client.get("c") == "c1"

    async def test_pipeline_no_permission(self, client, user_client):
        no_perm_client = await user_client("testuser", "on", "+@all", "-MULTI")
        async with no_perm_client:
            with pytest.raises(AuthorizationError):
                async with no_perm_client.pipeline(transaction=True) as pipe:
                    pipe.get("fubar")

    async def test_unwatch(self, client):
        await client.set("a{fubar}", "1")
        await client.set("b{fubar}", "2")

        async with client.pipeline() as pipe:
            async with pipe.watch("a{fubar}", "b{fubar}"):
                await client.set("b{fubar}", "3")
            res = pipe.get("a{fubar}")
        assert await res == "1"

    async def test_pipeline_transaction_with_watch(self, client):
        async with client.pipeline(transaction=False) as pipe:
            async with pipe.watch("a{fu}", "b{fu}"):
                await client.set("d{fu}", 1)
                res = pipe.set("a{fu}", 2)
        assert await res

    async def test_pipeline_transaction_with_watch_inline_fail(self, client):
        with pytest.raises(WatchError):
            async with client.pipeline(transaction=False) as pipe:
                async with pipe.watch("a{fu}", "b{fu}"):
                    await client.set("a{fu}", 1)
                    pipe.set("a{fu}", 2)

    async def test_pipeline_transaction(self, client):
        async with client.pipeline(transaction=True) as pipe:
            a = pipe.set("a{fu}", "a1")
            b = pipe.set("b{fu}", "b1")
            c = pipe.set("c{fu}", "c1")
        assert await gather(a, b, c) == (
            True,
            True,
            True,
        )
        assert await client.get("a{fu}") == "a1"
        assert await client.get("b{fu}") == "b1"
        assert await client.get("c{fu}") == "c1"

    async def test_pipeline_transaction_cross_slot(self, client):
        with pytest.raises(ClusterTransactionError):
            async with client.pipeline(transaction=True) as pipe:
                pipe.set("a{fu}", "a1")
                pipe.set("b{fu}", "b1")
                pipe.set("c{fu}", "c1")
                pipe.set("a{bar}", "fail!")
        assert await client.exists(["a{fu}", "b{fu}", "c{fu}"]) == 0
        assert await client.exists(["a{bar}"]) == 0

    async def test_pipeline_eval(self, client):
        async with client.pipeline(transaction=False) as pipe:
            eval_res = pipe.eval(
                "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
                [
                    "A{foo}",
                    "B{foo}",
                ],
                [
                    "first",
                    "second",
                ],
            )
        res = await eval_res
        assert res[0] == "A{foo}"
        assert res[1] == "B{foo}"
        assert res[2] == "first"
        assert res[3] == "second"

    async def test_exec_error_in_response(self, client):
        """
        an invalid pipeline command at exec time adds the exception instance
        to the list of returned values
        """
        await client.set("c", "a")
        async with client.pipeline(raise_on_error=False) as pipe:
            a = pipe.set("a", "1")
            b = pipe.set("b", 2)
            c = pipe.lpush("c", ["3"])
            d = pipe.set("d", "4")

        assert await a
        assert await client.get("a") == "1"
        assert await b
        assert await client.get("b") == "2"

        # we can't lpush to a key that's a string value, so this should
        # be a ResponseError exception
        assert isinstance(await c, ResponseError)
        assert await client.get("c") == "a"

        # since this isn't a transaction, the other commands after the
        # error are still executed
        assert await d
        assert await client.get("d") == "4"

    async def test_exec_error_raised(self, client):
        await client.set("c", "a")
        with pytest.raises(ResponseError) as ex:
            async with client.pipeline() as pipe:
                pipe.set("a", "1")
                pipe.set("b", "2")
                pipe.lpush("c", ["3"])
                pipe.set("d", "4")
        assert str(ex.value).startswith("Command # 3 (LPUSH c 3) of pipeline caused error: ")

    async def test_parse_error_raised(self, client):
        with pytest.raises(ResponseError) as ex:
            async with client.pipeline() as pipe:
                # the zrem is invalid because we don't pass any keys to it
                pipe.set("a", "1")
                pipe.zrem("b", [])
                pipe.set("b", "2")

        assert str(ex.value).startswith("Command # 2 (ZREM b) of pipeline caused error: ")

    @pytest.mark.parametrize("cluster_remap_keyslots", [("a{fu}", "b{fu}", "c{bar}", "d{bar}")])
    async def test_moved_error_retried(self, client, cluster_remap_keyslots, _s):
        async with client.pipeline() as pipe:
            a = pipe.set("a{fu}", 1)
            b = pipe.get("a{fu}")

        assert (True, _s("1")) == await gather(a, b)

    @pytest.mark.parametrize(
        "function, args, kwargs",
        [
            (ClusterPipeline.bgrewriteaof, (), {}),
            (ClusterPipeline.bgsave, (), {}),
            (ClusterPipeline.keys, ("*",), {}),
            (ClusterPipeline.flushdb, (), {}),
            (ClusterPipeline.flushdb, (), {}),
            (ClusterPipeline.flushall, (), {}),
        ],
    )
    async def test_no_key_command(self, client, function, args, kwargs):
        with pytest.raises(RedisClusterException) as exc:
            async with client.pipeline() as pipe:
                function(pipe, *args, **kwargs)
        exc.match("No way to dispatch (.*?) to Redis Cluster. Missing key")

    @pytest.mark.parametrize(
        "function, args, kwargs",
        [
            (ClusterPipeline.bitop, (["a{fu}"], "not", "b{bar}"), {}),
            (ClusterPipeline.brpoplpush, ("a{fu}", "b{bar}", 1.0), {}),
        ],
    )
    async def test_multi_key_cross_slot_commands(self, client, function, args, kwargs):
        with pytest.raises(ClusterCrossSlotError) as exc:
            async with client.pipeline() as pipe:
                function(pipe, *args, **kwargs)
        exc.match("Keys in request don't hash to the same slot")

    @pytest.mark.parametrize(
        "function, args, kwargs, expectation",
        [
            (ClusterPipeline.bitop, (["a{fu}"], "not", "b{fu}"), {}, 0),
            (ClusterPipeline.brpoplpush, ("a{fu}", "b{fu}", 1.0), {}, None),
        ],
    )
    async def test_multi_key_non_cross_slot(self, client, function, args, kwargs, expectation):
        async with client.pipeline() as pipe:
            pipe.set("x{fu}", 1)
            res = function(pipe, *args, **kwargs)
        assert await res == expectation
        assert await client.get("x{fu}") == "1"

    async def test_multi_node_pipeline(self, client):
        async with client.pipeline() as pipe:
            a = pipe.set("x{foo}", 1)
            b = pipe.set("x{bar}", 1)
            c = pipe.set("x{baz}", 1)
        assert (True, True, True) == await gather(a, b, c)

    async def test_multi_node_pipeline_partially_correct(self, client):
        await client.lpush("list{baz}", [1, 2, 3])
        with pytest.raises(ClusterCrossSlotError) as exc:
            async with client.pipeline() as pipe:
                pipe.set("x{foo}", 1)
                pipe.set("x{bar}", 1)

                pipe.set("x{baz}", 1)
                pipe.brpoplpush("list{baz}", "list{foo}", 1.0)
        exc.match("Keys in request don't hash to the same slot")
        assert await client.get("x{foo}") is None
        assert await client.get("x{bar}") is None
        assert await client.get("x{baz}") is None

    async def test_pipeline_timeout(self, client):
        await client.hset("hash", {str(i): bytes(1024) for i in range(1024)})
        with pytest.raises(TimeoutError):
            async with client.pipeline(timeout=0.01) as pipeline:
                for _ in range(20):
                    pipeline.hgetall("hash")
        async with client.pipeline(timeout=5) as pipeline:
            for _ in range(20):
                pipeline.hgetall("hash")
