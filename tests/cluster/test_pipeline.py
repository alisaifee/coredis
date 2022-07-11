from __future__ import annotations

import asyncio

import pytest

from coredis.exceptions import (
    AuthorizationError,
    ClusterCrossSlotError,
    ClusterTransactionError,
    RedisClusterException,
    ResponseError,
    WatchError,
)
from coredis.pipeline import ClusterPipelineImpl
from tests.conftest import targets


@pytest.mark.asyncio()
@targets("redis_cluster", "redis_cluster_resp2")
class TestPipeline:
    async def test_pipeline(self, client):
        async with await client.pipeline() as pipe:
            await (
                await (await (await pipe.set("a", "a1")).get("a")).zadd("z", dict(z1=1))
            ).zadd("z", dict(z2=4))
            await (await pipe.zincrby("z", "z1", 1)).zrange("z", 0, 5, withscores=True)
            assert await pipe.execute() == (
                True,
                "a1",
                True,
                True,
                2.0,
                (("z1", 2.0), ("z2", 4)),
            )

    async def test_pipeline_length(self, client):
        async with await client.pipeline() as pipe:
            # Initially empty.
            assert len(pipe) == 0
            assert pipe

            # Fill 'er up!
            await (await (await pipe.set("a", "a1")).set("b", "b1")).set("c", "c1")
            assert len(pipe) == 3
            assert pipe

            # Execute calls reset(), so empty once again.
            await pipe.execute()
            assert len(pipe) == 0
            assert pipe

    async def test_pipeline_no_transaction(self, client):
        async with await client.pipeline(transaction=False) as pipe:
            await (await (await pipe.set("a", "a1")).set("b", "b1")).set("c", "c1")
            assert await pipe.execute() == (
                True,
                True,
                True,
            )
            assert await client.get("a") == "a1"
            assert await client.get("b") == "b1"
            assert await client.get("c") == "c1"

    async def test_pipeline_invalid_flow(self, client):
        async with await client.pipeline(transaction=False) as pipe:
            with pytest.raises(RedisClusterException):
                pipe.multi()

    async def test_pipeline_no_permission(self, client, user_client):
        no_perm_client = await user_client("testuser", "on", "+@all", "-MULTI")
        async with await no_perm_client.pipeline(transaction=True) as pipe:
            await pipe.get("fubar")
            with pytest.raises(AuthorizationError):
                await pipe.execute()

    @pytest.mark.xfail
    async def test_pipeline_transaction_with_watch_on_construction(self, client):
        pipe = await client.pipeline(transaction=True, watches=["a{fu}"])

        async def overwrite():
            i = 0
            while True:
                try:
                    await client.set("a{fu}", i)
                except asyncio.CancelledError:
                    break
                except Exception:
                    break

        [await pipe.set("a{fu}", -1 * i) for i in range(1000)]

        task = asyncio.create_task(overwrite())
        try:
            await asyncio.sleep(0.1)
            with pytest.raises(WatchError):
                await pipe.execute()
        finally:
            task.cancel()

    async def test_pipeline_transaction_with_watch_not_implemented(self, client):
        async with await client.pipeline(transaction=True) as pipe:
            with pytest.raises(NotImplementedError):
                await pipe.watch("a{fu}")

    async def test_pipeline_transaction(self, client):
        async with await client.pipeline(transaction=True) as pipe:
            await (await (await pipe.set("a{fu}", "a1")).set("b{fu}", "b1")).set(
                "c{fu}", "c1"
            )
            assert await pipe.execute() == (
                True,
                True,
                True,
            )
            assert await client.get("a{fu}") == "a1"
            assert await client.get("b{fu}") == "b1"
            assert await client.get("c{fu}") == "c1"

    async def test_pipeline_transaction_cross_slot(self, client):
        with pytest.raises(ClusterTransactionError):
            async with await client.pipeline(transaction=True) as pipe:
                await (await (await pipe.set("a{fu}", "a1")).set("b{fu}", "b1")).set(
                    "c{fu}", "c1"
                )
                await pipe.set("a{bar}", "fail!")
                await pipe.execute()
        assert await client.exists(["a{fu}", "b{fu}", "c{fu}"]) == 0
        assert await client.exists(["a{bar}"]) == 0

    async def test_pipeline_eval(self, client):
        async with await client.pipeline(transaction=False) as pipe:
            await pipe.eval(
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
            res = (await pipe.execute())[0]
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
        async with await client.pipeline() as pipe:
            await (
                await (await (await pipe.set("a", "1")).set("b", "2")).lpush("c", "3")
            ).set("d", "4")
            result = await pipe.execute(raise_on_error=False)

            assert result[0]
            assert await client.get("a") == "1"
            assert result[1]
            assert await client.get("b") == "2"

            # we can't lpush to a key that's a string value, so this should
            # be a ResponseError exception
            assert isinstance(result[2], ResponseError)
            assert await client.get("c") == "a"

            # since this isn't a transaction, the other commands after the
            # error are still executed
            assert result[3]
            assert await client.get("d") == "4"

            # make sure the pipe was restored to a working state
            await pipe.set("z", "zzz")
            assert await pipe.execute() == (True,)
            assert await client.get("z") == "zzz"

    async def test_exec_error_raised(self, client):
        await client.set("c", "a")
        async with await client.pipeline() as pipe:
            await pipe.set("a", "1")
            await pipe.set("b", "2")
            await pipe.lpush("c", "3")
            await pipe.set("d", "4")
            with pytest.raises(ResponseError) as ex:
                await pipe.execute()
            assert str(ex.value).startswith(
                "Command # 3 (LPUSH c 3) of " "pipeline caused error: "
            )

            # make sure the pipe was restored to a working state
            await pipe.set("z", "zzz")
            assert await pipe.execute() == (True,)
            assert await client.get("z") == "zzz"

    async def test_parse_error_raised(self, client):
        async with await client.pipeline() as pipe:
            # the zrem is invalid because we don't pass any keys to it
            await (await (await pipe.set("a", "1")).zrem("b", [])).set("b", "2")
            with pytest.raises(ResponseError) as ex:
                await pipe.execute()

            assert str(ex.value).startswith(
                "Command # 2 (ZREM b) of pipeline caused error: "
            )

            # make sure the pipe was restored to a working state
            assert await (await pipe.set("z", "zzz")).execute() == (True,)
            assert await client.get("z") == "zzz"

    @pytest.mark.parametrize(
        "function, args, kwargs",
        [
            (ClusterPipelineImpl.bgrewriteaof, (), {}),
            (ClusterPipelineImpl.bgsave, (), {}),
            (ClusterPipelineImpl.keys, ("*",), {}),
            (ClusterPipelineImpl.flushdb, (), {}),
            (ClusterPipelineImpl.flushdb, (), {}),
            (ClusterPipelineImpl.flushall, (), {}),
        ],
    )
    async def test_no_key_command(self, client, function, args, kwargs):
        with pytest.raises(RedisClusterException) as exc:
            async with await client.pipeline() as pipe:
                await function(pipe, *args, **kwargs)
                await pipe.execute()
        exc.match("No way to dispatch (.*?) to Redis Cluster. Missing key")

    @pytest.mark.parametrize(
        "function, args, kwargs",
        [
            (ClusterPipelineImpl.bitop, (["a{fu}"], "not", "b{bar}"), {}),
            (ClusterPipelineImpl.brpoplpush, ("a{fu}", "b{bar}", 1.0), {}),
        ],
    )
    async def test_multi_key_cross_slot_commands(self, client, function, args, kwargs):
        with pytest.raises(ClusterCrossSlotError) as exc:
            async with await client.pipeline() as pipe:
                await function(pipe, *args, **kwargs)
                await pipe.execute()
        exc.match("Keys in request don't hash to the same slot")

    @pytest.mark.parametrize(
        "function, args, kwargs, expectation",
        [
            (ClusterPipelineImpl.bitop, (["a{fu}"], "not", "b{fu}"), {}, (0,)),
            (ClusterPipelineImpl.brpoplpush, ("a{fu}", "b{fu}", 1.0), {}, (None,)),
        ],
    )
    async def test_multi_key_non_cross_slot(
        self, client, function, args, kwargs, expectation
    ):
        async with await client.pipeline() as pipe:
            await pipe.set("x{fu}", 1)
            await function(pipe, *args, **kwargs)
            res = await pipe.execute()
        assert res == (True,) + expectation
        assert await client.get("x{fu}") == "1"

    async def test_multi_node_pipeline(self, client):
        async with await client.pipeline() as pipe:
            await pipe.set("x{foo}", 1)
            await pipe.set("x{bar}", 1)
            await pipe.set("x{baz}", 1)
            res = await pipe.execute()
        assert res == (True, True, True)

    async def test_multi_node_pipeline_partially_correct(self, client):
        await client.lpush("list{baz}", [1, 2, 3])
        with pytest.raises(ClusterCrossSlotError) as exc:
            async with await client.pipeline() as pipe:
                await pipe.set("x{foo}", 1)
                await pipe.set("x{bar}", 1)

                await pipe.set("x{baz}", 1)
                await pipe.brpoplpush("list{baz}", "list{foo}", 1.0)
                await pipe.execute()
        exc.match("Keys in request don't hash to the same slot")
        assert await client.get("x{foo}") is None
        assert await client.get("x{bar}") is None
        assert await client.get("x{baz}") is None

    @pytest.mark.flaky
    async def test_transaction_callable(self, client, cloner):
        clone = await cloner(client)

        async def _incr():
            for i in range(10):
                await clone.incr("a{fubar}")

        await client.set("a{fubar}", "1")
        await client.set("b{fubar}", "2")

        async def my_transaction(pipe):
            await asyncio.sleep(0)
            a_value = await client.get("a{fubar}")
            b_value = await client.get("b{fubar}")
            await pipe.set("c{fubar}", str(int(a_value) + int(b_value)))

        results = await asyncio.gather(
            client.transaction(
                my_transaction, "a{fubar}", "b{fubar}", watch_delay=0.01
            ),
            _incr(),
        )
        assert results[0] == (True,)
        assert int(await client.get("c{fubar}")) > 3
