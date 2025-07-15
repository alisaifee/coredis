from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from coredis.exceptions import (
    AuthorizationError,
    RedisError,
    ResponseError,
    TimeoutError,
    WatchError,
)
from coredis.typing import Serializable
from tests.conftest import targets


@targets(
    "redis_basic",
    "redis_basic_blocking",
    "dragonfly",
    "valkey",
    "redict",
)
class TestPipeline:
    async def test_empty_pipeline(self, client):
        async with await client.pipeline() as pipe:
            assert await pipe.execute() == ()

    async def test_pipeline(self, client):
        async with await client.pipeline() as pipe:
            pipe.set("a", "a1")
            pipe.get("a")
            pipe.zadd("z", dict(z1=1))
            pipe.zadd("z", dict(z2=4))
            pipe.zincrby("z", "z1", 1)
            pipe.zrange("z", 0, 5, withscores=True)
            assert await pipe.execute() == (
                True,
                "a1",
                1,
                1,
                2.0,
                (("z1", 2.0), ("z2", 4)),
            )

    async def test_pipeline_transforms(self, client, _s):
        client.type_adapter.register(
            Decimal,
            lambda v: str(v),
            lambda v: Decimal(v if isinstance(v, str) else v.decode("utf-8")),
        )
        pipe = await client.pipeline()
        pipe.set("a", Serializable(Decimal(1.23)))
        r = pipe.get("a").transform(Decimal)
        assert (True, _s(str(Decimal(1.23)))) == await pipe.execute()
        assert Decimal(1.23) == await r

    async def test_pipeline_length(self, client):
        async with await client.pipeline() as pipe:
            # Initially empty.
            assert len(pipe) == 0
            assert pipe

            # Fill 'er up!
            pipe.set("a", "a1")
            pipe.set("b", "b1")
            pipe.set("c", "c1")
            assert len(pipe) == 3
            assert pipe

            # Execute calls reset(), so empty once again.
            await pipe.execute()
            assert len(pipe) == 0
            assert pipe

    async def test_pipeline_no_transaction(self, client):
        async with await client.pipeline(transaction=False) as pipe:
            pipe.set("a", "a1")
            pipe.set("b", "b1")
            pipe.set("c", "c1")
            assert await pipe.execute() == (True, True, True)
            assert await client.get("a") == "a1"
            assert await client.get("b") == "b1"
            assert await client.get("c") == "c1"

    async def test_pipeline_invalid_flow(self, client):
        pipe = await client.pipeline(transaction=False)
        pipe.multi()
        with pytest.raises(RedisError):
            pipe.multi()

        pipe = await client.pipeline(transaction=False)
        pipe.multi()
        with pytest.raises(RedisError):
            pipe.watch("test")

        pipe = await client.pipeline(transaction=False)
        pipe.set("fubar", 1)
        with pytest.raises(RedisError):
            pipe.multi()

    @pytest.mark.nodragonfly
    async def test_pipeline_no_permission(self, client, user_client):
        no_perm_client = await user_client("testuser", "on", "+@all", "-MULTI")
        async with await no_perm_client.pipeline(transaction=False) as pipe:
            pipe.multi()
            pipe.get("fubar")
            with pytest.raises(AuthorizationError):
                await pipe.execute()

    async def test_pipeline_no_transaction_watch(self, client):
        await client.set("a", "0")

        async with await client.pipeline(transaction=False) as pipe:
            await pipe.watch("a")
            a = await pipe.get("a")

            pipe.multi()
            pipe.set("a", str(int(a) + 1))
            assert await pipe.execute() == (True,)

    async def test_pipeline_no_transaction_watch_failure(self, client):
        await client.set("a", "0")

        async with await client.pipeline(transaction=False) as pipe:
            await pipe.watch("a")
            a = await pipe.get("a")

            await client.set("a", "bad")

            pipe.multi()
            pipe.set("a", str(int(a) + 1))

            with pytest.raises(WatchError):
                await pipe.execute()

            assert await client.get("a") == "bad"

    async def test_exec_error_in_response(self, client):
        """
        an invalid pipeline command at exec time adds the exception instance
        to the list of returned values
        """
        await client.set("c", "a")
        async with await client.pipeline() as pipe:
            pipe.set("a", "1")
            pipe.set("b", "2")
            pipe.lpush("c", ["3"])
            pipe.set("d", "4")
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
            pipe.set("z", "zzz")
            assert await pipe.execute() == (True,)
            assert await client.get("z") == "zzz"

    async def test_exec_error_in_response_explicit_transaction(self, client):
        """
        an invalid pipeline command at exec time adds the exception instance
        to the list of returned values
        """
        await client.set("c", "a")
        async with await client.pipeline(transaction=False) as pipe:
            pipe.multi()
            pipe.set("a", "1")
            pipe.set("b", "2")
            pipe.lpush("c", ["3"])
            pipe.set("d", "4")
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
            pipe.set("z", "zzz")
            assert await pipe.execute() == (True,)
            assert await client.get("z") == "zzz"

    async def test_exec_error_raised(self, client):
        await client.set("c", "a")
        async with await client.pipeline() as pipe:
            pipe.set("a", "1")
            pipe.set("b", "2")
            pipe.lpush("c", ["3"])
            pipe.set("d", "4")
            with pytest.raises(ResponseError):
                await pipe.execute()

            # make sure the pipe was restored to a working state
            pipe.set("z", "zzz")
            assert await pipe.execute() == (True,)
            assert await client.get("z") == "zzz"

    async def test_exec_error_raised_explicit_transaction(self, client):
        await client.set("c", "a")
        async with await client.pipeline(transaction=False) as pipe:
            pipe.multi()
            pipe.set("a", "1")
            pipe.set("b", "2")
            pipe.lpush("c", ["3"])
            pipe.set("d", "4")
            with pytest.raises(ResponseError):
                await pipe.execute()

            # make sure the pipe was restored to a working state
            pipe.set("z", "zzz")
            assert await pipe.execute() == (True,)
            assert await client.get("z") == "zzz"

    @pytest.mark.nodragonfly
    async def test_parse_error_raised(self, client):
        async with await client.pipeline() as pipe:
            # the zrem is invalid because we don't pass any keys to it
            pipe.set("a", "1")
            pipe.zrem("b", [])
            pipe.set("b", "2")
            with pytest.raises(ResponseError):
                await pipe.execute()

            # make sure the pipe was restored to a working state
            pipe.set("z", "zzz")
            assert await pipe.execute() == (True,)
            assert await client.get("z") == "zzz"

    @pytest.mark.nodragonfly
    async def test_parse_error_raised_explicit_transaction(self, client):
        async with await client.pipeline(transaction=False) as pipe:
            pipe.multi()
            # the zrem is invalid because we don't pass any keys to it
            pipe.set("a", "1")
            pipe.zrem("b", [])
            pipe.set("b", "2")
            with pytest.raises(ResponseError):
                await pipe.execute()

            # make sure the pipe was restored to a working state
            pipe.set("z", "zzz")
            assert await pipe.execute() == (True,)
            assert await client.get("z") == "zzz"

    async def test_watch_succeed(self, client):
        await client.set("a", "1")
        await client.set("b", "2")

        async with await client.pipeline() as pipe:
            await pipe.watch("a", "b")
            assert pipe.watching
            a_value = await pipe.get("a")
            b_value = await pipe.get("b")
            assert a_value == "1"
            assert b_value == "2"
            pipe.multi()

            pipe.set("c", "3")
            assert await pipe.execute() == (True,)
            assert not pipe.watching

    async def test_watch_failure(self, client):
        await client.set("a", "1")
        await client.set("b", "2")

        async with await client.pipeline() as pipe:
            await pipe.watch("a", "b")
            await client.set("b", "3")
            pipe.multi()
            pipe.get("a")
            with pytest.raises(WatchError):
                await pipe.execute()

            assert not pipe.watching

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

        [pipe.set("a{fu}", -1 * i) for i in range(1000)]

        task = asyncio.create_task(overwrite())
        try:
            await asyncio.sleep(0.1)
            with pytest.raises(WatchError):
                await pipe.execute()
        finally:
            task.cancel()

    async def test_unwatch(self, client):
        await client.set("a", "1")
        await client.set("b", "2")

        async with await client.pipeline() as pipe:
            await pipe.watch("a", "b")
            await client.set("b", "3")
            await pipe.unwatch()
            assert not pipe.watching
            pipe.get("a")
            assert await pipe.execute() == ("1",)

    async def test_transaction_callable(self, client):
        await client.set("a", "1")
        await client.set("b", "2")
        has_run = []

        async def my_transaction(pipe):
            a_value = await pipe.get("a")
            assert a_value in ("1", "2")
            b_value = await pipe.get("b")
            assert b_value == "2"

            # silly run-once code... incr's "a" so WatchError should be raised
            # forcing this all to run again. this should incr "a" once to "2"

            if not has_run:
                await client.incr("a")
                has_run.append("it has")

            pipe.multi()
            pipe.set("c", str(int(a_value) + int(b_value)))

        result = await client.transaction(my_transaction, "a", "b", watch_delay=0.01)
        assert result == (True,)
        assert await client.get("c") == "4"

    async def test_exec_error_in_no_transaction_pipeline(self, client):
        await client.set("a", "1")
        async with await client.pipeline(transaction=False) as pipe:
            pipe.llen("a")
            pipe.expire("a", 100)

            with pytest.raises(ResponseError):
                await pipe.execute()

        assert await client.get("a") == "1"

    async def test_exec_error_in_no_transaction_pipeline_unicode_command(self, client):
        key = chr(11) + "abcd" + chr(23)
        await client.set(key, "1")
        async with await client.pipeline(transaction=False) as pipe:
            pipe.llen(key)
            pipe.expire(key, 100)

            with pytest.raises(ResponseError):
                await pipe.execute()

        assert await client.get(key) == "1"

    async def test_pipeline_timeout(self, client):
        await client.hset("hash", {str(i): i for i in range(4096)})
        await client.ping()
        pipeline = await client.pipeline(timeout=0.01)
        for i in range(20):
            pipeline.hgetall("hash")
        with pytest.raises(TimeoutError):
            await pipeline.execute()

        await client.ping()
        pipeline = await client.pipeline(timeout=5)
        for i in range(20):
            pipeline.hgetall("hash")
        await pipeline.execute()
