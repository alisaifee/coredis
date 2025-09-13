from __future__ import annotations

from decimal import Decimal

import pytest

from coredis._utils import gather
from coredis.client.basic import Redis
from coredis.exceptions import (
    AuthorizationError,
    RedisError,
    ResponseError,
    TimeoutError,
    WatchError,
)
from coredis.typing import Serializable
from tests.conftest import targets

pytestmark = pytest.mark.anyio


@targets(
    "redis_basic",
    "redis_basic_blocking",
    "dragonfly",
    "valkey",
    "redict",
)
class TestPipeline:
    async def test_empty_pipeline(self, client):
        async with client.pipeline():
            pass

    async def test_pipeline(self, client: Redis[str]):
        async with client.pipeline() as pipe:
            a = pipe.set("a", "a1")
            b = pipe.get("a")
            c = pipe.zadd("z", {"z1": 1})
            d = pipe.zadd("z", {"z2": 4})
            e = pipe.zincrby("z", "z1", 1)
            f = pipe.zrange("z", 0, 5, withscores=True)
        assert await gather(a, b, c, d, e, f) == (
            True,
            "a1",
            1,
            1,
            2.0,
            (("z1", 2.0), ("z2", 4)),
        )

    async def test_pipeline_transforms(self, client):
        client.type_adapter.register(
            Decimal,
            lambda v: str(v),
            lambda v: Decimal(v if isinstance(v, str) else v.decode("utf-8")),
        )
        async with client.pipeline() as pipe:
            a = pipe.set("a", Serializable(Decimal(1.23)))
            b = pipe.get("a").transform(Decimal)
        assert (True, Decimal(1.23)) == await gather(a, b)

    async def test_pipeline_length(self, client):
        async with client.pipeline() as pipe:
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
        assert len(pipe) == 0
        assert pipe

    async def test_pipeline_no_transaction(self, client):
        async with client.pipeline(transaction=False) as pipe:
            a = pipe.set("a", "a1")
            b = pipe.set("b", "b1")
            c = pipe.set("c", "c1")
        assert await gather(a, b, c) == (True, True, True)
        assert await client.get("a") == "a1"
        assert await client.get("b") == "b1"
        assert await client.get("c") == "c1"

    async def test_pipeline_invalid_flow(self, client):
        async with client.pipeline(transaction=False) as pipe:
            pipe.multi()
            with pytest.raises(RedisError):
                pipe.multi()

        async with client.pipeline(transaction=False) as pipe:
            pipe.multi()
            with pytest.raises(RedisError):
                await pipe.watch("test")

        async with client.pipeline(transaction=False) as pipe:
            pipe.set("fubar", 1)
            with pytest.raises(RedisError):
                pipe.multi()

    @pytest.mark.nodragonfly
    async def test_pipeline_no_permission(self, user_client):
        no_perm_client = await user_client("testuser", "on", "+@all", "-MULTI")
        async with no_perm_client:
            with pytest.raises(AuthorizationError):
                async with no_perm_client.pipeline(transaction=False) as pipe:
                    pipe.multi()
                    pipe.get("fubar")

    async def test_pipeline_no_transaction_watch(self, client):
        await client.set("a", "0")

        async with client.pipeline(transaction=False) as pipe:
            await pipe.watch("a")
            a = await pipe.get("a")
            pipe.multi()
            b = pipe.set("a", str(int(a) + 1))
        assert await b

    async def test_pipeline_no_transaction_watch_failure(self, client):
        await client.set("a", "0")

        with pytest.raises(WatchError):
            async with client.pipeline(transaction=False) as pipe:
                await pipe.watch("a")
                a = await pipe.get("a")

                await client.set("a", "bad")

                pipe.multi()
                pipe.set("a", str(int(a) + 1))

        assert await client.get("a") == "bad"

    async def test_exec_error_in_response(self, client: Redis[str]):
        """
        an invalid pipeline command at exec time adds the exception instance
        to the list of returned values
        """
        await client.set("c", "a")
        async with client.pipeline(raise_on_error=False, transaction=False) as pipe:
            a = pipe.set("a", "1")
            b = pipe.set("b", "2")
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

    async def test_exec_error_in_response_explicit_transaction(self, client: Redis[str]):
        """
        an invalid pipeline command at exec time adds the exception instance
        to the list of returned values
        """
        await client.set("c", "a")
        async with client.pipeline(raise_on_error=False, transaction=False) as pipe:
            pipe.multi()
            a = pipe.set("a", "1")
            b = pipe.set("b", "2")
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
        with pytest.raises(ResponseError):
            async with client.pipeline() as pipe:
                pipe.set("a", "1")
                pipe.set("b", "2")
                pipe.lpush("c", ["3"])
                pipe.set("d", "4")

    async def test_exec_error_raised_explicit_transaction(self, client):
        await client.set("c", "a")
        with pytest.raises(ResponseError):
            async with client.pipeline(transaction=False) as pipe:
                pipe.multi()
                pipe.set("a", "1")
                pipe.set("b", "2")
                pipe.lpush("c", ["3"])
                pipe.set("d", "4")

    @pytest.mark.nodragonfly
    async def test_parse_error_raised(self, client: Redis[str]):
        with pytest.raises(ResponseError):
            async with client.pipeline() as pipe:
                # the zrem is invalid because we don't pass any keys to it
                pipe.set("a", "1")
                pipe.zrem("b", [])
                pipe.set("b", "2")

    @pytest.mark.nodragonfly
    async def test_parse_error_raised_explicit_transaction(self, client: Redis[str]):
        with pytest.raises(ResponseError):
            async with client.pipeline(transaction=False) as pipe:
                pipe.multi()
                # the zrem is invalid because we don't pass any keys to it
                pipe.set("a", "1")
                pipe.zrem("b", [])
                pipe.set("b", "2")

    async def test_watch_succeed(self, client: Redis[str]):
        await client.set("a", "1")
        await client.set("b", "2")

        async with client.pipeline() as pipe:
            await pipe.watch("a", "b")
            assert pipe.watching
            a_value = await pipe.get("a")
            b_value = await pipe.get("b")
            assert a_value == "1"
            assert b_value == "2"
            pipe.multi()

            res = pipe.set("c", "3")

        assert await res
        assert not pipe.watching

    async def test_watch_failure(self, client: Redis[str]):
        await client.set("a", "1")
        await client.set("b", "2")

        with pytest.raises(WatchError):
            async with client.pipeline() as pipe:
                await pipe.watch("a", "b")
                await client.set("b", "3")
                pipe.multi()
                pipe.get("a")

    async def test_unwatch(self, client: Redis[str]):
        await client.set("a", "1")
        await client.set("b", "2")

        async with client.pipeline() as pipe:
            await pipe.watch("a", "b")
            await client.set("b", "3")
            await pipe.unwatch()
            assert not pipe.watching
            res = pipe.get("a")
        assert await res == "1"

    async def test_transaction_callable(self, client: Redis[str]):
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
        assert result
        assert await client.get("c") == "4"

    async def test_exec_error_in_no_transaction_pipeline(self, client: Redis[str]):
        await client.set("a", "1")
        with pytest.raises(ResponseError):
            async with client.pipeline(transaction=False) as pipe:
                pipe.llen("a")
                pipe.expire("a", 100)

        assert await client.get("a") == "1"

    async def test_exec_error_in_no_transaction_pipeline_unicode_command(self, client: Redis[str]):
        key = chr(11) + "abcd" + chr(23)
        await client.set(key, "1")
        with pytest.raises(ResponseError):
            async with client.pipeline(transaction=False) as pipe:
                pipe.llen(key)
                pipe.expire(key, 100)

        assert await client.get(key) == "1"

    async def test_pipeline_timeout(self, client: Redis[str]):
        await client.hset("hash", {str(i): i for i in range(4096)})
        await client.ping()
        with pytest.raises(TimeoutError):
            async with client.pipeline(timeout=0.01) as pipe:
                for _ in range(20):
                    pipe.hgetall("hash")

        await client.ping()
        async with client.pipeline(timeout=5) as pipe:
            for _ in range(20):
                pipe.hgetall("hash")
