from __future__ import annotations

from decimal import Decimal

import pytest

from coredis._concurrency import gather
from coredis.client.basic import Redis
from coredis.commands.request import CommandRequest
from coredis.exceptions import AuthorizationError, ResponseError, WatchError
from coredis.patterns.pipeline import Pipeline
from coredis.typing import Serializable
from tests.conftest import targets


@targets("redis_basic", "dragonfly", "valkey")
class TestPipeline:
    async def test_incorrect_use_pipeline(self, client):
        async with client.pipeline() as pipe:
            a = pipe.get("a")
            with pytest.raises(
                RuntimeError, match="You can't await a pipeline command before it completes"
            ):
                await a
            with pytest.raises(RuntimeError, match="Pipeline results are not available"):
                _ = pipe.results

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
        assert (
            await gather(a, b, c, d, e, f)
            == pipe.results
            == (
                True,
                "a1",
                1,
                1,
                2.0,
                (("z1", 2.0), ("z2", 4)),
            )
        )

    async def test_pipeline_transforms(self, client):
        client.type_adapter.register(
            Decimal,
            lambda v: str(v),
            lambda v: Decimal(v if isinstance(v, str) else v.decode("utf-8")),
        )
        async with client.pipeline() as pipe:
            a = pipe.set("a", Serializable(Decimal("1.23")))
            b = pipe.get("a").transform(Decimal)
        assert (True, Decimal("1.23")) == await gather(a, b)

    async def test_pipeline_no_transaction(self, client):
        async with client.pipeline(transaction=False) as pipe:
            a = pipe.set("a", "a1")
            b = pipe.set("b", "b1")
            c = pipe.set("c", "c1")
        assert await gather(a, b, c) == pipe.results == (True, True, True)
        assert await client.get("a") == "a1"
        assert await client.get("b") == "b1"
        assert await client.get("c") == "c1"

    async def test_pipeline_no_permission(self, user_client):
        no_perm_client = await user_client("testuser", "on", "+@all", "-MULTI")
        async with no_perm_client:
            with pytest.raises(AuthorizationError):
                async with no_perm_client.pipeline(transaction=False, watches=["fubar"]) as pipe:
                    pipe.get("fubar")

    async def test_pipeline_no_transaction_watch(self, client):
        await client.set("a", "0")

        async with client.pipeline(transaction=False, watches=["a"]) as pipe:
            a = await client.get("a")
            b = pipe.set("a", str(int(a) + 1))
        assert await b

    async def test_pipeline_no_transaction_watch_failure(self, client):
        await client.set("a", "0")

        with pytest.raises(WatchError):
            async with client.pipeline(transaction=False, watches=["a"]) as pipe:
                a = await client.get("a")
                await client.set("a", "bad")
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

        # we can't lpush to a key that's a string value
        with pytest.raises(ResponseError):
            await c
        assert isinstance(pipe.results[2], ResponseError)
        assert await client.get("c") == "a"

        # since this isn't a transaction, the other commands after the
        # error are still executed
        assert await d
        assert await client.get("d") == "4"

    async def test_exec_error_raised(self, client):
        await client.set("c", "a")
        with pytest.raises(ResponseError):
            async with client.pipeline() as pipe:
                a = pipe.set("a", "1")
                b = pipe.set("b", "2")
                c = pipe.lpush("c", ["3"])
                d = pipe.set("d", "4")

        assert await a
        assert await b
        with pytest.raises(
            ResponseError, match="Operation against a key holding the wrong kind of value"
        ):
            await c
        assert await d
        assert await client.get("d") == "4"

    @pytest.mark.nodragonfly
    async def test_parse_error_raised(self, client: Redis[str]):
        with pytest.raises(ResponseError):
            async with client.pipeline() as pipe:
                # the zrem is invalid because we don't pass any keys to it
                pipe.set("a", "1")
                pipe.zrem("b", [])
                pipe.set("b", "2")

    async def test_watch_succeed(self, client: Redis[str]):
        await client.set("a", "1")
        await client.set("b", "2")

        async with client.pipeline(watches=["a", "b"]) as pipe:
            a_value = await client.get("a")
            b_value = await client.get("b")
            assert a_value == "1"
            assert b_value == "2"
            res = pipe.set("c", "3")

        assert await res

    async def test_watch_failure(self, client: Redis[str]):
        await client.set("a", "1")
        await client.set("b", "2")

        with pytest.raises(WatchError):
            async with client.pipeline(watches=["a", "b"]) as pipe:
                await client.set("b", "3")
                pipe.get("a")

    async def test_unwatch(self, client: Redis[str]):
        await client.set("a", "1")
        await client.set("b", "2")

        async with client.pipeline(watches=["a", "b"]) as pipe:
            r1 = pipe.get("a")

        async with client.pipeline() as pipe:
            await client.set("b", "3")
            r2 = pipe.get("b")

        assert await r1 == "1"
        assert await r2 == "3"

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
        await client.hset("hash", {str(i): bytes(1024) for i in range(1024)})
        with pytest.raises(TimeoutError):
            async with client.pipeline(timeout=0.01) as pipe:
                for _ in range(20):
                    pipe.hgetall("hash")

        async with client.pipeline(timeout=5) as pipe:
            for _ in range(20):
                pipe.hgetall("hash")

    async def test_transaction_callable(self, client: Redis[str]):
        await client.set("a", "1")
        await client.set("b", "2")
        has_run = False

        async def my_transaction(pipe: Pipeline[str]) -> CommandRequest[bool]:
            nonlocal has_run
            a_value = await client.get("a")
            assert a_value in ("1", "2")
            b_value = await client.get("b")
            assert b_value == "2"

            # silly run-once code... incr's "a" so WatchError should be raised
            # forcing this all to run again. this should incr "a" once to "2"
            if not has_run:
                await client.incr("a")
                has_run = True

            return pipe.set("c", str(int(a_value) + int(b_value)))

        result = await client.transaction(my_transaction, "a", "b", watch_delay=0.01)
        assert await result
        assert await client.get("c") == "4"


@targets("redis_basic", "dragonfly", "valkey")
class TestPipelineConnection:
    @pytest.mark.parametrize("transaction", [False, True])
    async def test_pipeline_exit_no_exceptions(self, client: Redis[str], transaction: bool):
        await client.set("a", "0")

        in_use = client.connection_pool.statistics.in_use_connections
        async with client.pipeline(transaction=transaction) as pipe:
            assert client.connection_pool.statistics.in_use_connections == in_use
            pipe.get("a")

        assert client.connection_pool.statistics.in_use_connections == in_use

    @pytest.mark.parametrize("transaction", [False, True])
    async def test_pipeline_exit_response_error(self, client: Redis[str], transaction: bool):
        await client.set("a", "0")

        in_use = client.connection_pool.statistics.in_use_connections
        with pytest.raises(ResponseError):
            async with client.pipeline(transaction=transaction) as pipe:
                assert client.connection_pool.statistics.in_use_connections == in_use
                pipe.llen("a")

        assert client.connection_pool.statistics.in_use_connections == in_use

    @pytest.mark.parametrize("transaction", [False, True])
    async def test_pipeline_exit_body_error(self, client: Redis[str], transaction: bool):
        await client.set("a", "0")

        in_use = client.connection_pool.statistics.in_use_connections
        with pytest.raises(RuntimeError, match="body error"):
            async with client.pipeline(transaction=transaction, watches=["a"]):
                assert client.connection_pool.statistics.in_use_connections == in_use + 1
                raise RuntimeError("body error")

        assert client.connection_pool.statistics.in_use_connections == in_use

    @pytest.mark.parametrize("transaction", [False, True])
    async def test_pipeline_exit_watch_error(self, client: Redis[str], transaction: bool):
        await client.set("a", "0")

        in_use = client.connection_pool.statistics.in_use_connections
        with pytest.raises(WatchError):
            async with client.pipeline(transaction=transaction, watches=["a"]) as pipe:
                assert client.connection_pool.statistics.in_use_connections == in_use + 1
                await client.set("a", "-1")
                pipe.set("a", "1")

        assert client.connection_pool.statistics.in_use_connections == in_use

    @pytest.mark.parametrize("client_arguments", [{"max_connections": 1}])
    async def test_pipeline_conn_reuse(self, client: Redis[str], client_arguments: dict[str, int]):
        client.connection_pool.timeout = 1

        await client.set("a", "0")

        with pytest.raises(ResponseError):
            async with client.pipeline() as pipe:
                pipe.llen("a")

        await client.get("a")

    @pytest.mark.parametrize("client_arguments", [{"max_connections": 1}])
    async def test_pipeline_conn_clear(self, client: Redis[str], client_arguments: dict[str, int]):
        client.connection_pool.timeout = 1

        await client.set("a", "0")

        with pytest.raises(RuntimeError):
            async with client.pipeline(watches=["a"]):
                raise RuntimeError

        await client.set("a", "-1")

        async with client.pipeline() as pipe:
            pipe.set("a", "1")

    async def test_commands_after_watch_form_separate_batch(self, client: Redis[str]):
        await client.set("a", "0")

        async with client.pipeline(transaction=False, watches=["a"]) as pipe:
            pipe.set("a", "1")

        async with client.pipeline(transaction=False) as pipe:
            after = pipe.get("a")

        assert await after == "1"

    async def test_outer_body_error_after_successful_watch(self, client: Redis[str]):
        await client.set("a", "0")
        in_use = client.connection_pool.statistics.in_use_connections
        async with client.pipeline(transaction=False, watches=["a"]) as pipe:
            pipe.set("a", "1")
        with pytest.raises(RuntimeError, match="after watch"):
            async with client.pipeline(transaction=False):
                raise RuntimeError("after watch")
        assert client.connection_pool.statistics.in_use_connections == in_use
        assert await client.get("a") == "1"

    @pytest.mark.parametrize("client_arguments", [{"max_connections": 1}])
    async def test_pipeline_client_cmds(self, client: Redis[str], client_arguments: dict[str, int]):
        client.connection_pool.timeout = 1

        await client.set("a", "0")
        async with client.pipeline(transaction=False, watches=["a"]) as pipe:
            value = await pipe.client.get("a")
            pipe.set("a", int(value) + 1)
