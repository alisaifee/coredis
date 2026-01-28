from __future__ import annotations

import pytest
from beartype.roar import BeartypeCallHintParamViolation

from coredis import PureToken
from coredis._concurrency import gather
from coredis.client import Client
from coredis.client.basic import Redis
from coredis.commands import CommandRequest, Script
from coredis.exceptions import NoScriptError, NotBusyError, ResponseError
from coredis.typing import AnyStr, KeyT, RedisValueT
from tests.conftest import targets

multiply_script = """
local value = redis.call('GET', KEYS[1])
value = tonumber(value)

return value * ARGV[1]"""

msgpack_hello_script = """
local message = cmsgpack.unpack(ARGV[1])
local name = message['name']

return "hello " .. name
"""
msgpack_hello_script_broken = """
local message = cmsgpack.unpack(ARGV[1])
local names = message['name']

return "hello " .. name
"""

loop = """
local aTempKey = "a-temp-key{foo}"
local cycles
redis.call("SET",aTempKey,"1")
redis.call("PEXPIRE",aTempKey, 10)

for i = 0, ARGV[1], 1 do
    local keyExists = redis.call("EXISTS",aTempKey)
    cycles = i;
    if keyExists == 0 then
        break;
    end
end

return cycles
"""


@pytest.fixture(autouse=True)
async def flush_scripts(client):
    await client.script_flush()


@targets("redis_basic")
class TestScripting:
    async def test_eval(self, client):
        await client.set("a", "2")
        # 2 * 3 == 6
        assert await client.eval(multiply_script, keys=["a"], args=[3]) == 6

    async def test_evalsha(self, client):
        await client.set("a", "2")
        sha = await client.script_load(multiply_script)
        # 2 * 3 == 6
        assert await client.evalsha(sha, keys=["a"], args=[3]) == 6

    async def test_evalsha_script_not_loaded(self, client):
        await client.set("a", "2")
        sha = await client.script_load(multiply_script)
        # remove the script from Redis's cache
        await client.script_flush()
        with pytest.raises(NoScriptError):
            await client.evalsha(sha, keys=["a"], args=[3])

    async def test_script_loading(self, client):
        # get the sha, then clear the cache
        sha = await client.script_load(multiply_script)
        await client.script_flush()
        assert await client.script_exists([sha]) == (False,)
        await client.script_load(multiply_script)
        assert await client.script_exists([sha]) == (True,)

    async def test_script_flush_sync_mode(self, client):
        sha = await client.script_load(multiply_script)
        assert await client.script_flush(sync_type=PureToken.SYNC)
        assert await client.script_exists([sha]) == (False,)

    async def test_script_object(self, client: Redis[str]):
        await client.set("a", "2")
        multiply = client.register_script(multiply_script)
        precalculated_sha = multiply.sha
        assert precalculated_sha
        assert await client.script_exists([multiply.sha]) == (False,)
        # Test second evalsha block (after NoScriptError)
        assert await multiply(keys=["a"], args=[3]) == 6
        # At this point, the script should be loaded
        assert await client.script_exists([multiply.sha]) == (True,)
        # Test that the precalculated sha matches the one from redis
        assert multiply.sha == precalculated_sha
        # Test first evalsha block
        assert await multiply(keys=["a"], args=[3]) == 6

    async def test_script_object_in_pipeline(self, client: Redis[str]):
        multiply = client.register_script(multiply_script)
        precalculated_sha = multiply.sha
        assert precalculated_sha
        async with client.pipeline() as pipe:
            a = pipe.set("a", "2")
            b = pipe.get("a")
            c = multiply(keys=["a"], args=[3], client=pipe)
            assert await client.script_exists([multiply.sha]) == (False,)
        # [SET worked, GET 'a', result of multiple script]
        assert await gather(a, b, c) == (True, "2", 6)
        # The script should have been loaded by pipe.execute()
        assert await client.script_exists([multiply.sha]) == (True,)
        # The precalculated sha should have been the correct one
        assert multiply.sha == precalculated_sha

        # purge the script from redis's cache and re-run the pipeline
        # the multiply script should be reloaded by pipe.execute()
        await client.script_flush()
        async with client.pipeline() as pipe:
            a = pipe.set("a", "2")
            b = pipe.get("a")
            c = multiply(keys=["a"], args=[3], client=pipe)
            assert await client.script_exists([multiply.sha]) == (False,)
        # [SET worked, GET 'a', result of multiple script]
        assert await gather(a, b, c) == (True, "2", 6)
        assert await client.script_exists([multiply.sha]) == (True,)

    async def testscript_flush_eval_msgpack_pipeline_error_in_lua(self, client):
        msgpack_hello = client.register_script(msgpack_hello_script)
        assert msgpack_hello.sha

        # avoiding a dependency to msgpack, this is the output of
        # msgpack.dumps({"name": "joe"})
        msgpack_message_1 = b"\x81\xa4name\xa3Joe"
        async with client.pipeline() as pipe:
            res = msgpack_hello(args=[msgpack_message_1], client=pipe)
            assert await client.script_exists([msgpack_hello.sha]) == (False,)

        assert await res == "hello Joe"
        assert await client.script_exists([msgpack_hello.sha]) == (True,)

        msgpack_hello_broken = client.register_script(msgpack_hello_script_broken)

        with pytest.raises(ResponseError) as excinfo:
            async with client.pipeline() as pipe:
                msgpack_hello_broken(args=[msgpack_message_1], client=pipe)
            assert excinfo.type == ResponseError

    async def test_script_kill_no_scripts(self, client):
        with pytest.raises(NotBusyError):
            await client.script_kill()

    async def test_wraps_function_no_args(self, client, cloner, _s):
        no_args = client.register_script("return 'PONG'")

        @no_args.wraps()
        def pong() -> CommandRequest[bytes | str]: ...

        assert await pong() == _s("PONG")

    async def test_wraps_function_no_keys(self, client):
        no_key_script = client.register_script(
            "return {tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])}"
        )

        @no_key_script.wraps()
        def synth_no_key(*args: int) -> CommandRequest[list[int]]: ...

        assert await synth_no_key(1, 2, 3) == [1, 2, 3]

    async def test_wraps_function_key_only(self, client):
        no_key_script = client.register_script("return redis.call('GET', KEYS[1])")
        await client.set("co", "redis")

        @no_key_script.wraps()
        def synth_key_only(key: KeyT) -> CommandRequest[str]: ...

        assert await synth_key_only(key="co") == "redis"

    async def test_wraps_function_key_value(self, client):
        script = client.register_script("return redis.call('GET', KEYS[1]) or ARGV[1]")

        @script.wraps()
        def default_get(key: KeyT, default: RedisValueT) -> CommandRequest[RedisValueT]: ...

        await client.set("key", "redis")
        await default_get("key", "coredis") == "redis"
        await client.delete(["key"])
        await default_get("key", "coredis") == "coredis"

    async def test_wraps_function_with_callback(self, client):
        script = client.register_script("return redis.call('GET', KEYS[1]) or ARGV[1]")

        @script.wraps(callback=lambda value: int(value))
        def int_get(key: KeyT, default: RedisValueT) -> CommandRequest[int]: ...

        await client.set("key", 1)
        await int_get("key", 2) == 1
        await client.delete(["key"])
        await int_get("key", 2) == 2

    async def test_wraps_function_key_value_type_checking(self, client):
        script = client.register_script("return redis.call('GET', KEYS[1]) or ARGV[1]")

        @script.wraps(runtime_checks=True)
        def default_get(key: KeyT, default: str) -> CommandRequest[str]: ...

        await client.set("key", "redis")
        assert await default_get("key", "coredis") == "redis"
        await client.delete(["key"])
        assert await default_get("key", "coredis") == "coredis"
        with pytest.raises(BeartypeCallHintParamViolation):
            await default_get("key", 1)

    @pytest.mark.runtimechecks
    async def test_wraps_class_method(self, client):
        scrpt = Script(None, "return redis.call('GET', KEYS[1]) or ARGV[1]")

        class Wrapper:
            @classmethod
            @scrpt.wraps(client_arg="client", runtime_checks=True)
            def default_get(
                cls,
                client: Client[AnyStr] | None,
                key: KeyT,
                default: str = "coredis",
            ) -> CommandRequest[str]: ...

        await client.set("key", "redis")
        await Wrapper.default_get(client, "key", "coredis") == "redis"
        await client.delete(["key"])
        await Wrapper.default_get(client, "key", "coredis") == "coredis"
        await Wrapper.default_get(client, "key") == "coredis"
        with pytest.raises(BeartypeCallHintParamViolation):
            await Wrapper.default_get(client, "key", 1) == "coredis"
