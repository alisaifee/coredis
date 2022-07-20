from __future__ import annotations

import pytest

from coredis.exceptions import (
    NoScriptError,
    NotBusyError,
    RedisClusterException,
    ResponseError,
)
from tests.conftest import targets

multiply_script = """
local value = redis.call('GET', KEYS[1])
value = tonumber(value)
return value * ARGV[1]"""

multiply_and_set_script = """
local value = redis.call('GET', KEYS[1])
value = tonumber(value) * ARGV[1]
redis.call("SET", KEYS[1], value)
"""

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


@targets("redis_cluster", "redis_cluster_resp2")
@pytest.mark.asyncio()
class TestScripting:
    async def reset_scripts(self, client):
        await client.script_flush()

    async def test_eval(self, client, _s):
        await client.set("a", 2)
        # 2 * 3 == 6
        assert await client.eval(multiply_script, ["a"], [3]) == 6
        await client.eval(multiply_and_set_script, ["a"], [3])
        assert await client.get("a") == _s(6)

    @pytest.mark.min_server_version("7.0")
    async def test_eval_ro(self, cloner, client, _s):
        clone = await cloner(client, readonly=True)
        await client.set("a", 2)
        # 2 * 3 == 6
        assert await clone.eval_ro(multiply_script, ["a"], [3]) == 6
        with pytest.raises(ResponseError, match="Write commands are not allowed"):
            await clone.eval_ro(multiply_and_set_script, ["a"], [3])

    async def test_eval_same_slot(self, client):
        await client.set("A{foo}", 2)
        await client.set("B{foo}", 4)
        # 2 * 4 == 8

        script = """
        local value = redis.call('GET', KEYS[1])
        local value2 = redis.call('GET', KEYS[2])
        return value * value2
        """
        result = await client.eval(script, ["A{foo}", "B{foo}"])
        assert result == 8

    async def test_eval_crossslot(self, client):
        """
        This test assumes that {foo} and {bar} will not go to the same
        server when used. In 3 masters + 3 slaves config this should pass.
        """
        await client.set("A{foo}", 2)
        await client.set("B{bar}", 4)
        # 2 * 4 == 8

        script = """
        local value = redis.call('GET', KEYS[1])
        local value2 = redis.call('GET', KEYS[2])
        return value * value2
        """
        with pytest.raises(RedisClusterException):
            await client.eval(script, ["A{foo}", "B{bar}"])

    async def test_evalsha(self, client):
        await client.set("a", 2)
        sha = await client.script_load(multiply_script)
        # 2 * 3 == 6
        assert await client.evalsha(sha, ["a"], [3]) == 6

    @pytest.mark.parametrize("client_arguments", [({"readonly": True})])
    @pytest.mark.min_server_version("7.0")
    async def test_evalsha_ro(self, client, client_arguments, mocker):
        await client.set("a", 2)
        sha = await client.script_load(multiply_script)
        # 2 * 3 == 6
        get_primary_node_by_slot = mocker.spy(
            client.connection_pool, "get_primary_node_by_slot"
        )
        assert await client.evalsha_ro(sha, ["a"], [3]) == 6
        get_primary_node_by_slot.assert_not_called()

    async def test_evalsha_script_not_loaded(self, client):
        await client.set("a", 2)
        sha = await client.script_load(multiply_script)
        # remove the script from Redis's cache
        await client.script_flush()
        with pytest.raises(NoScriptError):
            await client.evalsha(sha, ["a"], [3])

    async def test_script_loading(self, client):
        # get the sha, then clear the cache
        sha = await client.script_load(multiply_script)
        await client.script_flush()

        assert await client.script_exists([sha]) == (False,)
        await client.script_load(multiply_script)
        assert await client.script_exists([sha]) == (True,)

    async def test_script_kill_no_scripts(self, client):
        with pytest.raises(NotBusyError):
            await client.script_kill()

    async def test_script_object(self, client):
        await client.set("a", 2)
        multiply = client.register_script(multiply_script)
        assert multiply.sha == "29cdf3e36c89fa05d7e6d6b9734b342ab15c9ea7"
        # test evalsha fail -> script load + retry
        assert await multiply(keys=["a"], args=[3]) == 6
        assert multiply.sha
        assert await client.script_exists([multiply.sha]) == (True,)
        # test first evalsha
        assert await multiply(keys=["a"], args=[3]) == 6
