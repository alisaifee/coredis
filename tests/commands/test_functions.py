from __future__ import annotations

import pytest

from coredis import PureToken, ResponseError
from tests.conftest import targets

library_definition = """#!lua name=coredis

local function fu(keys, args)
    return keys[1]
end
local function bar(keys, args)
    return 1.0*args[1]
end
redis.register_function{function_name='fu', callback=fu, flags={ 'no-writes' }}
redis.register_function('bar', bar)
"""


@pytest.fixture(autouse=True)
async def setup(client):
    await client.function_flush()


@pytest.fixture
async def simple_library(client):
    await client.function_load(library_definition)


@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_basic_resp3",
    "redis_basic_raw_resp3",
    "redis_cluster",
    "keydb",
)
@pytest.mark.asyncio
@pytest.mark.min_server_version("7.0.0")
class TestFunctions:
    async def test_empty_library(self, client, _s):
        assert await client.function_list() == {}

    async def test_load_library(self, client, _s):
        assert _s("coredis") == await client.function_load(
            library_definition,
        )
        libraries = await client.function_list()
        assert libraries["coredis"]
        assert len(libraries["coredis"]["functions"]) == 2
        stats = await client.function_stats()
        assert stats[_s("running_script")] is None

    async def test_fcall(self, client, simple_library, _s):
        assert await client.fcall("fu", ["a"], []) == _s("a")
        assert await client.fcall("bar", ["a"], [2]) == 2.0

    async def test_function_delete(self, client, simple_library, _s):
        assert _s("coredis") in await client.function_list()
        assert await client.function_delete("coredis")
        assert _s("coredis") not in await client.function_list()
        with pytest.raises(ResponseError):
            await client.function_delete("coredis")

    async def test_dump_restore(self, client, simple_library, _s):
        dump = await client.function_dump()
        assert await client.function_flush()
        assert await client.function_list() == {}
        assert await client.function_restore(dump, policy=PureToken.FLUSH)
        function_list = await client.function_list()
        assert len(function_list["coredis"]["functions"]) == 2
        assert function_list[_s("coredis")][_s("functions")][_s("fu")][_s("flags")] == {
            _s("no-writes")
        }


@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_basic_resp3",
    "redis_basic_raw_resp3",
    "redis_cluster",
)
@pytest.mark.asyncio
@pytest.mark.min_server_version("7.0.0")
class TestLibrary:
    async def test_register_library(self, client, _s):
        library = await client.register_library("coredis", library_definition)
        assert len(library.functions) == 2

    async def test_load_library(self, client, simple_library):
        library = await client.get_library("coredis")
        assert len(library.functions) == 2

    async def test_call_library_function(self, client, simple_library, _s):
        library = await client.get_library("coredis")
        assert await library["fu"](args=(1, 2, 3), keys=["A"]) == _s("A")
        assert await library["bar"](args=(1.0, 2.0, 3.0), keys=["A"]) == 1.0

    async def test_call_library_update(self, client, simple_library):
        library = await client.get_library("coredis")
        assert len(library.functions) == 2
        assert await library.update(
            """#!lua name=coredis

        local function baz(keys, args)
            return args[1] + args[2]
        end
        redis.register_function('baz', baz)
        """
        )
        assert len(library.functions) == 1
        assert await library["baz"](args=[1, 2, 3]) == 3
