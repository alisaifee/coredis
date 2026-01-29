from __future__ import annotations

import pytest

from coredis import PureToken
from coredis.commands.function import Library, wraps
from coredis.commands.request import CommandRequest
from coredis.exceptions import FunctionError, NotBusyError, ResponseError
from coredis.typing import KeyT, RedisValueT, StringT
from tests.conftest import targets

library_definition = """#!lua name=coredis

local function echo_key(keys, args)
    return keys[1]
end
local function return_arg(keys, args)
    return 10*args[1]
end
local function default_get(keys, args)
    return redis.call("get", keys[1]) or table.concat(args)
end
local function hmmerge(keys, args)
    local fields = {}
    local i = 1
    local j = 1
    while args[i] do
        fields[i] = args[i]
        i = i + 2
        j = j + 1
    end
    local values = redis.call("HMGET", keys[1], unpack(fields))
    i = 1
    j = 1
    while args[i] do
        if not values[j] then
            values[j] = args[i+1]
        end
        i = i + 2
        j = j + 1
    end
    return values
end

redis.register_function{function_name='echo_key', callback=echo_key, flags={ 'no-writes' }}
redis.register_function('return_arg', return_arg)
redis.register_function('default_get', default_get)
redis.register_function('hmmerge', hmmerge)
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
    "redis_cluster",
    "redis_cluster_raw",
    "valkey",
)
class TestFunctions:
    async def test_empty_library(self, client, _s):
        assert await client.function_list() == {}

    async def test_load_library(self, client, _s):
        assert _s("coredis") == await client.function_load(
            library_definition,
        )
        libraries = await client.function_list(withcode=True)
        assert libraries["coredis"]
        assert len(libraries["coredis"]["functions"]) == 4
        assert libraries["coredis"]["library_code"] == _s(library_definition)
        stats = await client.function_stats()
        assert stats[_s("running_script")] is None

    async def test_fcall(self, client, simple_library, _s):
        assert await client.fcall("echo_key", ["a"], []) == _s("a")
        assert await client.fcall("return_arg", ["a"], [2]) == 20

    @pytest.mark.clusteronly
    @pytest.mark.parametrize("client_arguments", [{"read_from_replicas": True}])
    async def test_fcall_ro(self, client, simple_library, _s, client_arguments, mocker):
        get_primary_node_by_slot = mocker.spy(client.connection_pool, "get_primary_node_by_slots")
        await client.fcall_ro("echo_key", ["a"], []) == _s("a")
        with pytest.raises(ResponseError):
            await client.fcall_ro("return_arg", ["a"], [2])
        get_primary_node_by_slot.assert_not_called()

    async def test_function_delete(self, client, simple_library, _s):
        assert _s("coredis") in await client.function_list()
        assert await client.function_delete("coredis")
        assert _s("coredis") not in await client.function_list()
        with pytest.raises(ResponseError):
            await client.function_delete("coredis")

    async def test_function_kill(self, client, simple_library, _s):
        with pytest.raises(NotBusyError):
            await client.function_kill()

    async def test_dump_restore(self, client, simple_library, _s):
        dump = await client.function_dump()
        assert await client.function_flush(async_=PureToken.SYNC)
        assert await client.function_list() == {}
        assert await client.function_restore(dump, policy=PureToken.FLUSH)
        function_list = await client.function_list()
        assert len(function_list["coredis"]["functions"]) == 4
        assert function_list[_s("coredis")][_s("functions")][_s("echo_key")][_s("flags")] == {
            _s("no-writes")
        }


@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_cluster",
    "redis_cluster_raw",
)
class TestLibrary:
    async def test_register_library(self, client, _s):
        library = await client.register_library("coredis", library_definition)
        assert len(library.functions) == 4

    async def test_library_constructor(self, client, _s, mocker):
        function_load = mocker.spy(client, "function_load")
        library = await Library(client, "coredis", library_definition)
        assert len(library.functions) == 4
        assert function_load.call_count == 1
        library = await Library(client, "coredis", library_definition)
        assert len(library.functions) == 4
        assert function_load.call_count == 1
        library = await Library(client, "coredis", library_definition, replace=True)
        assert len(library.functions) == 4
        assert function_load.call_count == 2

    async def test_load_library(self, client, simple_library):
        library = await client.get_library("coredis")
        assert len(library.functions) == 4

    async def test_call_library_function(self, client, simple_library, _s):
        library = await client.get_library("coredis")
        assert await library["echo_key"](args=(1, 2, 3), keys=["A"]) == _s("A")
        assert await library["return_arg"](args=(1.0, 2.0, 3.0), keys=["A"]) == 10

    @pytest.mark.parametrize("client_arguments", [{"readonly": True}])
    async def test_call_library_function_ro(
        self, client, simple_library, _s, client_arguments, mocker
    ):
        fcall = mocker.spy(client, "fcall")
        library = await client.get_library("coredis")
        assert await library["echo_key"](args=(1, 2, 3), keys=["A"]) == _s("A")
        fcall.assert_not_called()

    @pytest.mark.nocluster
    async def test_call_library_update(self, client, simple_library):
        library = await client.get_library("coredis")
        assert len(library.functions) == 4
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

    async def test_missing_library(self, client):
        class Missing(Library):
            def __init__(self, client):
                super().__init__(client, name="missing")

        with pytest.raises(FunctionError, match="No library found for missing"):
            await Missing(client)

        with pytest.raises(FunctionError, match="No library found for missing"):
            async with client.pipeline() as pipeline:
                await Missing(pipeline)

    async def test_missing_function(self, client, simple_library):
        class Coredis(Library):
            def __init__(self, client):
                super().__init__(client, name="coredis")

            @wraps()
            def fail(self, key: KeyT) -> CommandRequest[None]: ...

        with pytest.raises(AttributeError, match="has no registered function fail"):
            lib = await Coredis(client)
            await lib.fail("test")

        with pytest.raises(AttributeError, match="has no registered function fail"):
            async with client.pipeline() as pipeline:
                lib = await Coredis(pipeline)
                lib.fail("test")

    async def test_subclass_wrap(self, client, simple_library, _s):
        class Coredis(Library):
            def __init__(self, client):
                super().__init__(client, "coredis")

            @wraps(readonly=True)
            def echo_key(self, key: KeyT) -> CommandRequest[StringT]: ...

            @wraps()
            def return_arg(self, value: RedisValueT) -> CommandRequest[RedisValueT]: ...

            @wraps()
            def default_get(
                self, key: KeyT, *values: RedisValueT
            ) -> CommandRequest[RedisValueT]: ...

            @wraps(function_name="default_get", callback=lambda value: int(value))
            def default_get_int(self, key: KeyT, *values: RedisValueT) -> CommandRequest[int]: ...

            @wraps()
            def hmmerge(
                self, key: KeyT, **values: RedisValueT
            ) -> CommandRequest[list[RedisValueT]]: ...

        lib = await Coredis(client)
        assert await lib.echo_key("bar") == _s("bar")
        assert await lib.return_arg(1) == 10
        assert await lib.default_get("bar", "fu") == _s("fu")
        assert await lib.default_get("bar", "fu", "bar", "baz") == _s("fubarbaz")
        assert await client.set("bar", "fubar")
        assert await lib.default_get("bar", "fu", "bar", "baz") == _s("fubar")
        assert await lib.default_get_int("int", "1", "2", "3") == 123
        assert await client.set("int", "10")
        assert await lib.default_get_int("int", "1", "2", "3") == 10
        await client.hset("hbar", {"fu": "whut?"})
        assert await lib.hmmerge("hbar", fu="bar", bar="fu", baz="fubar") == [
            _s("whut?"),
            _s("fu"),
            _s("fubar"),
        ]

    @pytest.mark.parametrize("client_arguments", [{"readonly": True}])
    async def test_subclass_wrap_ro_defaults(
        self, client, simple_library, _s, client_arguments, mocker
    ):
        class Coredis(Library):
            def __init__(self, client):
                super().__init__(client, "coredis")

            @wraps(readonly=True)
            def echo_key(self, key: KeyT) -> CommandRequest[StringT]: ...

            @wraps()
            def return_arg(self, value: RedisValueT) -> CommandRequest[RedisValueT]: ...

        fcall = mocker.spy(client, "fcall")
        fcall_ro = mocker.spy(client, "fcall_ro")

        lib = await Coredis(client)
        assert await lib.echo_key("bar") == _s("bar")
        assert await lib.return_arg(1) == 10

        assert fcall.call_count == 1
        assert fcall_ro.call_count == 1

    @pytest.mark.parametrize("client_arguments", [{"readonly": True}])
    async def test_subclass_wrap_ro_forced(
        self, client, simple_library, _s, client_arguments, mocker
    ):
        class Coredis(Library):
            def __init__(self, client):
                super().__init__(client, "coredis")

            @wraps(readonly=True)
            def echo_key(self, key: KeyT) -> CommandRequest[StringT]: ...

            @wraps(readonly=True)
            def return_arg(self, value: RedisValueT) -> CommandRequest[RedisValueT]: ...

        fcall = mocker.spy(client, "fcall")
        fcall_ro = mocker.spy(client, "fcall_ro")
        lib = await Coredis(client)
        assert await lib.echo_key("bar") == _s("bar")
        with pytest.raises(ResponseError):
            await lib.return_arg(1) == 10

        assert fcall.call_count == 0
        assert fcall_ro.call_count == 2

    async def test_pipeline_execution(self, client, simple_library, _s):
        class Coredis(Library):
            def __init__(self, client):
                super().__init__(client, "coredis")

            @wraps()
            def default_get(
                self, key: KeyT, *values: RedisValueT
            ) -> CommandRequest[RedisValueT]: ...
            @wraps(function_name="default_get", callback=lambda value: int(value))
            def default_int_get(
                self, key: KeyT, *values: RedisValueT
            ) -> CommandRequest[RedisValueT]: ...

        async with client.pipeline() as pipeline:
            lib = await Coredis(pipeline)
            r1 = lib.default_get("bar", "fu", "bar")
            r2 = lib.default_int_get("bar", 1, 2)

        assert await r1 == _s("fubar")
        assert await r2 == 12

    async def test_skip_verify(self, client, simple_library, _s):
        class Coredis(Library):
            def __init__(self, client):
                super().__init__(client, "coredis")

            @wraps(readonly=True, verify_existence=False)
            def echo_key(self, key: KeyT) -> CommandRequest[StringT]: ...

            @wraps(verify_existence=True)
            def return_arg(self, value: RedisValueT) -> CommandRequest[RedisValueT]: ...

            @wraps(verify_existence=False)
            def nonexistent(self) -> CommandRequest[RedisValueT]: ...

        lib = Coredis(client)
        assert await lib.echo_key("bar") == _s("bar")
        with pytest.raises(AttributeError, match="no registered function"):
            assert await lib.return_arg(1) == 10
        with pytest.raises(ResponseError):
            assert await lib.nonexistent()
