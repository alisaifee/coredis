from __future__ import annotations

import pytest

from coredis import PureToken
from coredis.commands.function import Library
from coredis.exceptions import NotBusyError, ResponseError
from coredis.typing import KeyT, List, StringT, ValueT
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
    "redis_basic_blocking",
    "redis_basic_raw",
    "redis_basic_resp2",
    "redis_basic_raw_resp2",
    "redis_cluster",
    "redis_cluster_raw",
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
    @pytest.mark.parametrize("client_arguments", [({"readonly": True})])
    async def test_fcall_ro(self, client, simple_library, _s, client_arguments, mocker):
        get_primary_node_by_slot = mocker.spy(
            client.connection_pool, "get_primary_node_by_slot"
        )
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
            print(await client.function_kill())

    async def test_dump_restore(self, client, simple_library, _s):
        dump = await client.function_dump()
        assert await client.function_flush(async_=PureToken.SYNC)
        assert await client.function_list() == {}
        assert await client.function_restore(dump, policy=PureToken.FLUSH)
        function_list = await client.function_list()
        assert len(function_list["coredis"]["functions"]) == 4
        assert function_list[_s("coredis")][_s("functions")][_s("echo_key")][
            _s("flags")
        ] == {_s("no-writes")}


@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_basic_resp2",
    "redis_basic_raw_resp2",
    "redis_cluster",
    "redis_cluster_raw",
)
@pytest.mark.asyncio
@pytest.mark.min_server_version("7.0.0")
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

    @pytest.mark.parametrize("client_arguments", [({"readonly": True})])
    @pytest.mark.clusteronly
    async def test_call_library_function_ro(
        self, client, simple_library, _s, client_arguments, mocker
    ):
        fcall = mocker.spy(client, "fcall")
        library = await client.get_library("coredis")
        assert await library["echo_key"](args=(1, 2, 3), keys=["A"]) == _s("A")
        fcall.assert_not_called()

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

    async def test_subclass_wrap(selfself, client, simple_library, _s):
        class Coredis(Library):
            def __init__(self, client):
                super().__init__(client, "coredis")

            @Library.wraps("echo_key")
            async def echo_key(self, key: KeyT) -> StringT:
                ...

            @Library.wraps("return_arg")
            async def return_arg(self, value: ValueT) -> ValueT:
                ...

            @Library.wraps("default_get")
            async def default_get(self, key: KeyT, value: ValueT) -> ValueT:
                ...

            @Library.wraps("default_get", key_spec=["quay"])
            async def default_get_variadic(self, quay: str, *values: ValueT) -> ValueT:
                ...

            @Library.wraps("hmmerge")
            async def hmmerge(self, key: KeyT, **values: ValueT) -> List[ValueT]:
                ...

        lib = await Coredis(client)
        assert await lib.echo_key("bar") == _s("bar")
        assert await lib.return_arg(1) == 10
        assert await lib.default_get("bar", "fu") == _s("fu")
        assert await lib.default_get_variadic("bar", "fu", "bar", "baz") == _s(
            "fubarbaz"
        )
        assert await client.set("bar", "fubar")
        assert await lib.default_get_variadic("bar", "fu", "bar", "baz") == _s("fubar")
        await client.hset("hbar", {"fu": "whut?"})
        assert await lib.hmmerge("hbar", fu="bar", bar="fu", baz="fubar") == [
            _s("whut?"),
            _s("fu"),
            _s("fubar"),
        ]

    @pytest.mark.parametrize("client_arguments", [({"readonly": True})])
    @pytest.mark.clusteronly
    async def test_subclass_wrap_ro_defaults(
        selfself, client, simple_library, _s, client_arguments, mocker
    ):
        class Coredis(Library):
            def __init__(self, client):
                super().__init__(client, "coredis")

            @Library.wraps("echo_key")
            async def echo_key(self, key: KeyT) -> StringT:
                ...

            @Library.wraps("return_arg")
            async def return_arg(self, value: ValueT) -> ValueT:
                ...
                ...

        fcall = mocker.spy(client, "fcall")
        fcall_ro = mocker.spy(client, "fcall_ro")

        lib = await Coredis(client)
        assert await lib.echo_key("bar") == _s("bar")
        assert await lib.return_arg(1) == 10

        assert fcall.call_count == 1
        assert fcall_ro.call_count == 1

    @pytest.mark.parametrize("client_arguments", [({"readonly": True})])
    @pytest.mark.clusteronly
    async def test_subclass_wrap_ro_forced(
        selfself, client, simple_library, _s, client_arguments, mocker
    ):
        class Coredis(Library):
            def __init__(self, client):
                super().__init__(client, "coredis")

            @Library.wraps("echo_key", readonly=False)
            async def echo_key(self, key: KeyT) -> StringT:
                ...

            @Library.wraps("echo_key", readonly=True)
            async def echo_key_ro(self, key: KeyT) -> StringT:
                ...

            @Library.wraps("return_arg", readonly=False)
            async def return_arg(self, value: ValueT) -> ValueT:
                ...

            @Library.wraps("return_arg", readonly=True)
            async def return_arg_ro(self, value: ValueT) -> ValueT:
                ...

        fcall = mocker.spy(client, "fcall")
        fcall_ro = mocker.spy(client, "fcall_ro")
        lib = await Coredis(client)
        assert await lib.echo_key("bar") == _s("bar")
        assert await lib.echo_key_ro("bar") == _s("bar")
        assert await lib.return_arg(1) == 10
        with pytest.raises(ResponseError):
            await lib.return_arg_ro(1) == 10

        assert fcall.call_count == 2
        assert fcall_ro.call_count == 2
