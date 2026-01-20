from __future__ import annotations

import json
import pickle
from decimal import Decimal
from typing import Any

import pytest

from coredis.typing import Mapping, MutableMapping, Serializable, TypeAdapter
from tests.conftest import targets


@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_cluster",
    "redis_cluster_raw",
)
class TestTransformers:
    @pytest.fixture(autouse=True)
    async def configure_client(self, client):
        client.type_adapter = TypeAdapter()

    async def test_no_adapter(self, client):
        await client.set("fubar", 1)
        with pytest.raises(
            LookupError, match="No registered deserializer to convert (bytes|str) to int"
        ):
            await client.get("fubar").transform(int)
        with pytest.raises(LookupError, match="No registered serializer to serialize int"):
            await client.set("fubar", Serializable(1))

    @pytest.mark.runtimechecks
    async def test_conflicting_types_resolution(self, client):
        client.type_adapter.register(
            list[int], lambda v: pickle.dumps(v), lambda v: pickle.loads(v)
        )
        client.type_adapter.register(
            list[Any],
            lambda v: json.dumps(v),
            lambda v: json.loads(v.decode("utf-8") if isinstance(v, bytes) else v),
        )
        client.type_adapter.register(
            object,
            lambda v: pickle.dumps(v),
            lambda v: pickle.loads(v),
        )
        assert await client.set("intlist", Serializable([1, 2, 3]))
        assert await client.set("strlist", Serializable(["1", "2", "3"]))
        assert await client.set("mixedlist", Serializable(["1", "2", 3.0]))
        assert await client.set("strdict", Serializable({"1": "2"}))
        assert [1, 2, 3] == await client.get("intlist").transform(list[int])
        assert ["1", "2", "3"] == await client.get("strlist").transform(list[str])
        assert ["1", "2", 3.0] == await client.get("mixedlist").transform(list[str | int | float])
        assert {"1": "2"} == await client.get("strdict").transform(
            Mapping[str | bytes, str | bytes]
        )
        assert {"1": "2"} == await client.get("strdict").transform(dict[str, Any])
        with pytest.raises(TypeError):
            assert {"1": "2"} == await client.get("strdict").transform(dict[int, int])

    async def test_response_by_redis_type(self, client, _s):
        client.type_adapter.register_deserializer(
            list, lambda v: v if isinstance(v, list) else [v], object
        )
        client.type_adapter.register_deserializer(
            list,
            lambda v: [Decimal(i) if i.isnumeric() else i for i in v],
            tuple[int | float | str, ...],
        )
        client.type_adapter.register_deserializer(
            list,
            lambda v: [
                Decimal(i.decode("utf-8")) if i.decode("utf-8").isnumeric() else i for i in v
            ],
            tuple[bytes, ...],
        )
        await client.set("a{a}", 1)
        await client.set("b{a}", 2)
        await client.set("c{a}", "three")

        assert [Decimal(1), Decimal(2)] == await client.mget(["a{a}", "b{a}"]).transform(list)
        assert [Decimal(1), Decimal(2), _s("three")] == await client.mget(
            ["a{a}", "b{a}", "c{a}"]
        ).transform(list)
        assert [_s("1")] == await client.get("a{a}").transform(list)

    async def test_vague_registered_types(self, client, _s):
        client.type_adapter.register_deserializer(
            Mapping, lambda v: v if isinstance(v, dict) else {"value": v}
        )
        client.type_adapter.register_deserializer(
            MutableMapping[Any, int],
            lambda v: {k: int(v) for k, v in v.items()}
            if isinstance(v, dict)
            else {"value": int(v)},
        )

        await client.set("fubar", 1)
        await client.hset("hfubar", {"a": 1, "b": 2})
        assert {"value": _s(1)} == await client.get("fubar").transform(dict)
        assert {_s("a"): _s(1), _s("b"): _s(2)} == await client.hgetall("hfubar").transform(dict)
        assert {"value": 1} == await client.get("fubar").transform(dict[str | bytes, int])
        assert {_s("a"): 1, _s("b"): 2} == await client.hgetall("hfubar").transform(
            dict[str | bytes, int]
        )

    async def test_decorator(self, client, _s):
        @client.type_adapter.serializer
        def any_adapter(value: Any) -> str:
            return json.dumps(value)

        @client.type_adapter.serializer
        def _(value: Decimal) -> str:
            return str(value)

        @client.type_adapter.deserializer
        def _(value: str | bytes) -> Decimal:
            return Decimal(value.decode("utf-8") if isinstance(value, bytes) else value)

        @client.type_adapter.deserializer
        def _(value: int) -> Decimal:
            return Decimal(value)

        assert await client.set("fubar", Serializable(Decimal(1.23)))
        assert Decimal(1.23) == await client.get("fubar").transform(Decimal)

        assert await client.set("fubar", Serializable({1: 2}))
        assert {"1": 2} == json.loads(await client.get("fubar"))

    async def test_default_collection_deserializers(self, client, _s):
        if client.decode_responses:
            client.type_adapter.register_deserializer(Decimal, lambda v: Decimal(v), str)
        else:
            client.type_adapter.register_deserializer(
                Decimal, lambda v: Decimal(v.decode("utf-8")), bytes
            )

        await client.hset("hash", {"a": 1})
        await client.lpush("list", [1])
        await client.sadd("set", [1])
        await client.set("key", 1)

        assert {_s("a"): Decimal(1)} == await client.hgetall("hash").transform(
            dict[str if client.decode_responses else bytes, Decimal]
        )
        assert [Decimal(1)] == await client.lrange("list", 0, -1).transform(list[Decimal])
        assert {Decimal(1)} == await client.lrange("list", 0, -1).transform(set[Decimal])
        assert (Decimal(1),) == await client.lrange("list", 0, -1).transform(tuple[Decimal])
        assert (Decimal(1),) == await client.lrange("list", 0, -1).transform(tuple[Decimal, ...])
        assert [Decimal(1)] == await client.smembers("set").transform(list[Decimal])
        assert {Decimal(1)} == await client.smembers("set").transform(set[Decimal])
        assert (Decimal(1),) == await client.smembers("set").transform(tuple[Decimal])
        assert (Decimal(1),) == await client.smembers("set").transform(tuple[Decimal, ...])

    async def test_explicit_transform_callable(self, client):
        await client.set("key", 1)
        assert await client.get("key").transform(lambda value: float(value)) == 1.0

    async def test_decorator_errors(self, client):
        with pytest.raises(ValueError):

            @client.type_adapter.serializer
            def _(value) -> str:
                return str(value)

        with pytest.raises(ValueError):

            @client.type_adapter.deserializer
            def _(value: int):
                pass

        with pytest.raises(ValueError):

            @client.type_adapter.deserializer
            def _(value):
                pass
