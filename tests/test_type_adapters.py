from __future__ import annotations

import json
import pickle
from decimal import Decimal
from typing import Any, Iterable, Mapping, MutableMapping

import pytest

from coredis.typing import CustomInputT, TypeAdapter
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
        with pytest.raises(TypeError, match="No type adapter registered to deserialize"):
            await client.get("fubar").transform(int)
        with pytest.raises(TypeError, match="No type adapter registered to serialize"):
            await client.set("fubar", CustomInputT(1))

    async def test_conflicting_types_resolution(self, client):
        client.type_adapter.register_adapter(
            list[int], lambda v: pickle.dumps(v), lambda v: pickle.loads(v)
        )
        client.type_adapter.register_adapter(
            list[Any],
            lambda v: json.dumps(v),
            lambda v: json.loads(v.decode("utf-8") if isinstance(v, bytes) else v),
        )
        client.type_adapter.register_adapter(
            object,
            lambda v: pickle.dumps(v),
            lambda v: pickle.loads(v),
        )
        assert await client.set("intlist", CustomInputT([1, 2, 3]))
        assert await client.set("strlist", CustomInputT(["1", "2", "3"]))
        assert await client.set("mixedlist", CustomInputT(["1", "2", 3.0]))
        assert await client.set("dict", CustomInputT({"1": "2"}))
        assert [1, 2, 3] == await client.get("intlist").transform(list[int])
        assert ["1", "2", "3"] == await client.get("strlist").transform(list[str])
        assert ["1", "2", 3.0] == await client.get("mixedlist").transform(list[str | int | float])
        assert {"1": "2"} == await client.get("dict").transform(Iterable)
        assert {"1": "2"} == await client.get("dict").transform(dict[str, Any])
        with pytest.raises(TypeError):
            assert {"1": "2"} == await client.get("dict").transform(dict[int, int])

    async def test_response_by_redis_type(self, client, _s):
        client.type_adapter.register_response_adapter(
            list, lambda v: v if isinstance(v, list) else [v], object
        )
        client.type_adapter.register_response_adapter(
            list,
            lambda v: [Decimal(i) if i.isnumeric() else i for i in v],
            tuple[int | float | str, ...],
        )
        client.type_adapter.register_response_adapter(
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
        client.type_adapter.register_response_adapter(
            Mapping, lambda v: v if isinstance(v, dict) else {"value": v}
        )
        client.type_adapter.register_response_adapter(
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
