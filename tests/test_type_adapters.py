from __future__ import annotations

import json
import pickle
from typing import Any, Iterable

import pytest

from coredis.typing import CustomInputT, TypeAdapter
from tests.conftest import targets


@targets(
    "redis_basic",
    "redis_cluster",
)
class TestTransformers:
    @pytest.fixture(autouse=True)
    async def configure_client(self, client):
        client.type_adapter = TypeAdapter()

    async def test_no_adapter(self, client):
        with pytest.raises(TypeError):
            await client.get("fubar").transform(int)
        with pytest.raises(TypeError):
            await client.set("fubar", CustomInputT(1))

    async def test_builtin_type_adapter(self, client):
        client.type_adapter.register_adapter(
            list[int], lambda v: pickle.dumps(v), lambda v: pickle.loads(v)
        )
        assert await client.set("fubar", CustomInputT([1, 2, 3]))
        assert [1, 2, 3] == await client.get("fubar").transform(list[int])

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
