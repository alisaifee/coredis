from __future__ import annotations

import struct

import pytest

from coredis import PureToken
from coredis.exceptions import CommandSyntaxError, ResponseError
from tests.conftest import targets


@pytest.fixture
async def sample_data(client):
    vectors = {
        "a1": {"group": "a", "data": [1.0, 0.0, 0.0]},
        "a2": {"group": "a", "data": [0.99, 0.1, 0.0]},
        "a3": {"group": "a", "data": [0.95, 0.1, 0.0]},
        "b1": {"group": "b", "data": [0.0, 1.0, 0.0]},
        "b2": {"group": "b", "data": [0.0, 0.99, 0.1]},
        "b3": {"group": "b", "data": [0.0, 0.95, 0.1]},
        "c1": {"group": "c", "data": [0.0, 0.0, 1.0]},
        "c2": {"group": "c", "data": [0.0, 0.1, 0.99]},
        "c3": {"group": "c", "data": [0.0, 0.1, 0.95]},
        "outlier": {"group": "nether", "data": [5.0, 5.0, 5.0]},
    }

    for key, value in vectors.items():
        await client.vadd("sample", key, value["data"], attributes={"group": value["group"]})
    return vectors


@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_cluster",
    "redis_cluster_raw",
)
@pytest.mark.min_server_version("8.0.0")
class TestVectorSets:
    @pytest.mark.parametrize("quantization", [None, PureToken.NOQUANT, PureToken.BIN, PureToken.Q8])
    async def test_vadd_new_set(self, client, _s, quantization):
        assert await client.vadd("test", "element", [1, 2, 3], quantization=quantization)
        assert not await client.vadd("test", "element", [1, 2, 3], quantization=quantization)

    async def test_vadd_mismatched_set_element(self, client, _s):
        assert await client.vadd("test", "element", [1, 2, 3], quantization=PureToken.Q8)
        with pytest.raises(ResponseError, match="dimension mismatch"):
            await client.vadd("test", "element-2", [1, 2, 3, 4])
        with pytest.raises(ResponseError, match="quantization mismatch"):
            await client.vadd("test", "element-2", [1, 2, 3], quantization=PureToken.BIN)

    async def test_vadd_update_element(self, client, sample_data, _s):
        assert (_s("a1"), _s("a2")) == await client.vsim("sample", element="a1", count=2)
        await client.vadd("sample", "a1", [3, 3, 3])
        assert (_s("a1"), _s("outlier")) == await client.vsim("sample", element="a1", count=2)

    async def test_vrem(self, client, _s):
        assert await client.vadd("test", "element", [1, 2, 3])
        assert await client.vadd("test", "element-2", [1, 2, 3])
        assert 2 == await client.vcard("test")
        assert await client.vrem("test", "element-2")
        assert not await client.vrem("test", "element-2")
        assert 1 == await client.vcard("test")
        assert not await client.vrem("missing", "missing")

    async def test_vlinks(self, client, sample_data, _s):
        assert None is await client.vlinks("missing", "missing")
        assert None is await client.vlinks("sample", "nether")
        links = await client.vlinks("sample", "a1")
        assert (
            _s("a2"),
            _s("a3"),
        ) == links[-1][:2]
        links_with_scores = await client.vlinks("sample", "a1", withscores=True)
        assert pytest.approx(0.99, 1e-2) == links_with_scores[-1][_s("a2")]
        assert pytest.approx(0.99, 1e-2) == links_with_scores[-1][_s("a3")]

    @pytest.mark.min_server_version("8.2")
    async def test_vsim(self, client, sample_data, _s):
        assert () == await client.vsim("missing", element="missing")
        with pytest.raises(ResponseError, match="element not found"):
            await client.vsim("sample", element="nether")
        with pytest.raises(CommandSyntaxError, match="One of .* must be provided"):
            await client.vsim("sample")

        similar_elements = await client.vsim("sample", element="a1")
        assert len(similar_elements) == 10
        assert (_s("a1"), _s("a2"), _s("a3")) == similar_elements[:3]

        assert (_s("a1"), _s("a2"), _s("a3")) == await client.vsim("sample", element="a1", count=3)
        assert (_s("a1"), _s("a2"), _s("a3")) == await client.vsim(
            "sample", values=sample_data["a1"]["data"], count=3
        )
        assert (_s("a1"), _s("a2"), _s("a3")) == await client.vsim(
            "sample", values=struct.pack("<fff", *sample_data["a1"]["data"]), count=3
        )

        with pytest.raises(ResponseError, match="invalid EF"):
            await client.vsim("sample", element="a1", ef=0)

        assert (_s("a1"), _s("a2"), _s("a3")) == await client.vsim(
            "sample", element="a1", count=3, ef=1000
        )

        similarity_with_scores = await client.vsim("sample", element="a1", withscores=True)
        assert len(similarity_with_scores) == 10
        assert 1.0 == similarity_with_scores[_s("a1")]
        assert pytest.approx(0.99, 1e-2) == similarity_with_scores[_s("a2")]
        assert pytest.approx(0.99, 1e-2) == similarity_with_scores[_s("a3")]

        with pytest.raises(ResponseError, match="invalid FILTER-EF"):
            assert (
                len(await client.vsim("sample", element="a1", filter=".group=='a'", filter_ef=0))
                == 3
            )
        assert len(await client.vsim("sample", element="a1", filter=".group=='a'")) == 3
        assert len(await client.vsim("sample", element="a1", filter=".group=='a'", filter_ef=1)) < 3

        assert (_s("a1"), _s("a2"), _s("a3")) == await client.vsim(
            "sample", values=sample_data["a1"]["data"], count=10, epsilon=0.2
        )
        assert (_s("a1"),) == await client.vsim(
            "sample", values=sample_data["a1"]["data"], count=10, epsilon=0.001
        )

        assert (_s("c1"), _s("c2"), _s("c3")) != (await client.vsim("sample", element="a1"))[-3:]
        assert (_s("c1"), _s("c2"), _s("c3")) == (
            await client.vsim("sample", element="a1", truth=True)
        )[-3:]

    @pytest.mark.min_server_version("8.2")
    async def test_vsim_withattribs(self, client, sample_data, _s):
        similarity_with_attribs = await client.vsim("sample", element="a1", withattribs=True)
        assert similarity_with_attribs[_s("a1")] == {"group": "a"}
        similarity_with_attribs_and_scores = await client.vsim(
            "sample", element="a1", withattribs=True, withscores=True
        )
        assert similarity_with_attribs_and_scores[_s("a1")] == (1.0, {"group": "a"})

    async def test_attributes(self, client, _s):
        attributes = {"a": 1, "b": [1, 2, 3], "c": {"4": 5, "6": 7}}
        assert await client.vadd("test", "element", [1, 2, 3], attributes=attributes)
        for i in range(2, 5):
            assert await client.vadd(
                "test", f"element-{i}", [1, 2, 3], attributes={**attributes, **{"a": i}}
            )
        assert attributes == await client.vgetattr("test", "element")
        assert await client.vsetattr("test", "element", [1, 2, 3])
        assert [1, 2, 3] == await client.vgetattr("test", "element")
        assert await client.vsetattr("test", "element", None)
        assert None is await client.vgetattr("test", "element")
        assert not await client.vsetattr("test", "missing", [1, 2, 3])
        assert not await client.vsetattr("missing", "missing", [1, 2, 3])
        assert None is await client.vgetattr("test", "missing")
        assert None is await client.vgetattr("missing", "missing")

    async def test_vemb(self, client, sample_data, _s):
        assert (1.0, 0.0, 0.0) == await client.vemb("sample", "a1")
        vector_data = await client.vemb("sample", "a1", raw=True)
        assert vector_data["quantization"] == "int8"
        assert (127, 0, 0) == struct.unpack("bbb", vector_data["blob"])
        assert None is await client.vemb("missing", "missing")
        assert None is await client.vemb("sample", "missing")

    async def test_vdim(self, client, _s):
        await client.vadd("1", "element", values=[1])
        await client.vadd("2", "element", values=[1, 1])
        await client.vadd("3", "element", values=[1, 1, 1])
        await client.vadd("1024", "element", values=[1] * 1024)

        assert await client.vdim("1") == 1
        assert await client.vdim("2") == 2
        assert await client.vdim("3") == 3
        assert await client.vdim("1024") == 1024

        await client.vrem("1", "element")
        with pytest.raises(ResponseError, match="key does not exist"):
            await client.vdim("1")

        with pytest.raises(ResponseError, match="key does not exist"):
            await client.vdim("missing")

    async def test_vinfo(self, client, sample_data, _s):
        info = await client.vinfo("sample")
        assert _s("int8") == info[_s("quant-type")]
        assert 3 == info[_s("vector-dim")]
        assert 10 == info[_s("size")]
        assert 10 == info[_s("attributes-count")]
        assert None is await client.vinfo("missing")

    async def test_vrandmember(self, client, sample_data, _s):
        all_elements = {_s(k) for k in sample_data.keys()}
        assert await client.vrandmember("sample") in all_elements
        assert set(await client.vrandmember("sample", count=5)) & all_elements
        assert set(await client.vrandmember("sample", count=100)) == all_elements
        assert None is await client.vrandmember("missing")
        assert () == await client.vrandmember("missing", 10)

    @pytest.mark.min_server_version("8.4")
    async def test_vrange(self, client, sample_data, _s):
        all_elements = {_s(k) for k in sample_data.keys()}
        assert all_elements == set(await client.vrange("sample", "-", "+"))
        assert (_s("a1"), _s("a2")) == await client.vrange("sample", "-", "+", 2)
        assert (_s("a1"), _s("a2"), _s("a3")) == await client.vrange("sample", "[a", "(b")

    @pytest.mark.min_server_version("8.2")
    async def test_vismember(self, client, sample_data, _s):
        assert await client.vismember("sample", "a1")
        assert not await client.vismember("sample", "x1")
        assert not await client.vismember("xample", "x1")
