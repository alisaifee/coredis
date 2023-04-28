from __future__ import annotations

import pytest

from coredis import PureToken, Redis
from coredis.exceptions import ResponseError
from tests.conftest import targets


@pytest.fixture
async def seed(client):
    json_object = {
        "int": 1,
        "string": "string",
        "intlist": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "stringlist": [
            "a",
            "b",
            "c",
            "d",
            "e",
            "f",
            "g",
        ],
        "mixedlist": [1, 2, 3, 4, 5, "6", "7", "8", "9", "10"],
        "object": {
            "int": 2,
            "string": "substring",
            "intlist": [2, 3, 4, 5, 6, 7, 8, 9, 10],
            "stringlist": [
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
            ],
            "mixedlist": [2, 3, 4, 5, "6", "7", "8", "9", "10"],
        },
    }
    await client.json.set("seed", ".", json_object)
    return json_object


@targets(
    "redis_stack",
    "redis_stack_resp2",
    "redis_stack_cached",
    "redis_stack_cluster",
)
class TestJson:
    async def test_get(self, client: Redis, seed):
        assert seed == await client.json.get("seed")
        assert seed["int"] == await client.json.get("seed", ".int")
        assert seed["string"] == await client.json.get("seed", ".string")
        assert seed["intlist"] == await client.json.get("seed", ".intlist")
        assert seed["mixedlist"] == await client.json.get("seed", ".mixedlist")
        assert seed["object"] == await client.json.get("seed", ".object")
        assert [
            seed["mixedlist"],
            seed["object"]["mixedlist"],
        ] == await client.json.get("seed", "$..mixedlist")

    async def test_forget(self, client: Redis, seed):
        assert await client.json.forget("seed", "$.intlist") == 1
        assert await client.json.forget("seed", "$.intlist") == 0
        assert not await client.json.get("seed", "$.intlist")
        assert await client.json.forget("seed") == 1
        assert not await client.exists(["seed"])

    async def test_set(self, client: Redis):
        obj = {"foo": "bar"}
        assert await client.json.set("obj", ".", obj)

        # Test that flags prevent updates when conditions are unmet
        assert not (await client.json.set("obj", "foo", "baz", condition=PureToken.NX))
        assert not (await client.json.set("obj", "qaz", "baz", condition=PureToken.XX))

        # Test that flags allow updates when conditions are met
        assert await client.json.set("obj", "foo", "baz", condition=PureToken.XX)
        assert await client.json.set("obj", "qaz", "baz", condition=PureToken.NX)

    async def test_type(self, client: Redis):
        await client.json.set("1", ".", 1)
        assert "integer" == await client.json.type("1", ".")
        assert "integer" == await client.json.type("1")
        jdata, jtypes = self.load_types_data("a")
        await client.json.set("doc1", "$", jdata)
        # Test multi
        assert await client.json.type("doc1", "$..a") == jtypes

        # Test single
        assert await client.json.type("doc1", "$.nested2.a") == [jtypes[1]]

        # Test missing key
        assert await client.json.type("non_existing_doc", "..a") is None

    async def test_numincrby(self, client: Redis, seed):
        assert 2 == await client.json.numincrby("seed", ".int", 1)
        assert 2.5 == await client.json.numincrby("seed", ".int", 0.5)
        assert 1.25 == await client.json.numincrby("seed", ".int", -1.25)
        assert [10001.25, 10002] == await client.json.numincrby("seed", "$..int", 10000)

    async def test_nummultby(self, client: Redis, seed):
        assert 2 == await client.json.nummultby("seed", ".int", 2)
        assert 5 == await client.json.nummultby("seed", ".int", 2.5)
        assert 2.5 == await client.json.nummultby("seed", ".int", 0.5)
        assert [5.0, 4] == await client.json.nummultby("seed", "$..int", 2)

    async def test_arrindex(self, client: Redis):
        await client.json.set("arr", ".", [0, 1, 2, 3, 4])
        assert 1 == await client.json.arrindex("arr", ".", 1)
        assert -1 == await client.json.arrindex("arr", ".", 1, 2)

    async def test_resp(self, client: Redis):
        obj = {"foo": "bar", "baz": 1, "qaz": True}
        await client.json.set("obj", ".", obj)
        assert "bar" == await client.json.resp("obj", "foo")
        assert 1 == await client.json.resp("obj", "baz")
        assert await client.json.resp("obj", "qaz")
        assert isinstance(await client.json.resp("obj"), list)

    async def test_delete(self, client: Redis):
        doc1 = {"a": 1, "nested": {"a": 2, "b": 3}}
        assert await client.json.set("doc1", "$", doc1)
        assert await client.json.delete("doc1", "$..a") == 2
        r = await client.json.get("doc1", "$")
        assert r == [{"nested": {"b": 3}}]

        doc2 = {
            "a": {"a": 2, "b": 3},
            "b": ["a", "b"],
            "nested": {"b": [True, "a", "b"]},
        }
        assert await client.json.set("doc2", "$", doc2)
        assert await client.json.delete("doc2", "$..a") == 1
        res = await client.json.get("doc2", "$")
        assert res == [{"nested": {"b": [True, "a", "b"]}, "b": ["a", "b"]}]

        doc3 = [
            {
                "ciao": ["non ancora"],
                "nested": [
                    {"ciao": [1, "a"]},
                    {"ciao": [2, "a"]},
                    {"ciaoc": [3, "non", "ciao"]},
                    {"ciao": [4, "a"]},
                    {"e": [5, "non", "ciao"]},
                ],
            }
        ]
        assert await client.json.set("doc3", "$", doc3)
        assert await client.json.delete("doc3", '$.[0]["nested"]..ciao') == 3

        doc3val = [
            [
                {
                    "ciao": ["non ancora"],
                    "nested": [
                        {},
                        {},
                        {"ciaoc": [3, "non", "ciao"]},
                        {},
                        {"e": [5, "non", "ciao"]},
                    ],
                }
            ]
        ]
        res = await client.json.get("doc3", "$")
        assert res == doc3val

        # Test async default path
        assert await client.json.delete("doc3") == 1
        assert await client.json.get("doc3", "$") is None

        await client.json.delete("not_a_document", "..a")

    @pytest.mark.nocluster
    async def test_mget(self, client: Redis):
        # Test mget with multi paths
        await client.json.set(
            "doc1",
            "$",
            {"a": 1, "b": 2, "nested": {"a": 3}, "c": None, "nested2": {"a": None}},
        )
        await client.json.set(
            "doc2",
            "$",
            {"a": 4, "b": 5, "nested": {"a": 6}, "c": None, "nested2": {"a": [None]}},
        )
        # Compare also to single JSON.GET
        assert await client.json.get("doc1", "$..a") == [1, 3, None]
        assert await client.json.get("doc2", "$..a") == [4, 6, [None]]

        # Test mget with single path
        await client.json.mget(["doc1"], "$..a") == [1, 3, None]
        # Test mget with multi path
        res = await client.json.mget(["doc1", "doc2"], "$..a")
        assert res == [[1, 3, None], [4, 6, [None]]]

        # Test missing key
        res = await client.json.mget(["doc1", "missing_doc"], "$..a")
        assert res == [[1, 3, None], None]
        res = await client.json.mget(["missing_doc1", "missing_doc2"], "$..a")
        assert res == [None, None]

    async def test_incrby(self, client: Redis):
        await client.json.set(
            "doc1", "$", {"a": "b", "b": [{"a": 2}, {"a": 5.0}, {"a": "c"}]}
        )
        assert await client.json.numincrby("doc1", "$..a", 2) == [None, 4, 7.0, None]

        res = await client.json.numincrby("doc1", "$..a", 2.5)
        assert res == [None, 6.5, 9.5, None]
        assert await client.json.numincrby("doc1", "$.b[1].a", 2) == [11.5]

        assert await client.json.numincrby("doc1", "$.b[2].a", 2) == [None]
        assert await client.json.numincrby("doc1", "$.b[1].a", 3.5) == [15.0]

    async def test_strappend(self, client: Redis):
        await client.json.set("jsonkey", ".", "foo")
        assert 6 == await client.json.strappend("jsonkey", "bar")
        assert "foobar" == await client.json.get("jsonkey", ".")

        await client.json.set(
            "doc1", "$", {"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}
        )
        # Test multi
        await client.json.strappend("doc1", "bar", "$..a") == [6, 8, None]

        await client.json.get("doc1", "$") == [
            {"a": "foobar", "nested1": {"a": "hellobar"}, "nested2": {"a": 31}}
        ]
        # Test single
        await client.json.strappend("doc1", "baz", "$.nested1.a") == [11]

        await client.json.get("doc1", "$") == [
            {"a": "foobar", "nested1": {"a": "hellobarbaz"}, "nested2": {"a": 31}}
        ]

        # Test missing key
        with pytest.raises(ResponseError):
            await client.json.strappend("non_existing_doc", "$..a", "err")

        # Test multi
        await client.json.strappend("doc1", "bar", ".*.a") == 8
        await client.json.get("doc1", "$") == [
            {"a": "foo", "nested1": {"a": "hellobar"}, "nested2": {"a": 31}}
        ]

        # Test missing path
        with pytest.raises(ResponseError):
            await client.json.strappend("doc1", "piu")

    async def test_strlen(self, client: Redis):
        await client.json.set("str", ".", "foo")
        assert 3 == await client.json.strlen("str", ".")
        await client.json.strappend("str", "bar", ".")
        assert 6 == await client.json.strlen("str", ".")
        assert 6 == await client.json.strlen("str")
        # Test multi
        await client.json.set(
            "doc1", "$", {"a": "foo", "nested1": {"a": "hello"}, "nested2": {"a": 31}}
        )
        assert await client.json.strlen("doc1", "$..a") == [3, 5, None]

        res2 = await client.json.strappend("doc1", "bar", "$..a")
        res1 = await client.json.strlen("doc1", "$..a")
        assert res1 == res2

        # Test single
        await client.json.strlen("doc1", "$.nested1.a") == [8]
        await client.json.strlen("doc1", "$.nested2.a") == [None]

        # Test missing key
        with pytest.raises(ResponseError):
            await client.json.strlen("non_existing_doc", "$..a")

    async def test_arrappend(self, client: Redis):
        await client.json.set("arr", ".", [1])
        assert 2 == await client.json.arrappend("arr", [2], ".")
        assert 4 == await client.json.arrappend("arr", [3, 4], ".")
        assert 7 == await client.json.arrappend("arr", [5, 6, 7], ".")
        await client.json.set(
            "doc1",
            "$",
            {
                "a": ["foo"],
                "nested1": {"a": ["hello", None, "world"]},
                "nested2": {"a": 31},
            },
        )
        # Test multi
        await client.json.arrappend("doc1", ["bar", "racuda"], "$..a") == [3, 5, None]
        assert await client.json.get("doc1", "$") == [
            {
                "a": ["foo", "bar", "racuda"],
                "nested1": {"a": ["hello", None, "world", "bar", "racuda"]},
                "nested2": {"a": 31},
            }
        ]

        # Test single
        assert await client.json.arrappend("doc1", ["baz"], "$.nested1.a") == [6]
        assert await client.json.get("doc1", "$") == [
            {
                "a": ["foo", "bar", "racuda"],
                "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]},
                "nested2": {"a": 31},
            }
        ]

        # Test missing key
        with pytest.raises(ResponseError):
            await client.json.arrappend("non_existing_doc", [], "$..a")

        # Test legacy
        await client.json.set(
            "doc1",
            "$",
            {
                "a": ["foo"],
                "nested1": {"a": ["hello", None, "world"]},
                "nested2": {"a": 31},
            },
        )
        # Test multi (all paths are updated, but return result of last path)
        assert await client.json.arrappend("doc1", ["bar", "racuda"], "..a") == 5

        assert await client.json.get("doc1", "$") == [
            {
                "a": ["foo", "bar", "racuda"],
                "nested1": {"a": ["hello", None, "world", "bar", "racuda"]},
                "nested2": {"a": 31},
            }
        ]
        # Test single
        assert await client.json.arrappend("doc1", ["baz"], ".nested1.a") == 6
        assert await client.json.get("doc1", "$") == [
            {
                "a": ["foo", "bar", "racuda"],
                "nested1": {"a": ["hello", None, "world", "bar", "racuda", "baz"]},
                "nested2": {"a": 31},
            }
        ]

        # Test missing key
        with pytest.raises(ResponseError):
            await client.json.arrappend("non_existing_doc", [], "$..a")

    async def test_arrinsert(self, client: Redis):
        await client.json.set("arr", ".", [0, 4])
        assert 5 - -await client.json.arrinsert(
            "arr",
            ".",
            1,
            [
                1,
                2,
                3,
            ],
        )
        assert [0, 1, 2, 3, 4] == await client.json.get("arr")

        # test prepends
        await client.json.set("val2", ".", [5, 6, 7, 8, 9])
        await client.json.arrinsert("val2", ".", 0, [["some", "thing"]])
        assert await client.json.get("val2") == [["some", "thing"], 5, 6, 7, 8, 9]
        await client.json.set(
            "doc1",
            "$",
            {
                "a": ["foo"],
                "nested1": {"a": ["hello", None, "world"]},
                "nested2": {"a": 31},
            },
        )
        # Test multi
        res = await client.json.arrinsert("doc1", "$..a", 1, ["bar", "racuda"])
        assert res == [3, 5, None]

        assert await client.json.get("doc1", "$") == [
            {
                "a": ["foo", "bar", "racuda"],
                "nested1": {"a": ["hello", "bar", "racuda", None, "world"]},
                "nested2": {"a": 31},
            }
        ]
        # Test single
        assert await client.json.arrinsert("doc1", "$.nested1.a", -2, ["baz"]) == [6]
        assert await client.json.get("doc1", "$") == [
            {
                "a": ["foo", "bar", "racuda"],
                "nested1": {"a": ["hello", "bar", "racuda", "baz", None, "world"]},
                "nested2": {"a": 31},
            }
        ]

        # Test missing key
        with pytest.raises(ResponseError):
            await client.json.arrappend("non_existing_doc", [], "$..a")

    async def test_arrlen(self, client: Redis):
        await client.json.set("arr", ".", [0, 1, 2, 3, 4])
        assert 5 == await client.json.arrlen("arr", ".")
        assert 5 == await client.json.arrlen("arr")
        assert await client.json.arrlen("fakekey") is None
        await client.json.set(
            "doc1",
            "$",
            {
                "a": ["foo"],
                "nested1": {"a": ["hello", None, "world"]},
                "nested2": {"a": 31},
            },
        )

        # Test multi
        assert await client.json.arrlen("doc1", "$..a") == [1, 3, None]
        res = await client.json.arrappend("doc1", ["non", "abba", "stanza"], "$..a")
        assert res == [4, 6, None]

        await client.json.clear("doc1", "$.a")
        assert await client.json.arrlen("doc1", "$..a") == [0, 6, None]
        # Test single
        assert await client.json.arrlen("doc1", "$.nested1.a") == [6]

        # Test missing key
        with pytest.raises(ResponseError):
            await client.json.arrappend("non_existing_doc", ["fail"], "$..a")

        await client.json.set(
            "doc1",
            "$",
            {
                "a": ["foo"],
                "nested1": {"a": ["hello", None, "world"]},
                "nested2": {"a": 31},
            },
        )
        # Test multi (return result of last path)
        assert await client.json.arrlen("doc1", "$..a") == [1, 3, None]
        assert (
            await client.json.arrappend("doc1", ["non", "abba", "stanza"], "..a") == 6
        )

        # Test single
        assert await client.json.arrlen("doc1", ".nested1.a") == 6

        # Test missing key
        assert await client.json.arrlen("non_existing_doc", "..a") is None

    async def test_arrpop(self, client: Redis):
        await client.json.set("arr", ".", [0, 1, 2, 3, 4])
        assert 4 == await client.json.arrpop("arr", ".", 4)
        assert 3 == await client.json.arrpop("arr", ".", -1)
        assert 2 == await client.json.arrpop("arr", ".")
        assert 0 == await client.json.arrpop("arr", ".", 0)
        assert [1] == await client.json.get("arr")

        # test out of bounds
        await client.json.set("arr", ".", [0, 1, 2, 3, 4])
        assert 4 == await client.json.arrpop("arr", ".", 99)

        # none test
        await client.json.set("arr", ".", [])
        assert await client.json.arrpop("arr") is None

        await client.json.set(
            "doc1",
            "$",
            {
                "a": ["foo"],
                "nested1": {"a": ["hello", None, "world"]},
                "nested2": {"a": 31},
            },
        )

        # # # Test multi
        assert await client.json.arrpop("doc1", "$..a", 1) == ["foo", None, None]

        assert await client.json.get("doc1", "$") == [
            {"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}
        ]

        # Test missing key
        with pytest.raises(ResponseError):
            await client.json.arrpop("non_existing_doc", "..a")

        # # Test legacy
        await client.json.set(
            "doc1",
            "$",
            {
                "a": ["foo"],
                "nested1": {"a": ["hello", None, "world"]},
                "nested2": {"a": 31},
            },
        )
        # Test multi (all paths are updated, but return result of last path)
        await client.json.arrpop("doc1", "..a", 1) is None
        assert await client.json.get("doc1", "$") == [
            {"a": [], "nested1": {"a": ["hello", "world"]}, "nested2": {"a": 31}}
        ]

        # # Test missing key
        with pytest.raises(ResponseError):
            await client.json.arrpop("non_existing_doc", "..a")

    async def test_arrtrim(self, client: Redis):
        await client.json.set("arr", ".", [0, 1, 2, 3, 4])
        assert 3 == await client.json.arrtrim("arr", ".", 1, 3)
        assert [1, 2, 3] == await client.json.get("arr")

        # <0 test, should be 0 equivalent
        await client.json.set("arr", ".", [0, 1, 2, 3, 4])
        assert 0 == await client.json.arrtrim("arr", ".", -1, 3)

        # testing stop > end
        await client.json.set("arr", ".", [0, 1, 2, 3, 4])
        assert 2 == await client.json.arrtrim("arr", ".", 3, 99)

        # start > array size and stop
        await client.json.set("arr", ".", [0, 1, 2, 3, 4])
        assert 0 == await client.json.arrtrim("arr", ".", 9, 1)

        # all larger
        await client.json.set("arr", ".", [0, 1, 2, 3, 4])
        assert 0 == await client.json.arrtrim("arr", ".", 9, 11)

        await client.json.set(
            "doc1",
            "$",
            {
                "a": ["foo"],
                "nested1": {"a": ["hello", None, "world"]},
                "nested2": {"a": 31},
            },
        )
        # Test multi
        assert await client.json.arrtrim("doc1", "$..a", 1, -1) == [0, 2, None]
        assert await client.json.get("doc1", "$") == [
            {"a": [], "nested1": {"a": [None, "world"]}, "nested2": {"a": 31}}
        ]

        assert await client.json.arrtrim("doc1", "$..a", 1, 1) == [0, 1, None]
        assert await client.json.get("doc1", "$") == [
            {"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}
        ]
        # Test single
        assert await client.json.arrtrim("doc1", "$.nested1.a", 1, 0) == [0]
        assert await client.json.get("doc1", "$") == [
            {"a": [], "nested1": {"a": []}, "nested2": {"a": 31}}
        ]

        # Test missing key
        with pytest.raises(ResponseError):
            await client.json.arrtrim("non_existing_doc", "..a", 0, 1)

        # Test legacy
        await client.json.set(
            "doc1",
            "$",
            {
                "a": ["foo"],
                "nested1": {"a": ["hello", None, "world"]},
                "nested2": {"a": 31},
            },
        )

        # Test multi (all paths are updated, but return result of last path)
        assert await client.json.arrtrim("doc1", "..a", 1, -1) == 2

        # Test single
        assert await client.json.arrtrim("doc1", ".nested1.a", 1, 1) == 1
        assert await client.json.get("doc1", "$") == [
            {"a": [], "nested1": {"a": ["world"]}, "nested2": {"a": 31}}
        ]

        # Test missing key
        with pytest.raises(ResponseError):
            await client.json.arrtrim("non_existing_doc", "..a", 1, 1)

    async def test_objkeys(self, client: Redis):
        obj = {"foo": "bar", "baz": "qaz"}
        await client.json.set("obj", ".", obj)
        keys = await client.json.objkeys("obj", ".")
        keys.sort()
        exp = list(obj.keys())
        exp.sort()
        assert exp == keys

        await client.json.set("obj", ".", obj)
        keys = await client.json.objkeys("obj")
        assert keys == list(obj.keys())

        assert await client.json.objkeys("fakekey") is None

        await client.json.set(
            "doc1",
            "$",
            {
                "nested1": {"a": {"foo": 10, "bar": 20}},
                "a": ["foo"],
                "nested2": {"a": {"baz": 50}},
            },
        )

        # Test single
        assert await client.json.objkeys("doc1", "$.nested1.a") == [["foo", "bar"]]

        # Test legacy
        assert await client.json.objkeys("doc1", ".*.a") == ["foo", "bar"]
        # Test single
        assert await client.json.objkeys("doc1", ".nested2.a") == ["baz"]

        # Test missing key
        assert await client.json.objkeys("non_existing_doc", "..a") is None

        # Test non existing doc
        with pytest.raises(ResponseError):
            assert await client.json.objkeys("non_existing_doc", "$..a") == []

        assert await client.json.objkeys("doc1", "$..nowhere") == []

    @pytest.mark.min_module_version("ReJSON", "2.4.0")
    async def test_objlen(self, client: Redis):
        obj = {"foo": "bar", "baz": "qaz"}
        await client.json.set("obj", ".", obj)
        assert len(obj) == await client.json.objlen("obj", ".")

        await client.json.set("obj", ".", obj)
        assert len(obj) == await client.json.objlen("obj")

        #
        await client.json.set(
            "doc1",
            "$",
            {
                "nested1": {"a": {"foo": 10, "bar": 20}},
                "a": ["foo"],
                "nested2": {"a": {"baz": 50}},
            },
        )
        # Test multi
        assert await client.json.objlen("doc1", "$..a") == [None, 2, 1]
        # Test single
        assert await client.json.objlen("doc1", "$.nested1.a") == [2]

        # Test missing key, and path
        with pytest.raises(ResponseError):
            await client.json.objlen("non_existing_doc", "$..a")

        assert await client.json.objlen("doc1", "$.nowhere") == []

        # Test legacy
        assert await client.json.objlen("doc1", ".*.a") == 2

        # Test single
        assert await client.json.objlen("doc1", ".nested2.a") == 1

        # Test missing key
        assert await client.json.objlen("non_existing_doc", "..a") is None

        # Test missing path
        # with pytest.raises(ResponseError):
        await client.json.objlen("doc1", ".nowhere")

    def load_types_data(self, nested_key_name):
        td = {
            "object": {},
            "array": [],
            "string": "str",
            "integer": 42,
            "number": 1.2,
            "boolean": False,
            "null": None,
        }
        jdata = {}
        types = []
        for i, (k, v) in zip(range(1, len(td) + 1), iter(td.items())):
            jdata["nested" + str(i)] = {nested_key_name: v}
            types.append(k)

        return jdata, types

    async def test_clear(self, client: Redis):
        await client.json.set("arr", ".", [0, 1, 2, 3, 4])
        assert 1 == await client.json.clear("arr", ".")
        assert [] == await client.json.get("arr")
        await client.json.set(
            "doc1",
            "$",
            {
                "nested1": {"a": {"foo": 10, "bar": 20}},
                "a": ["foo"],
                "nested2": {"a": "claro"},
                "nested3": {"a": {"baz": 50}},
            },
        )

        # Test multi
        assert await client.json.clear("doc1", "$..a") == 3

        assert await client.json.get("doc1", "$") == [
            {
                "nested1": {"a": {}},
                "a": [],
                "nested2": {"a": "claro"},
                "nested3": {"a": {}},
            }
        ]

        # Test single
        await client.json.set(
            "doc1",
            "$",
            {
                "nested1": {"a": {"foo": 10, "bar": 20}},
                "a": ["foo"],
                "nested2": {"a": "claro"},
                "nested3": {"a": {"baz": 50}},
            },
        )
        assert await client.json.clear("doc1", "$.nested1.a") == 1
        assert await client.json.get("doc1", "$") == [
            {
                "nested1": {"a": {}},
                "a": ["foo"],
                "nested2": {"a": "claro"},
                "nested3": {"a": {"baz": 50}},
            }
        ]

        # Test missing path (async defaults to root)
        assert await client.json.clear("doc1") == 1
        assert await client.json.get("doc1", "$") == [{}]

        # Test missing key
        with pytest.raises(ResponseError):
            await client.json.clear("non_existing_doc", "$..a")

    async def test_toggle(self, client: Redis):
        await client.json.set("bool", ".", False)
        assert await client.json.toggle("bool", ".") is True
        assert await client.json.toggle("bool", ".") is False
        # check non-boolean value
        await client.json.set("num", ".", 1)
        with pytest.raises(ResponseError):
            await client.json.toggle("num", ".")
        await client.json.set(
            "doc1",
            "$",
            {
                "a": ["foo"],
                "nested1": {"a": False},
                "nested2": {"a": 31},
                "nested3": {"a": True},
            },
        )
        # Test multi
        assert await client.json.toggle("doc1", "$..a") == [None, 1, None, 0]
        assert await client.json.get("doc1", "$") == [
            {
                "a": ["foo"],
                "nested1": {"a": True},
                "nested2": {"a": 31},
                "nested3": {"a": False},
            }
        ]

        # Test missing key
        with pytest.raises(ResponseError):
            await client.json.toggle("non_existing_doc", "$..a")

    @pytest.mark.min_module_version("ReJSON", "2.4.0")
    async def test_debug_memory(self, client: Redis, seed):
        assert 8 == await client.json.debug_memory("seed", ".int")
        assert 14 == await client.json.debug_memory("seed", ".string")
        assert 872 == await client.json.debug_memory("seed", ".object")
        assert [8, 8] == await client.json.debug_memory("seed", "$..int")
        assert 1772 == await client.json.debug_memory("seed")

    @pytest.mark.parametrize("transaction", [True, False])
    async def test_pipeline(self, client: Redis, transaction: bool):
        p = await client.pipeline(transaction=transaction)
        await p.json.set(
            "key",
            "$",
            {"a": 1, "b": [2], "c": {"d": "3"}, "e": {"f": [{"g": 4, "h": True}]}},
        )
        await p.json.numincrby("key", "$.a", 1)
        await p.json.arrappend("key", [1], "..*")
        await p.json.strappend("key", "bar", "..*")
        await p.json.toggle("key", "..*")
        await p.json.toggle("key", "..*")
        assert (
            True,
            [2],
            2,
            4,
            False,
            True,
        ) == await p.execute()
        assert {
            "a": 2,
            "b": [2, 1],
            "c": {"d": "3bar"},
            "e": {"f": [{"g": 4, "h": True}, 1]},
        } == await client.json.get("key")
