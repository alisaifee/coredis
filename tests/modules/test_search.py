from __future__ import annotations

import json
import re
from datetime import timedelta

import numpy
import pytest

from coredis import PureToken, Redis
from coredis.exceptions import ResponseError
from coredis.modules.response.types import (
    SearchAggregationResult,
    SearchDocument,
    SearchResult,
)
from coredis.modules.search import Apply, Field, Filter, Group, Reduce
from coredis.retry import ConstantRetryPolicy, retryable
from tests.conftest import targets


@pytest.fixture(scope="module")
def query_vectors():
    data = json.loads(open("tests/modules/data/vss_queries.json").read())
    return {k["text"]: numpy.asarray(k.get("embedding", [])) for k in data}


@pytest.fixture
async def city_index(client: Redis):
    data = json.loads(open("tests/modules/data/city_index.json").read())
    with client.ignore_replies():
        await client.search.create(
            "{city}idx",
            [
                Field("name", PureToken.TEXT),
                Field("country", PureToken.TEXT),
                Field("tags", PureToken.TAG, separator=","),
                Field("population", PureToken.NUMERIC),
                Field("location", PureToken.GEO),
                Field("summary_text", PureToken.TEXT),
                Field(
                    "summary_vector",
                    PureToken.VECTOR,
                    algorithm="FLAT",
                    attributes={
                        "TYPE": "FLOAT32",
                        "DIM": 768,
                        "DISTANCE_METRIC": "L2",
                        "INITIAL_CAP": 10,
                        "BLOCK_SIZE": 10,
                    },
                ),
            ],
            on=PureToken.HASH,
            payload_field="last_updated",
            prefixes=["{city}idx:"],
        )
        await client.search.create(
            "{jcity}idx",
            [
                Field("$.name", PureToken.TEXT, alias="name"),
                Field("$.country", PureToken.TEXT, alias="country"),
                Field("$.tags", PureToken.TAG, alias="tags"),
                Field("$.population", PureToken.NUMERIC, alias="population"),
                Field("$.location", PureToken.GEO, alias="location"),
                Field("$.summary_text", PureToken.TEXT, alias="summary_text"),
                Field(
                    "$.summary_vector",
                    PureToken.VECTOR,
                    algorithm="FLAT",
                    attributes={
                        "TYPE": "FLOAT32",
                        "DIM": 768,
                        "DISTANCE_METRIC": "COSINE",
                        "INITIAL_CAP": 10,
                        "BLOCK_SIZE": 10,
                    },
                    alias="summary_vector",
                ),
            ],
            on=PureToken.JSON,
            payload_field="$.last_updated",
            prefixes=["{jcity}idx:"],
        )
        for name, city in data.items():
            await client.hset(
                f"{{city}}idx:{name}",
                {
                    "name": city["name"],
                    "country": city["country"],
                    "tags": ",".join(city["iso_tags"]),
                    "population": city["population"],
                    "location": f'{city["lng"]},{city["lat"]}',
                    "summary_text": city["summary"],
                    "summary_vector": numpy.asarray(city["summary_vector"])
                    .astype(numpy.float32)
                    .tobytes(),
                    "last_updated": "2012-12-12",
                },
            )
            await client.json.set(
                f"{{jcity}}idx:{name}",
                ".",
                {
                    "name": city["name"],
                    "country": city["country"],
                    "tags": city["iso_tags"],
                    "population": int(city["population"]),
                    "location": f'{city["lng"]},{city["lat"]}',
                    "summary_text": city["summary"],
                    "summary_vector": city["summary_vector"],
                    "last_updated": "2012-12-12",
                },
            )
    return data


@retryable(ConstantRetryPolicy((ValueError,), 5, 0.1))
async def wait_for_index(index_name, client: Redis):
    info = await client.search.info(index_name)
    if int(info["indexing"]):
        raise ValueError("Index not available")


@pytest.mark.min_module_version("search", "2.6.1")
@targets(
    "redis_stack", "redis_stack_resp2", "redis_stack_cached", "redis_stack_cluster"
)
class TestSchema:
    @pytest.mark.parametrize("on", [PureToken.HASH, PureToken.JSON])
    @pytest.mark.parametrize(
        "args",
        [
            {"alias": "alias"},
        ],
    )
    @pytest.mark.parametrize(
        "type_args",
        [
            {"type": PureToken.TEXT},
            {"type": PureToken.TEXT, "sortable": True},
            {"type": PureToken.TEXT, "sortable": True, "unf": True},
            {"type": PureToken.TEXT, "phonetic": "dm:fr"},
            {"type": PureToken.TEXT, "nostem": True},
            {"type": PureToken.TEXT, "noindex": True},
            {"type": PureToken.TEXT, "weight": 100},
            {"type": PureToken.TEXT, "withsuffixtrie": True},
            {"type": PureToken.NUMERIC},
            {"type": PureToken.TAG},
            {"type": PureToken.TAG, "withsuffixtrie": True},
            {"type": PureToken.TAG, "casesensitive": True},
            {"type": PureToken.GEO},
            {
                "type": PureToken.VECTOR,
                "algorithm": "FLAT",
                "attributes": {
                    "TYPE": "FLOAT32",
                    "DIM": 512,
                    "DISTANCE_METRIC": "COSINE",
                },
            },
            {
                "type": PureToken.VECTOR,
                "algorithm": "HNSW",
                "attributes": {
                    "TYPE": "FLOAT32",
                    "DIM": 512,
                    "DISTANCE_METRIC": "COSINE",
                },
            },
        ],
        ids=lambda val: str([(str(k[0]), str(k[1])) for k in val.items()]),
    )
    async def test_field_type(self, client: Redis, on, type_args, args):
        name = "field" if on == PureToken.HASH else "$.field"
        assert await client.search.create(
            "idx",
            [
                Field(name, **args, **type_args),
            ],
            on=on,
        )
        assert {"idx"} == await client.search.list()
        info = await client.search.info("idx")
        assert info["index_name"] == "idx"
        assert info["index_definition"]["key_type"] == str(on)

        assert info["attributes"][0]["identifier"] == name

        assert await client.search.alter(
            "idx",
            Field(f"{name}_new", **type_args),
            skipinitialscan=True,
        )
        info = await client.search.info("idx")
        assert f"{name}_new" == info["attributes"][1]["identifier"]

    @pytest.mark.parametrize("on", [PureToken.HASH, PureToken.JSON])
    @pytest.mark.parametrize(
        "schema_args",
        [
            {"filter_expression": "1==1"},
            {"prefixes": ["a:", "b:"]},
            {"language": "French"},
            {"language": "French", "language_field": "field"},
            {"score": 0.1},
            {"score": 0.1, "score_field": "field"},
            {"payload_field": "payload"},
            {"maxtextfields": True},
            {"nooffsets": True},
            {"temporary": 1},
            {"temporary": timedelta(microseconds=2)},
            {"nohl": True},
            {"nofields": True},
            {"nofreqs": True},
            {"stopwords": ["fu", "bar"]},
            {"skipinitialscan": True},
        ],
        ids=lambda val: str(val),
    )
    async def test_index_options(self, client: Redis, on, schema_args):
        fields = [
            Field("field", PureToken.TEXT),
        ]
        assert await client.search.create("idx", fields, on=on, **schema_args)

    @pytest.mark.parametrize("on", [PureToken.HASH, PureToken.JSON])
    @pytest.mark.parametrize(
        "schema_args, exception, matcher",
        [
            ({"filter_expression": "\\0/"}, ResponseError, "Syntax error at offset"),
            ({"language": "Klingon"}, ResponseError, "Invalid language"),
            ({"score": 2}, ResponseError, "Invalid score"),
        ],
        ids=lambda val: str(val),
    )
    async def test_invalid_index_options(
        self, client: Redis, on, schema_args, exception, matcher
    ):
        fields = [
            Field("field", PureToken.TEXT),
        ]
        with pytest.raises(exception, match=matcher):
            await client.search.create("idx", fields, on=on, **schema_args)

    @pytest.mark.parametrize("on", [PureToken.HASH, PureToken.JSON])
    async def test_drop_index(self, client: Redis, on):
        await client.search.create(
            "idx", [Field("field", PureToken.TEXT)], on=on, prefixes=["doc:"]
        )

        assert await client.search.dropindex("idx")
        with pytest.raises(
            ResponseError, match=re.compile("unknown index name", re.IGNORECASE)
        ):
            await client.search.info("idx")

    async def test_drop_index_cascade(self, client: Redis):
        await client.search.create(
            "idx{a}",
            [Field("field", PureToken.TEXT)],
            on=PureToken.HASH,
            prefixes=["doc{a}:"],
        )
        await client.search.create(
            "jidx{a}",
            [Field("field", PureToken.TEXT)],
            on=PureToken.JSON,
            prefixes=["jdoc{a}:"],
        )
        await client.hset("doc{a}:1", {"field": "value"})
        await client.json.set("jdoc{a}:1", ".", {"field": "value"})
        assert {"doc{a}:1", "jdoc{a}:1"} == await client.keys("*")
        assert await client.search.dropindex("idx{a}", delete_docs=True)
        assert await client.search.dropindex("jidx{a}", delete_docs=True)
        assert not await client.keys()
        with pytest.raises(
            ResponseError, match=re.compile("unknown index name", re.IGNORECASE)
        ):
            await client.search.info("idx{a}")
        with pytest.raises(
            ResponseError, match=re.compile("unknown index name", re.IGNORECASE)
        ):
            await client.search.info("jidx{a}")

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_alias(self, client: Redis, city_index, index_name):
        assert await client.search.create(
            f"{index_name}:empty",
            [Field("field", PureToken.TEXT)],
            on=PureToken.HASH,
            prefixes=["empty:"],
        )
        assert await client.search.aliasadd(f"{index_name}:alias", index_name)
        original_results = await client.search.search(
            f"{index_name}", "*", nocontent=True
        )
        results = await client.search.search(f"{index_name}:alias", "*", nocontent=True)
        assert original_results.total == results.total

        assert await client.search.aliasupdate(
            f"{index_name}:alias", f"{index_name}:empty"
        )
        results = await client.search.search(f"{index_name}:alias", "*", nocontent=True)
        assert 0 == results.total

        assert await client.search.aliasdel(f"{index_name}:alias")
        with pytest.raises(ResponseError):
            await client.search.search(f"{index_name}:alias", "*", nocontent=True)

    async def test_search_config(self, client: Redis):
        config_all = await client.search.config_get("*")
        assert config_all["DEFAULT_DIALECT"] == 1
        assert await client.search.config_set("DEFAULT_DIALECT", 2)
        config_partial = await client.search.config_get("DEFAULT_DIALECT")
        assert config_partial["DEFAULT_DIALECT"] == 2
        assert await client.search.config_set("DEFAULT_DIALECT", 1)
        with pytest.raises(ResponseError, match="Invalid option"):
            assert await client.search.config_set("idk", 1)
        with pytest.raises(
            ResponseError, match="Default dialect version cannot be higher than"
        ):
            assert await client.search.config_set("DEFAULT_DIALECT", 42)


@pytest.mark.min_module_version("search", "2.6.1")
@targets(
    "redis_stack", "redis_stack_resp2", "redis_stack_cached", "redis_stack_cluster"
)
class TestSearch:
    @pytest.mark.parametrize("dialect", [1, 2, 3])
    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_spellcheck(self, client: Redis, city_index, index_name, dialect):
        assert not (await client.search.spellcheck(index_name, "menil"))["menil"]
        assert (
            "manila"
            in (
                await client.search.spellcheck(
                    index_name, "menil", distance=2, dialect=dialect
                )
            )["menil"]
        )

        await client.search.dictadd("{city}custom", ["menila"])
        assert (
            "menila"
            in (
                await client.search.spellcheck(
                    index_name,
                    "menil",
                    distance=2,
                    include="{city}custom",
                    dialect=dialect,
                )
            )["menil"]
        )
        assert (
            "menila"
            not in (
                await client.search.spellcheck(
                    index_name,
                    "menil",
                    distance=2,
                    exclude="{city}custom",
                    dialect=dialect,
                )
            )["menil"]
        )
        assert (
            "menila"
            in (
                await client.search.spellcheck(
                    index_name,
                    "menil",
                    distance=2,
                    exclude="{city}custom",
                    include="{city}custom",
                    dialect=dialect,
                )
            )["menil"]
        )

        await client.search.dictdel("{city}custom", ["menila"])
        assert set() == await client.search.dictdump("{city}custom")
        assert (
            "menila"
            not in (
                await client.search.spellcheck(
                    index_name,
                    "menil",
                    distance=2,
                    include="{city}custom",
                    dialect=dialect,
                )
            )["menil"]
        )

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_numeric_filter(self, client: Redis, city_index, index_name):
        results = await client.search.search(
            index_name, "@population:[35000000 inf]", returns={"name": None}
        )
        assert results.total == 1
        assert results.documents[0].properties["name"] == "tokyo"

        results = await client.search.search(
            index_name,
            "*",
            numeric_filters={"population": (35000000, "+inf")},
            returns={"name": None},
        )
        assert results.total == 1
        assert results.documents[0].properties["name"] == "tokyo"

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_geo_search(self, client: Redis, city_index, index_name):
        results = await client.search.search(
            index_name, "@location:[67.0011 24.8607 1 km]", returns={"name": None}
        )
        assert results.total == 1
        assert results.documents[0].properties["name"] == "karachi"

        results = await client.search.search(
            index_name,
            "*",
            geo_filters={"location": ((67.0011, 24.8607), 1, PureToken.KM)},
            returns={"name": None},
        )
        assert results.total == 1
        assert results.documents[0].properties["name"] == "karachi"

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_text_search(self, client: Redis, city_index, index_name):
        results = await client.search.search(
            index_name,
            "@name:karachi",
            returns={"name": None},
            withscores=True,
            withpayloads=True,
            withsortkeys=True,
            explainscore=True,
            sortby="name",
            sort_order=PureToken.DESC,
            offset=0,
            limit=1,
            language="English",
            timeout=timedelta(seconds=1),
            payload="last_updated",
        )
        assert results.total == 1
        assert results.documents[0].properties["name"] == "karachi"
        assert results.documents[0].score is not None
        assert isinstance(results.documents[0].score_explanation, list)

        results = await client.search.search(
            index_name, "@name:karachi", returns={"name": "nom"}
        )
        assert results.total == 1
        assert results.documents[0].properties["nom"] == "karachi"
        assert results.documents[0].score is None
        assert results.documents[0].score_explanation is None

        results = await client.search.search(
            index_name, "@name:karachi", nocontent=True
        )

        assert (
            SearchDocument(f"{index_name}:karachi", None, None, None, None, {}),
        ) == results.documents

        results = await client.search.search(
            index_name,
            "@summary_text:competing",
            verbatim=True,
            nocontent=True,
        )
        assert 0 == results.total

        results = await client.search.search(
            index_name,
            "@summary_text:competing",
            verbatim=False,
            nocontent=True,
        )
        assert 1 == results.total

        results = await client.search.search(
            index_name, "the Olympics", nostopwords=True
        )
        assert results.total == 0

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_text_search_with_highlighting(
        self, client: Redis, city_index, index_name
    ):
        results = await client.search.search(
            index_name,
            "Olympics",
            summarize_fields=["summary_text"],
            summarize_frags=2,
            summarize_length=10,
            summarize_separator="{{truncate}}",
            highlight_fields=["summary_text"],
            highlight_tags=("<blink>", "</blink>"),
            returns={"summary_text": None},
        )
        assert (
            "Summer <blink>Olympics</blink>"
            in results.documents[0].properties["summary_text"]
        )

        assert (
            "2016 Summer{{truncate}}" in results.documents[0].properties["summary_text"]
        )

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_text_search_with_slop(self, client: Redis, city_index, index_name):
        results = await client.search.search(
            index_name,
            "Summer Olympics Games",
            summarize_fields=["summary_text"],
            returns={"summary_text": None},
        )
        no_slop_results = await client.search.search(
            index_name,
            "Summer Olympics Games",
            slop=0,
            summarize_fields=["summary_text"],
            returns={"summary_text": None},
        )

        assert not all(
            "Summer Olympic Games" in doc.properties["summary_text"]
            for doc in results.documents
        )
        assert all(
            "Summer Olympic Games" in doc.properties["summary_text"]
            for doc in no_slop_results.documents
        )

        slop_results = await client.search.search(
            index_name,
            "Summer Olympics Games",
            slop=1,
            summarize_fields=["summary_text"],
            returns={"summary_text": None},
        )
        assert slop_results.total == 2
        inorder_slop_results = await client.search.search(
            index_name,
            "Summer Olympics Games",
            slop=1,
            inorder=True,
            summarize_fields=["summary_text"],
            returns={"summary_text": None},
        )
        assert inorder_slop_results.total == 1

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_text_search_restricted(self, client: Redis, city_index, index_name):
        key_prefix = index_name.replace("idx", "")
        keys = [
            k
            for k in await client.keys()
            if k.startswith(key_prefix) and "karachi" not in k
        ]
        results = await client.search.search(
            index_name,
            "karachi",
            in_keys=keys,
            returns={"name": None, "summary_text": None},
        )
        assert results.documents[0].properties["name"] == "lahore"

        results = await client.search.search(
            index_name,
            "tokyo",
            in_fields=["name"],
            returns={"name": None},
        )
        assert results.total == 1
        assert results.documents[0].properties["name"] == "tokyo"

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_tags(self, client: Redis, city_index, index_name, query_vectors):
        tags = await client.search.tagvals(index_name, "tags")
        assert not {"pk", "pak"} - tags

        tag_results = await client.search.search(
            index_name,
            "@tags:{pk | jpn}",
            returns={"name": None},
        )
        assert {"karachi", "lahore", "ōsaka", "nagoya", "tokyo"} == set(
            k.properties["name"] for k in tag_results.documents
        )

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_vector_similarity_search(
        self, client: Redis, city_index, index_name, query_vectors
    ):
        query = query_vectors["historical landmark"]
        results = await client.search.search(
            index_name,
            "*=>[KNN 1 @summary_vector $query_vec as query_score]",
            parameters={"query_vec": query.astype(numpy.float32).tobytes()},
            returns={"name": None},
            dialect=2,
        )
        assert results.documents[0].properties["name"] == "tehran"
        results = await client.search.search(
            index_name,
            '@summary_text:"olympics"=>[KNN 1 @summary_vector $query_vec as query_score]',
            parameters={"query_vec": query.astype(numpy.float32).tobytes()},
            returns={"name": None},
            dialect=2,
        )
        assert results.documents[0].properties["name"] == "moscow"

        results = await client.search.search(
            index_name,
            "@country:India=>[KNN 1 @summary_vector $query_vec as query_score]",
            parameters={"query_vec": query.astype(numpy.float32).tobytes()},
            geo_filters={"location": ((67.0011, 24.8607), 5000, PureToken.KM)},
            returns={"name": None},
            dialect=2,
        )
        assert results.documents[0].properties["name"] == "chennai"

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_synonyms(self, client: Redis, city_index, index_name):
        assert not (
            await client.search.search(index_name, "@name:kolachi", nocontent=True)
        ).total
        assert await client.search.synupdate(
            index_name, "karachi", ["karachi", "kolachi"], skipinitialscan=True
        )
        assert ["karachi"] == (await client.search.syndump(index_name))["kolachi"]
        await wait_for_index(index_name, client)
        assert (
            0
            == (
                await client.search.search(index_name, "@name:kolachi", nocontent=True)
            ).total
        )

        assert await client.search.synupdate(index_name, "karachi", ["karachi", "khi"])
        assert ["karachi"] == (await client.search.syndump(index_name))["khi"]
        await wait_for_index(index_name, client)
        assert (
            1
            == (
                await client.search.search(index_name, "@name:khi", nocontent=True)
            ).total
        )

    @pytest.mark.parametrize("dialect", [1, 2, 3])
    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_explain(self, client: Redis, city_index, index_name, dialect):
        assert "<WILDCARD>" in (
            await client.search.explain(index_name, "*", dialect=dialect)
        )

    @pytest.mark.parametrize("transaction", [True, False])
    async def test_pipeline(self, client: Redis, transaction: bool):
        p = await client.pipeline(transaction=transaction)
        await p.search.create(
            "{search}:idx",
            [
                Field("name", PureToken.TEXT),
            ],
            on=PureToken.HASH,
            prefixes=["{search}:"],
        )
        await p.hset("{search}:doc:1", {"name": "hello"})
        await p.hset("{search}:doc:2", {"name": "world"})
        await p.search.search(
            "{search}:idx",
            "@name:hello",
        )
        assert (
            True,
            1,
            1,
            SearchResult(
                total=1,
                documents=(
                    SearchDocument(
                        "{search}:doc:1", None, None, None, None, {"name": "hello"}
                    ),
                ),
            ),
        ) == await p.execute()


@pytest.mark.min_module_version("search", "2.6.1")
@targets(
    "redis_stack", "redis_stack_resp2", "redis_stack_cached", "redis_stack_cluster"
)
class TestAggregation:
    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_aggregation_no_transforms(
        self, client: Redis, city_index, index_name
    ):
        results = await client.search.aggregate(
            index_name,
            "*",
            load=["name"],
            verbatim=True,
            timeout=timedelta(seconds=1),
        )
        assert {k["name"] for k in results.results} == set(city_index.keys())

    async def test_aggregation_dialect_3_hash(self, client: Redis, city_index):
        results = await client.search.aggregate(
            "{city}idx",
            "@name:$q",
            load=["name"],
            verbatim=True,
            timeout=timedelta(seconds=1),
            dialect=3,
            parameters={"q": "tokyo"},
        )
        assert results.results[0]["name"] == "tokyo"

    async def test_aggregation_dialect_3_json(self, client: Redis, city_index):
        results = await client.search.aggregate(
            "{jcity}idx",
            "@name:$q",
            load=["name"],
            verbatim=True,
            timeout=timedelta(seconds=1),
            dialect=3,
            parameters={"q": "tokyo"},
        )
        assert results.results[0]["name"] == ["tokyo"]

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_aggregation_load_fields(self, client: Redis, city_index, index_name):
        results = await client.search.aggregate(
            index_name,
            "*",
            load=["name"],
            verbatim=True,
            timeout=timedelta(seconds=1),
        )
        assert list(results.results[0].keys()) == ["name"]

        results = await client.search.aggregate(index_name, "*", load="*")
        assert set(results.results[0].keys()).issuperset(
            {
                "name",
                "country",
                "summary_text",
                "summary_vector",
                "population",
                "tags",
                "location",
            }
        )

        results = await client.search.aggregate(index_name, "*", load=[("name", "nom")])
        assert list(results.results[0].keys()) == ["nom"]

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_sort(self, client: Redis, city_index, index_name):
        results = await client.search.aggregate(
            index_name,
            "*",
            load=["name"],
            sortby={"@population": PureToken.DESC},
        )
        assert results.results[0]["name"] == "tokyo"

        results = await client.search.aggregate(
            index_name,
            "*",
            load=["name"],
            sortby={"@population": PureToken.DESC},
            sortby_max=1,
        )
        assert len(results.results) == 1
        assert results.results[0]["name"] == "tokyo"

        results = await client.search.aggregate(
            index_name,
            "@country:Bangladesh|Pakistan",
            load=["name"],
            sortby={"@country": PureToken.ASC, "@population": PureToken.DESC},
        )
        assert ["dhaka", "karachi", "lahore"] == [k["name"] for k in results.results]

        results = await client.search.aggregate(
            index_name,
            "@country:Bangladesh|Pakistan",
            load=["name"],
            sortby={"@country": PureToken.ASC, "@population": PureToken.DESC},
            limit=1,
        )
        assert ["dhaka"] == [k["name"] for k in results.results]

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_group_by(self, client: Redis, city_index, index_name):
        results = await client.search.aggregate(
            index_name,
            "*",
            transforms=[Group("@country")],
        )
        assert "Pakistan" in [k["country"] for k in results.results]

        results = await client.search.aggregate(
            index_name,
            "*",
            transforms=[Group("@country", [(Reduce("count", [0], "count"))])],
        )

        assert [2] == [
            int(k["count"]) for k in results.results if k["country"] == "Pakistan"
        ]

        results = await client.search.aggregate(
            index_name,
            "*",
            load=["country", "population"],
            transforms=[
                Apply("floor(log(@population))", "population_log"),
                Group(["@country", "@population_log"], [Reduce("count", [0], "count")]),
            ],
        )

        assert {"17": "1", "16": "2"} == {
            k["population_log"]: k["count"]
            for k in results.results
            if k["country"] == "Japan"
        }

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_multi_stage_transforms(self, client: Redis, city_index, index_name):
        results = await client.search.aggregate(
            index_name,
            "*",
            transforms=[
                Group(
                    "@country",
                    [
                        Reduce("tolist", [1, "@name"], "cities"),
                        Reduce("max", [1, "@population"], "max_city_population"),
                    ],
                ),
                Apply(
                    "floor(@max_city_population/1000000.0)",
                    "max_city_population_in_millions",
                ),
                Group(
                    "@max_city_population_in_millions", [Reduce("count", [0], "count")]
                ),
                Filter("@max_city_population_in_millions < 30"),
            ],
            sortby={
                "@count": PureToken.DESC,
                "@max_city_population_in_millions": PureToken.DESC,
            },
        )

        assert int(results.results[0]["count"]) == 3
        assert int(results.results[-1]["count"]) == 1

    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_aggregation_with_cursor(self, client: Redis, city_index, index_name):
        results = await client.search.aggregate(
            index_name,
            "*",
            load=["name"],
            with_cursor=True,
            cursor_read_size=5,
            cursor_maxidle=timedelta(milliseconds=100),
        )
        assert len(results.results) == 5

        cursor_results = await client.search.cursor_read(
            index_name, results.cursor, count=2
        )
        assert len(cursor_results.results) == 2

        assert await client.search.cursor_del(index_name, cursor_results.cursor)

        with pytest.raises(ResponseError):
            await client.search.cursor_read(index_name, cursor_results.cursor)

    @pytest.mark.parametrize("transaction", [True, False])
    async def test_pipeline(self, client: Redis, transaction: bool):
        p = await client.pipeline(transaction=transaction)
        await p.search.create(
            "{search}:idx",
            [
                Field("name", PureToken.TEXT),
            ],
            on=PureToken.HASH,
            prefixes=["{search}:"],
        )
        await p.hset("{search}:doc:1", {"name": "hello"})
        await p.hset("{search}:doc:2", {"name": "world"})
        await p.search.aggregate(
            "{search}:idx",
            "*",
            transforms=[Group("@name", [Reduce("count", [0], "count")])],
        )
        assert (
            True,
            1,
            1,
            SearchAggregationResult(
                [{"name": "hello", "count": "1"}, {"name": "world", "count": "1"}],
                None,
            ),
        ) == await p.execute()
