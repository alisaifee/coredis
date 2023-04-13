from __future__ import annotations

import json
from datetime import timedelta
from platform import python_version

import numpy
import pytest

from coredis import PureToken, Redis
from coredis.exceptions import ResponseError
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
    # TODO: figure out why large pipelines aren't working in 3.12
    use_pipeline = True
    if python_version() >= "3.12":
        use_pipeline = False
    if use_pipeline:
        client = await client.pipeline()
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
        prefixes=["{city}:"],
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
        prefixes=["{jcity}:"],
    )
    for name, city in data.items():
        await client.hset(
            f"{{city}}:{name}",
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
            },
        )
        await client.json.set(
            f"{{jcity}}:{name}",
            ".",
            {
                "name": city["name"],
                "country": city["country"],
                "tags": city["iso_tags"],
                "population": int(city["population"]),
                "location": f'{city["lng"]},{city["lat"]}',
                "summary_text": city["summary"],
                "summary_vector": city["summary_vector"],
            },
        )
    if use_pipeline:
        await client.execute()
    return data


@retryable(ConstantRetryPolicy((ValueError,), 5, 0.1))
async def wait_for_index(index_name, client: Redis):
    info = await client.search.info(index_name)
    if info["indexing"]:
        raise ValueError("Index not available")


@pytest.mark.min_module_version("search", "2.6.1")
@targets("redis_stack", "redis_stack_cluster")
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
        ids=lambda val: str(val),
    )
    async def test_create_index(self, client: Redis, on, type_args, args):
        name = "field" if on == PureToken.HASH else "$.field"
        assert await client.search.create(
            "idx",
            [
                Field(name, **args, **type_args),
            ],
            on=on,
        )
        assert ("idx",) == await client.search.list()
        info = await client.search.info("idx")
        assert info["index_name"] == "idx"
        assert info["index_definition"]["key_type"] == str(on)
        assert name in info["attributes"][0]

        assert await client.search.alter(
            "idx",
            Field(f"{name}_new", **type_args),
        )
        info = await client.search.info("idx")
        assert f"{name}_new" in info["attributes"][1]

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


@pytest.mark.min_module_version("search", "2.6.1")
@targets("redis_stack", "redis_stack_cluster")
class TestSearch:
    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_spellcheck(self, client: Redis, city_index, index_name):
        assert not (await client.search.spellcheck(index_name, "menil"))["menil"]
        assert (
            "manila"
            in (await client.search.spellcheck(index_name, "menil", distance=2))[
                "menil"
            ]
        )

        await client.search.dictadd("{city}custom", ["menila"])
        assert (
            "menila"
            in (
                await client.search.spellcheck(
                    index_name, "menil", distance=2, include="{city}custom"
                )
            )["menil"]
        )
        assert (
            "menila"
            not in (
                await client.search.spellcheck(
                    index_name, "menil", distance=2, exclude="{city}custom"
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
                )
            )["menil"]
        )

        await client.search.dictdel("{city}custom", ["menila"])
        assert () == await client.search.dictdump("{city}custom")
        assert (
            "menila"
            not in (
                await client.search.spellcheck(
                    index_name, "menil", distance=2, include="{city}custom"
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
            numeric_filters={"population": [35000000, "+inf"]},
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
            index_name, "@name:karachi", returns={"name": None}
        )
        assert results.total == 1
        assert results.documents[0].properties["name"] == "karachi"

        results = await client.search.search(
            index_name, "@name:karachi", returns={"name": "nom"}
        )
        assert results.total == 1
        assert results.documents[0].properties["nom"] == "karachi"

        results = await client.search.search(
            index_name, "@name:karachi", nocontent=True
        )
        assert () == results.documents

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
            index_name,
            "Olympics",
            summarize_fields=["summary_text"],
            highlight_fields=["summary_text"],
            highlight_tags=("<blink>", "</blink>"),
            returns={"summary_text": None},
        )
        assert (
            "Summer <blink>Olympics</blink>"
            in results.documents[0].properties["summary_text"]
        )

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
            index_name, "karachi", ["karachi", "kolachi"]
        )
        assert ["karachi"] == (await client.search.syndump(index_name))["kolachi"]
        await wait_for_index(index_name, client)
        assert (
            1
            == (
                await client.search.search(index_name, "@name:kolachi", nocontent=True)
            ).total
        )


@pytest.mark.min_module_version("search", "2.6.1")
@targets("redis_stack", "redis_stack_cluster")
class TestAggregation:
    @pytest.mark.parametrize("index_name", ["{city}idx", "{jcity}idx"])
    async def test_aggregation_no_transforms(
        self, client: Redis, city_index, index_name
    ):
        results = await client.search.aggregate(index_name, "*", load=["name"])
        assert {k["name"] for k in results.results} == set(city_index.keys())

        results = await client.search.aggregate(index_name, "*", load=["name"])
        assert {k["name"] for k in results.results} == set(city_index.keys())

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

        cursor_results = await client.search.cursor_read(index_name, results.cursor)
        assert len(cursor_results.results) == 5

        assert await client.search.cursor_del(index_name, cursor_results.cursor)

        with pytest.raises(ResponseError):
            await client.search.cursor_read(index_name, cursor_results.cursor)
