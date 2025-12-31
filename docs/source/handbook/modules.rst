Redis Modules
-------------
.. currentmodule:: coredis

coredis contains built in support for a few popular :term:`Redis Modules`.

The commands exposed by the individual modules follow the same API conventions
as the core builtin commands and can be accessed via the appropriate
command group property (such as :attr:`~Redis.json`, :attr:`~Redis.cf`, :attr:`~Redis.timeseries` etc...)
of :class:`Redis` or :class:`RedisCluster`.

For example::

    client = coredis.Redis()
    async with client:
        # RedisJSON
        await client.json.get("key")
        # RediSearch
        await client.search.search("index", "*")
        # RedisBloom:BloomFilter
        await client.bf.reserve("bf", 0.001, 1000)
        # RedisBloom:CuckooFilter
        await client.cf.reserve("cf", 1000)
        # RedisTimeSeries
        await client.timeseries.add("ts", 1, 1)


Module commands can also be used in :ref:`handbook/pipelines:pipelines` (and transactions)
by accessing them via the command group property in the same way as described above.

For example::

    async with client.pipeline(transaction=True) as pipe:
        pipe.json.get("key")
        pipe.json.get("key")
        pipe.search.search("index", "*")
        pipe.bf.reserve("bf", 0.001, 1000)
        pipe.cf.reserve("cf", 1000)
        pipe.timeseries.add("ts", 1, 1)
        pipe.graph.query("graph", "CREATE (:Node {name: 'Node'})")


RedisJSON
^^^^^^^^^
``RedisJSON`` adds native support for storing and retrieving JSON documents.

To access the commands exposed by the module use the :attr:`~Redis.json` property
or manually instantiate the :class:`~modules.Json` class with an instance of
:class:`~Redis` or  :class:`~RedisCluster`

Get/set operations::

    import coredis
    client = coredis.Redis()

    async with client:
        await client.json.set(
            "key1", ".", {"a": 1, "b": [1, 2, 3], "c": "str"}
        )
        assert 1 == await client.json.get("key1", ".a")
        assert [1,2,3] == await client.json.get("key1", ".b")
        assert "str" == await client.json.get("key1", ".c")

        await client.json.set("key2", ".", {"a": 2, "b": [4,5,6], "c": ["str"]})

        # multi get
        assert ["str", ["str"]] == await client.json.mget(["key1", "key2"], ".c")

Clear versus Delete::

    await client.json.set(
        "key1", ".", {"a": 1, "b": [1, 2, 3], "c": "str", "d": {"e": []}}
    )

    # a numeric value
    assert 1 == await client.json.clear("key1", ".a")
    assert 0 == await client.json.get("key1", ".a")
    assert 1 == await client.json.delete("key1", ".a")
    assert {"b", "c", "d"} == set(await client.json.objkeys("key1"))

    # an array
    assert 1 == await client.json.clear("key1", ".b")
    assert [] == await client.json.get("key1", ".b")
    assert 1 == await client.json.delete("key1", ".b")
    assert {"d", "c"} == set(await client.json.objkeys("key1"))

    # a string
    assert 0 == await client.json.clear("key1", ".c")
    assert "str" == await client.json.get("key1", ".c")
    assert 1 == await client.json.delete("key1", ".c")
    assert ["d"] == await client.json.objkeys("key1")

    # an object
    assert 1 == await client.json.clear("key1", ".d")
    assert {} == await client.json.get("key1", ".d")
    assert 1 == await client.json.delete("key1", ".d")
    assert [] == await client.json.objkeys("key1")



Array operations::

    await client.json.set("key", ".", [])
    assert 1 == await client.json.arrappend("key", [1], ".")
    assert 0 == await client.json.arrindex("key", ".", 1)
    assert 3 == await client.json.arrappend("key", [2, 3], ".")
    assert 4 == await client.json.arrinsert("key", ".", 0, [-1])
    assert [-1, 1, 2, 3] == await client.json.get("key", ".")




For more details refer to the API documentation for :class:`~coredis.modules.Json`

.. note:: By default coredis uses the :mod:`json` module from the python standard library
   to serialize inputs and deserialize the responses to :class:`~coredis.modules.response.types.JsonType`.
   If, however the :pypi:`orjson` package is installed, coredis will transparently use it for improved
   performance.

RediSearch
^^^^^^^^^^
``RediSearch`` adds support for indexing ``hash`` and ``json`` datatypes and performing
search, aggregation, suggestion & autocompletion.

Search & Aggregation
====================
To access the search & aggregation related commands exposed by the module use the
:attr:`~Redis.search` property or manually instantiate the :class:`~modules.Search`
class with an instance of :class:`~Redis` or  :class:`~RedisCluster`

===============
Create an Index
===============
Since creating an index requires a non-trivial assembly of arguments that are sent to the
:rediscommand:`FT.CREATE`, the coredis method :meth:`~coredis.modules.Search.create` accepts a
collection of :class:`~coredis.modules.search.Field` instances as an argument to
:paramref:`~coredis.modules.Search.create.schema`.


The example below creates two similar indices for json and hash data, that demonstrate
some common field definitions::

    import coredis
    import coredis.modules
    client = coredis.Redis(decode_responses=True)
    async with client:
        # Create an index on json documents
        await client.search.create("json_index", on=coredis.PureToken.JSON, schema = [
            coredis.modules.search.Field('$.name', coredis.PureToken.TEXT, alias='name'),
            coredis.modules.search.Field('$.country', coredis.PureToken.TEXT, alias='country'),
            coredis.modules.search.Field('$.population', coredis.PureToken.NUMERIC, alias='population'),
            coredis.modules.search.Field("$.location", coredis.PureToken.GEO, alias='location'),
            coredis.modules.search.Field('$.iso_tags', coredis.PureToken.TAG, alias='iso_tags'),
            coredis.modules.search.Field('$.summary_vector', coredis.PureToken.VECTOR, alias='summary_vector',
                algorithm="FLAT",
                attributes={
                    "DIM": 768,
                    "DISTANCE_METRIC": "COSINE",
                    "TYPE": "FLOAT32",
                }
            )

        ], prefixes=['json:city:'])

        # or on all hashes that start with a prefix ``city:``
        await client.search.create("hash_index", on=coredis.PureToken.HASH, schema = [
            coredis.modules.search.Field('name', coredis.PureToken.TEXT),
            coredis.modules.search.Field('country', coredis.PureToken.TEXT),
            coredis.modules.search.Field('population', coredis.PureToken.NUMERIC),
            coredis.modules.search.Field("location", coredis.PureToken.GEO),
            coredis.modules.search.Field('iso_tags', coredis.PureToken.TAG, separator=","),
            coredis.modules.search.Field('summary_vector', coredis.PureToken.VECTOR,
                algorithm="FLAT",
                attributes={
                    "DIM": 768,
                    "DISTANCE_METRIC": "COSINE",
                    "TYPE": "FLOAT32",
                }
            )
        ], prefixes=['city:'])

To populate the indices we can add some sample city data (a sample that can be used for the above
index definition can be found `in the coredis repository <https://raw.githubusercontent.com/alisaifee/coredis/master/tests/modules/data/city_index.json>`__)
using a pipeline for performance::


    import requests
    import numpy

    cities = requests.get(
        "https://raw.githubusercontent.com/alisaifee/coredis/master/tests/modules/data/city_index.json"
    ).json()

    async with client.pipeline(transaction=False) as pipe:
        for name, fields in cities.items():
            pipe.json.set(f"json:city:{name}", f".", {
                "name": name,
                "country": fields["country"],
                "population": int(fields["population"]),
                "location": f"{fields['lng']},{fields['lat']}",
                "iso_tags": fields["iso_tags"],
                "summary_vector": fields["summary_vector"],
            })

            pipe.hset(f"city:{name}", {
                "name": name,
                "country": fields["country"],
                "population": fields["population"],
                "location": f"{fields['lng']},{fields['lat']}",
                "iso_tags": ",".join(fields["iso_tags"]),
                "summary_vector": numpy.asarray(fields["summary_vector"]).astype(numpy.float32).tobytes(),
            })

.. note:: Take special note of how the ``population`` (numeric field), ``iso_tags`` (tag field) & ``summary_vector`` (vector field)
   fields are handled differently in the case of hashes vs json documents.

Inspect the index information::


    json_index_info = await client.search.info("json_index")
    hash_index_info = await client.search.info("hash_index")


    assert (0.0, 50) == (json_index_info["indexing"], json_index_info["num_docs"])
    assert (0.0, 50) == (hash_index_info["indexing"], json_index_info["num_docs"])

======
Search
======

Searching for documents is done through the :meth:`~coredis.modules.Search.search`
function that provides the interface to the :rediscommand:`FT.SEARCH`. The returned
search results are represented by the :class:`~coredis.modules.response.types.SearchResult` class.


Perform a simple text search::

    results = await client.search.search("json_index", "Tok*", returns={"name": None, "country": "country"})
    # or with the hash index
    # results = await client.search.search("hash_index", "Tok*", returns={"name": None, "country": "country"})
    assert results.total == 1
    assert results.documents[0].properties["country"] == "Japan"

Perform a geo filtered search::

    results = await client.search.search(
        "json_index",
        "*",
        geo_filters={"location": ((67.0011, 24.8607), 1, coredis.PureToken.KM)},
        returns={"name": None},
    )
    assert results.total == 1
    assert results.documents[0].properties["name"] == "karachi"

Perform a vector similarity search::


    from sentence_transformers import SentenceTransformer

    query = SentenceTransformer(
        "sentence-transformers/all-distilroberta-v1",
    ).encode("The fishing village called Edo").astype(numpy.float32).tobytes()

    results = await client.search.search(
        "json_index",
        "*=>[KNN 1 @summary_vector $query_vec as query_score]",
        parameters={"query_vec": query},
        returns={"name": None},
        dialect=2,
    )

    assert results.documents[0].properties["name"] == "tokyo"

.. note:: The vector similarity search example above uses a pre-trained sentence transformer model
   to encode the query string into a vector. The ``query_vec`` parameter is then passed to the
   ``KNN`` operator to perform a vector similarity search. This ofcourse requires that the ``summary_vector``
   field in the index was encoded using the same model (which is the case for the sample data referenced
   in the earlier example when populating the index).


===========
Aggregation
===========

To perform aggregations use the :meth:`~coredis.modules.Search.aggregate`
method that provides the interface to the :rediscommand:`FT.AGGREGATE`.

To simplify construction of transformation steps in the aggregation pipeline a few helper
dataclasses are provided to construct the pipeline steps for the :paramref:`~coredis.modules.Search.aggregate.transforms`
parameter.

  - :class:`~coredis.modules.search.Filter`
  - :class:`~coredis.modules.search.Group`
  - :class:`~coredis.modules.search.Reduce`
  - :class:`~coredis.modules.search.Apply`

The results of the aggregation are represented by the :class:`~coredis.modules.response.types.SearchAggregationResult` class.


Group by country and count and sort by count desc::

    aggregations = await client.search.aggregate(
        "hash_index",
        "*",
        load="*",
        transforms=[
            # group by country=>count
            coredis.modules.search.Group(
                "@country", [
                    coredis.modules.search.Reduce("count", [0], "city_count"),
                 ]
            ),
        ],
        sortby={"@city_count": coredis.PureToken.DESC},
    )

    assert "China" == aggregations.results[0]["country"]
    assert 18 == int(aggregations.results[0]["city_count"])


Filter, Group->Reduce, Apply, Group::

    aggregations = await client.search.aggregate(
        "hash_index",
        "*",
        load="*",
        transforms=[
            # include only cities with population greater than 20 million
            coredis.modules.search.Filter(
                '@population > 20000000'
            ),
            # group by country=>{count, average_city_population}
            coredis.modules.search.Group(
                "@country", [
                    coredis.modules.search.Reduce("count", [0], "city_count"),
                    coredis.modules.search.Reduce("avg", [1, '@population'], "average_city_population")
                 ]
            ),
            # apply a transformation of average_city_population -> log10(average_city_population)
            coredis.modules.search.Apply(
                "floor(log(@average_city_population))",
                "average_population_bucket"
            ),
            # group by average_population_bucket=>countries
            coredis.modules.search.Group(
                "@average_population_bucket", [
                    coredis.modules.search.Reduce("tolist", [1, "@country"], "countries"),
                 ]
            ),
        ],
    )
    assert aggregations.results[0] == {
        'average_population_bucket': '16',
        'countries': ['Brazil', 'South Korea', 'Egypt', 'Mexico']
    }
    assert aggregations.results[1] == {
        'average_population_bucket': '17',
        'countries': ['Japan', 'Indonesia', 'China', 'Philippines', 'India']
    }



For more details refer to the API documentation for :class:`~coredis.modules.Search`

Autocomplete
============
To access the commands from the ``SUGGEST`` group of the ``RedisSearch`` module
use the :attr:`~Redis.autocomplete` property or manually instantiate the
:class:`~modules.autocomplete.Autocomplete` class with an instance of :class:`~Redis`
or  :class:`~RedisCluster`

Add some terms, each with an associated score to a group::

    await client.autocomplete.sugadd("cities", "New Milton", 5.0)
    await client.autocomplete.sugadd("cities", "New Port", 4.0)
    await client.autocomplete.sugadd("cities", "New Richmond", 3.0)
    await client.autocomplete.sugadd("cities", "New Albany", 2.0)
    await client.autocomplete.sugadd("cities", "New York", 1.0)

Fetch some suggestions::

    suggestions = await client.autocomplete.sugget("cities", "new")
    assert 5 == len(suggestions)
    assert "New Milton" == suggestions[0].string

    suggestions = await client.autocomplete.sugget("cities", "new po")
    assert "New Port" == suggestions[0].string

Boost the score of a term::

    await client.autocomplete.sugadd("cities", "New York", 5.0, increment_score=True)

    suggestions = await client.autocomplete.sugget("cities", "new")
    assert "New York" == suggestions[0].string

For more details refer to the API documentation for :class:`~coredis.modules.autocomplete.Autocomplete`

RedisBloom
^^^^^^^^^^
The probabilistic datastructures exposed by ``RedisBloom`` can be accessed
through coredis using the following properties (or explicitly by instantiating
the associated module exposed by the property):

BloomFilter
===========

:attr:`Redis.bf` / :attr:`RedisCluster.bf`

.. code-block::

    import coredis
    client = coredis.Redis()
    async with client:
        # create filter
        await client.bf.reserve("filter", 0.1, 1000)

        # add items
        await client.bf.add("filter", 1)
        await client.bf.madd("filter", [2,3,4])

        # test for inclusion
        assert await client.bf.exists("filter", 1)
        assert (True, False) == await client.bf.mexists("filter", [2,5])

        # or
        assert await coredis.modules.BloomFilter(client).exists("filter", 1)
        ...

For more details refer to the API documentation for :class:`~coredis.modules.BloomFilter`

CuckooFilter
============

:attr:`Redis.cf` / :attr:`RedisCluster.cf`

.. code-block::

  # create filter
  await client.cf.reserve("filter", 1000)

  # add items
  assert await client.cf.add("filter", 1)
  assert not await client.cf.addnx("filter", 1)

  # test for inclusion
  assert await client.cf.exists("filter", 1)
  assert 1 == await client.cf.count("filter", 1)

  # delete an item
  assert await client.cf.delete("filter", 1)

  # test for inclusion
  assert not await client.cf.exists("filter", 1)
  assert 0 == await client.cf.count("filter", 1)

For more details refer to the API documentation for :class:`~coredis.modules.CuckooFilter`

CountMinSketch
==============

:attr:`Redis.cms` / :attr:`RedisCluster.cms`

.. code-block::

  # create a sketch
  await client.cms.initbydim("sketch", 2, 50)

  # increment the counts for multiple entries
  assert (1, 2) == await client.cms.incrby("sketch", {"a": 1, "b": 2})

  # query the count for multiple entries
  assert (1, 2, 0) == await client.cms.query("sketch", ["a", "b", "c"])

For more details refer to the API documentation for :class:`~coredis.modules.CountMinSketch`

TopK
====

:attr:`Redis.topk` / :attr:`RedisCluster.topk`

.. code-block::

  import string
  import itertools
  import random

  # create a top-3
  await client.topk.reserve("top3", 3)

  # add entries
  letters = list(itertools.chain(*[k[0]*k[1] for k in list(enumerate(string.ascii_lowercase))]))
  random.shuffle(letters)
  await client.topk.add("top3", letters)

  # get top 3 letters
  assert (b'z', b'y', b'x') == await client.topk.list("top3")

For more details refer to the API documentation for :class:`~coredis.modules.TopK`

TDigest
=======

:attr:`Redis.tdigest` / :attr:`RedisCluster.tdigest`

.. code-block::

    # create a digest
    await client.tdigest.create("digest")

    # add some values
    await client.tdigest.add("digest", 1, [1, 2, 3, 4])

    # add some more values
    await client.tdigest.add("digest", 1, [1, 2, 3, 4])

    # get the rank & reverse ranks
    assert (1.0, 1.0, 2.0) == await client.tdigest.byrank("digest", [0, 1, 2])
    assert (6.0, 5.0, 4.0) == await client.tdigest.byrevrank("digest", [0, 1, 2])

    # get the quantiles
    assert (1.0, 3.0, 6.0) == await client.tdigest.quantile("digest", [0, 0.5, 1])

For more details refer to the API documentation for :class:`~coredis.modules.TDigest`

RedisTimeSeries
^^^^^^^^^^^^^^^
``RedisTimeSeries`` adds a time series data structure to Redis that allows ingesting
and querying time series'.

To access the commands exposed by the module use the :attr:`~Redis.timeseries` property
or manually instantiate the :class:`~modules.TimeSeries` class with an instance of
:class:`~Redis` or  :class:`~RedisCluster`

The below examples use random temperature data captured every few minutes
for different rooms in a house.

Create a few timeseries with different labels (:meth:`~modules.TimeSeries.create`)::

    import coredis
    from datetime import datetime, timedelta

    rooms = {"bedroom", "lounge", "bathroom"}
    client = coredis.Redis(port=9379)

    async with client:
        for room in rooms:
            assert await client.timeseries.create(f"temp:{room}", labels={"room": room})

Create compaction rules for hourly and daily averages (:meth:`~modules.TimeSeries.createrule`)::

    for room in rooms:
        assert await client.timeseries.create(
            f"temp:{room}:hourly:avg", labels={"room": room, "compaction": "hourly"}
        )
        assert await client.timeseries.create(
            f"temp:{room}:daily:avg", labels={"room": room, "compaction": "daily"}
        )

        assert await client.timeseries.createrule(
            f"temp:{room}", f"temp:{room}:hourly:avg",
            coredis.PureToken.AVG, timedelta(hours=1)
        )
        assert await client.timeseries.createrule(
            f"temp:{room}", f"temp:{room}:daily:avg",
            coredis.PureToken.AVG, timedelta(hours=24)
        )

Populate a year of random sample data (:meth:`~modules.TimeSeries.add`)::

    import random
    cur = datetime.fromtimestamp(0)
    async with client.pipeline(transaction=True) as pipe:
        while cur < datetime(1971, 1, 1, 0, 0, 0):
            cur += timedelta(minutes=random.randint(1, 60))
            for room in rooms:
                pipe.timeseries.add(f"temp:{room}", cur, random.randint(15, 30))

Query for the latest temperature in each room (:meth:`~modules.TimeSeries.get`)::

    for room in rooms:
        print(await client.timeseries.get(f"temp:{room}"))

Query for latest temperature in all rooms (:meth:`~modules.TimeSeries.mget`)::

    print(await client.timeseries.mget(
        filters=[f"room=({','.join(rooms)})", "compaction="])
    )

Query for daily averages by individual room (:meth:`~modules.TimeSeries.range` & :meth:`~modules.TimeSeries.mrange`)::

    # using individual range queries on the compacted timeseries
    for room in rooms:
        print(await client.timeseries.range(
            f"temp:{room}:daily:avg", 0, datetime(1971, 1, 1),
        ))

    # using individual range queries with an aggregation on the original timeseries
    for room in rooms:
        print(await client.timeseries.range(
            f"temp:{room}", 0, datetime(1971, 1, 1),
            aggregator=coredis.PureToken.AVG,
            bucketduration=timedelta(hours=24),
        ))

    # using a multi range query on the compacted series
    print(
        await client.timeseries.mrange(
            0, datetime(1971, 1, 1), filters=["compaction=daily"]
        )
    )

    # using a multi range query with aggregation on the original series
    print(
        await client.timeseries.mrange(
            0, datetime(1971, 1, 1),
            aggregator=coredis.PureToken.AVG,
            bucketduration=timedelta(hours=24),
            filters=[f"room=({','.join(rooms)})", "compaction="]
        )
    )

For more details refer to the API documentation for :class:`~coredis.modules.TimeSeries`
