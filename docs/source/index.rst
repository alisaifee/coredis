=======
coredis
=======
.. meta::
  :google-site-verification: qMs8VG0PBPYnnP-xkJxhuhlVq-1W2jlgSORpsYi26NQ

.. container:: badges

   .. image:: https://img.shields.io/github/actions/workflow/status/alisaifee/coredis/main.yml?logo=github&style=for-the-badge&labelColor=#282828
      :alt: CI status
      :target: https://github.com/alisaifee/coredis/actions?query=branch%3Amaster+workflow%3ACI
      :class: header-badge

   .. image::  https://img.shields.io/pypi/v/coredis.svg?style=for-the-badge
      :target: https://pypi.python.org/pypi/coredis/
      :alt: Latest Version in PyPI
      :class: header-badge

   .. image:: https://img.shields.io/pypi/pyversions/coredis.svg?style=for-the-badge
      :target: https://pypi.python.org/pypi/coredis/
      :alt: Supported Python versions
      :class: header-badge

   .. image:: https://img.shields.io/codecov/c/github/alisaifee/coredis?logo=codecov&style=for-the-badge&labelColor=#282828
      :target: https://codecov.io/gh/alisaifee/coredis
      :alt: Code coverage
      :class: header-badge

Fast, async, fully-typed Redis client with support for cluster and sentinel

The client API uses the specifications in the Redis command documentation to define the API by using the following conventions:

The coredis :ref:`api/clients:clients` use the specifications in
the `Redis command documentation <https://redis.io/commands>`__ to define the API by using the following conventions:

- Arguments retain naming from Redis as much as possible
- **Only** optional variadic arguments are mapped to position or keyword variadic arguments. When
  the variable length arguments are not optional the expected argument is an
  iterable of type :class:`~coredis.typing.Parameters` or :class:`~typing.Mapping`.
- Pure tokens used as flags are mapped to boolean arguments
- ``One of`` arguments accepting pure tokens are collapsed and accept a :class:`~coredis.tokens.PureToken`
- Responses are mapped as closely from :term:`RESP3` to python types as possible (See :ref:`api/typing:response types`).

For higher level concepts such as :ref:`handbook/pipelines:pipelines`, :ref:`handbook/scripting:lua scripts`,
:ref:`handbook/pubsub:pubsub` abstractions are provided to encapsulate recommended patterns.

Feature Summary
===============

* Clients for different topologies

  * :class:`~coredis.Redis`
  * :class:`~coredis.RedisCluster`
  * :class:`~coredis.Sentinel`

* Application patterns

  * :ref:`handbook/connections:connection pools`
  * :ref:`handbook/pubsub:pubsub`
  * :ref:`handbook/pubsub:cluster pub/sub`
  * :ref:`handbook/pubsub:sharded pub/sub`
  * :ref:`handbook/streams:streams`
  * :ref:`handbook/pipelines:pipelines`
  * :ref:`handbook/caching:caching`
  * :ref:`handbook/credentials:credential providers`

* Server side scripting

  * :ref:`handbook/scripting:lua scripts`
  * :ref:`handbook/scripting:library functions`

* :ref:`handbook/modules:redis modules`

  * :ref:`handbook/modules:RedisJSON`
  * :ref:`handbook/modules:RediSearch`
  * :ref:`handbook/modules:RedisBloom`
  * :ref:`handbook/modules:RedisTimeSeries`

* Miscellaneous

  * "Pretty complete" :ref:`handbook/typing:type annotations` for public API
  * :ref:`handbook/typing:runtime type checking`

Installation
============

Released versions of coredis published on `pypi <https://pypi.org/project/coredis/>`__ contain
precompiled native extensions for various architectures that provide significant performance improvements
especially when dealing with large bulk responses.

.. code-block:: bash

    $ pip install coredis

Getting started
===============

Single Node or Cluster client
-----------------------------

.. code-block:: python

    import anyio
    import coredis

    async def main() -> None:
        client = coredis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
        # or cluster
        # client = coredis.RedisCluster(startup_nodes=[{"host": "127.0.0.1", "port": 6379}], decode_responses=True)
        async with client:
            await client.flushdb()

            await client.set("foo", 1)
            assert await client.exists(["foo"]) == 1
            assert await client.incr("foo") == 2
            assert await client.expire("foo", 1)
            await anyio.sleep(0.1)
            assert await client.ttl("foo") == 1
            await anyio.sleep(1)
            assert not await client.exists(["foo"])

            async with client.pipeline() as pipeline:
                pipeline.incr("foo")
                value = pipeline.get("foo")
                pipeline.delete(["foo"])

            assert await value == "1"

    anyio.run(main, backend="asyncio") # or trio

Sentinel
--------

.. code-block:: python

    import anyio
    from coredis.sentinel import Sentinel

    async def main() -> None:
        sentinel = Sentinel(sentinels=[("localhost", 26379)])
        async with sentinel:
            primary = sentinel.primary_for("myservice")
            replica = sentinel.replica_for("myservice")

            async with primary, replica:
                assert await primary.set("fubar", 1)
                assert int(await replica.get("fubar")) == 1

    anyio.run(main, backend="asyncio") # or trio


Compatibility
=============

**coredis** is tested against redis versions >= ``7.0``
The test matrix status can be reviewed `here <https://github.com/alisaifee/coredis/actions/workflows/main.yml>`__

coredis is additionally tested against:

- :pypi:`uvloop` >= `0.15.0`
- :pypi:`trio`

Supported python versions
-------------------------

- 3.10
- 3.11
- 3.12
- 3.13
- PyPy 3.10

Support for Redis API compatible databases
------------------------------------------

**coredis** is known to work with the following databases that have redis protocol compatibility:

- `Dragonfly <https://dragonflydb.io/>`__
- `Valkey <https://valkey.io/>`__
- `Redict <https://redict.io/>`__

Compatibility tests for the above are included in the
continuous integration test matrix `here <https://github.com/alisaifee/coredis/actions/workflows/main.yml>`__.

.. toctree::
    :maxdepth: 2
    :hidden:

    handbook/index
    compatibility
    api/index
    recipes/index
    release_notes
    history
    glossary
