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

coredis is an async redis client with support for redis server, cluster & sentinel.
The client API uses the specifications in the Redis command documentation to define the API by using the following conventions:

The coredis :ref:`api/clients:clients` use the specifications in
the `Redis command documentation <https://redis.io/commands>`__ to define the API by using the following conventions:

- Arguments retain naming from redis as much as possible
- **Only** optional variadic arguments are mapped to position or keyword variadic arguments. When
  the variable length arguments are not optional the expected argument is an
  iterable of type :class:`~coredis.typing.Parameters` or :class:`~typing.Mapping`.
- Pure tokens used as flags are mapped to boolean arguments
- ``One of`` arguments accepting pure tokens are collapsed and accept a :class:`~coredis.tokens.PureToken`
- Responses are mapped as closely from :term:`RESP` <-> python types as possible.

For higher level concepts such as :ref:`handbook/pipelines:pipelines`, :ref:`handbook/scripting:lua scripts`,
:ref:`handbook/pubsub:pubsub` abstractions are provided to encapsulate recommended patterns.

Feature Summary
===============

* Clients for different topologies

  * :class:`~coredis.Redis`
  * :class:`~coredis.RedisCluster`
  * :class:`~coredis.sentinel.Sentinel`

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

    import asyncio
    from coredis import Redis, RedisCluster

    async def example():
        client = Redis(host='127.0.0.1', port=6379, db=0)
        # or with redis cluster
        # client = RedisCluster(startup_nodes=[{"host": "127.0.01", "port": 7001}])
        await client.flushdb()
        await client.set('foo', 1)
        assert await client.exists(['foo']) == 1
        assert await client.incr('foo') == 2
        assert await client.incrby('foo', increment=100) == 102
        assert int(await client.get('foo')) == 102

        assert await client.expire('foo', 1)
        await asyncio.sleep(0.1)
        assert await client.ttl('foo') == 1
        assert await client.pttl('foo') < 1000
        await asyncio.sleep(1)
        assert not await client.exists(['foo'])

    asyncio.run(example())

Sentinel
--------

.. code-block:: python

    import asyncio
    from coredis.sentinel import Sentinel

    async def example():
        sentinel = Sentinel(sentinels=[("localhost", 26379)])
        primary = sentinel.primary_for("myservice")
        replica = sentinel.replica_for("myservice")

        assert await primary.set("fubar", 1)
        assert int(await replica.get("fubar")) == 1

    asyncio.run(example())


Compatibility
=============

**coredis** is tested against redis versions >= ``7.0``
The test matrix status can be reviewed `here <https://github.com/alisaifee/coredis/actions/workflows/main.yml>`__

.. note:: Though **coredis** officially only supports :redis-version:`6.0.0` and above it is known to work with lower
   versions.

   A known compatibility issue with older redis versions is the lack of support for :term:`RESP3` and
   the :rediscommand:`HELLO` command. The default :class:`~coredis.Redis` and :class:`~coredis.RedisCluster` clients
   do not work in this scenario as the :rediscommand:`HELLO` command is used for initial handshaking to confirm that
   the default ``RESP3`` protocol version can be used and to perform authentication if necessary.

   This can be worked around by passing ``2`` to :paramref:`coredis.Redis.protocol_version` to downgrade to :term:`RESP`
   (see :ref:`handbook/response:redis response`).

   When using :term:`RESP` **coredis** will also fall back to the legacy :rediscommand:`AUTH` command if the
   :rediscommand:`HELLO` is not supported.


coredis is additionally tested against:

- :pypi:`uvloop` >= `0.15.0`.

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
