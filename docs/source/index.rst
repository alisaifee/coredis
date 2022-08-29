=======
coredis
=======
.. meta::
  :google-site-verification: qMs8VG0PBPYnnP-xkJxhuhlVq-1W2jlgSORpsYi26NQ

.. container:: badges

   .. image:: https://img.shields.io/github/workflow/status/alisaifee/coredis/CI?logo=github&style=for-the-badge&labelColor=#282828
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

The coredis :ref:`api:clients` API follows the specifications in
the `Redis command documentation <https://redis.io/commands>`__ by using the following rules:

- Arguments retain naming from redis as much as possible
- **Only** optional variadic arguments are mapped to position or keyword variadic arguments. When
  the variable length arguments are not optional the expected argument is an
  iterable of type :class:`~coredis.typing.Parameters` or :class:`~typing.Mapping`.
- Pure tokens used as flags are mapped to boolean arguments
- ``One of`` arguments accepting pure tokens are collapsed and accept a :class:`~coredis.tokens.PureToken`
- Responses are mapped as closely from redis <-> python types as possible.

For higher level concepts such as :ref:`handbook/pipelines:pipelines`, :ref:`handbook/scripting:lua scripts`,
:ref:`handbook/pubsub:pubsub` abstractions are provided to simplify interaction requires pre-defined
sequencing of redis commands (see :ref:`api:command wrappers`) and the :ref:`handbook/index:handbook`.

.. warning:: The command API does **NOT** mirror the official python :pypi:`redis` client.
   For details about the high level differences refer to :ref:`history:divergence from aredis & redis-py`

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

* Server side scripting

  * :ref:`handbook/scripting:lua scripts`
  * :ref:`handbook/scripting:library functions`

* Miscellaneous

  * "Pretty complete" :ref:`handbook/typing:type annotations` for public API

    .. command-output:: PYTHONPATH=$(pwd) pyright --verifytypes=coredis 2>&1 | grep completeness
       :shell:
       :cwd: ../../

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

Single Node client
------------------

.. code-block:: python

    import asyncio
    from coredis import Redis

    async def example():
        client = Redis(host='127.0.0.1', port=6379, db=0)
        await client.flushdb()
        await client.set('foo', 1)
        assert await client.exists(['foo']) == 1
        await client.incr('foo')
        await client.incrby('foo', increment=100)

        assert int(await client.get('foo')) == 102
        await client.expire('foo', 1)
        await asyncio.sleep(0.1)
        await client.ttl('foo')
        await asyncio.sleep(1)
        assert not await client.exists(['foo'])

    if __name__ == "__main__":
        asyncio.run(example())

Cluster client
--------------

.. code-block:: python

    import asyncio
    from coredis import RedisCluster

    async def example():
        client = RedisCluster(host='172.17.0.2', port=7001)
        await client.flushdb()
        await client.set('foo', 1)
        await client.lpush('a', [1])
        print(await client.cluster_slots())
        await client.rpoplpush('a', 'b')
        assert await client.rpop('b') == b'1'

    if __name__ == "__main__":
        asyncio.run(example())

    # {(10923, 16383): [{'host': b'172.17.0.2', 'node_id': b'332f41962b33fa44bbc5e88f205e71276a9d64f4', 'server_type': 'master', 'port': 7002},
    # {'host': b'172.17.0.2', 'node_id': b'c02deb8726cdd412d956f0b9464a88812ef34f03', 'server_type': 'slave', 'port': 7005}],
    # (5461, 10922): [{'host': b'172.17.0.2', 'node_id': b'3d1b020fc46bf7cb2ffc36e10e7d7befca7c5533', 'server_type': 'master', 'port': 7001},
    # {'host': b'172.17.0.2', 'node_id': b'aac4799b65ff35d8dd2ad152a5515d15c0dc8ab7', 'server_type': 'slave', 'port': 7004}],
    # (0, 5460): [{'host': b'172.17.0.2', 'node_id': b'0932215036dc0d908cf662fdfca4d3614f221b01', 'server_type': 'master', 'port': 7000},
    # {'host': b'172.17.0.2', 'node_id': b'f6603ab4cb77e672de23a6361ec165f3a1a2bb42', 'server_type': 'slave', 'port': 7003}]}

Compatibility
=============

**coredis** is tested against redis versions ``6.0.x``, ``6.2.x`` & ``7.0.x``. The
test matrix status can be reviewed `here <https://github.com/alisaifee/coredis/actions/workflows/main.yml>`__

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

- 3.7
- 3.8
- 3.9
- 3.10
- PyPy 3.7
- PyPy 3.8
- PyPy 3.9

Support for Redis-"like" databases
----------------------------------

**coredis** is known to work with the following databases that have redis protocol compatibility:

`KeyDB <https://docs.keydb.dev/>`__
    KeyDB exposes a few commands that don't exist in redis and these are exposed by the :class:`~coredis.KeyDB`
    and :class:`~coredis.KeyDBCluster` clients respectively.

`Dragonfly <https://dragonflydb.io/>`__
    Dragonfly currently has compatibility with redis 2.8, though there is increasing support for commands from higher versions.
    For up to date details please refer to the `dragonfly api status documentation <https://github.com/dragonflydb/dragonfly/blob/main/docs/api_status.md>`__.

    To see which functionality of **coredis** is tested against dragonfly, checkout a copy of **coredis** and run the
    following::

        pytest --collect-only -m dragonfly

    .. warning:: Since dragonfly does not yet support RESP3 (which is the default protocol version
       for **coredis**) connecting to a dragonfly instance requires setting :paramref:`coredis.Redis.protocol_version` to ``2``.

Compatibility tests for the above are included in the
continuous integration test matrix `here <https://github.com/alisaifee/coredis/actions/workflows/main.yml>`__.

.. toctree::
    :maxdepth: 2
    :hidden:

    handbook/index
    compatibility
    api
    recipes/index
    release_notes
    history
    glossary
