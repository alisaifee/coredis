coredis
=======

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

Feature Summary
===============

* Clients for different deployments

  * :class:`~coredis.Redis`
  * :class:`~coredis.RedisCluster`
  * :class:`~coredis.sentinel.Sentinel`

* Application patterns

  * :ref:`api_reference:pubsub`
  * :ref:`api_reference:cluster pub/sub`
  * :ref:`api_reference:pipelines`

* Server side scripting

  * :ref:`api_reference:scripting`
  * :ref:`api_reference:library functions`

* Miscellaneous

  * :ref:`api_reference:resp3` Support
  * "Pretty complete" type annotations for public API

    .. command-output:: PYTHONPATH=$(pwd) pyright --verifytypes=coredis 2>&1 | grep completeness
       :shell:
       :cwd: ../../

  * :ref:`api_reference:runtime type checking`

Installation
------------

.. code-block:: bash

    $ pip install coredis


Getting started
---------------

Single Node client
^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^

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

The coredis :ref:`api:clients` attempt to mirror the specifications in
the `Redis command documentation <https://redis.io/commands>`__ by using the following rules:

- Arguments retain naming from redis as much as possible
- **Only** optional variadic arguments are mapped to ``*args`` or ``**kwargs``. When
  the variable length arguments are not optional the expected argument is an
  :class:`~typing.Iterable` or :class:`~typing.Mapping`.
- Pure tokens used as flags are mapped to boolean arguments
- ``One of`` arguments accepting pure tokens are collapsed and accept a :class:`~coredis.PureToken`
- Responses are mapped as closely from redis <-> python types as possible.

For higher level concepts such as :ref:`api_reference:pipelines`, :ref:`api_reference:scripting` and
:ref:`api_reference:pubsub` abstractions are provided to simplify interaction requires pre-defined
sequencing of redis commands (see :ref:`api:command wrappers`). For details see :ref:`api_reference:api reference`

The redis command API does **NOT** mirror :pypi:`redis`.
For details about the high level differences refer to :ref:`history:divergence from aredis & redis-py`

Dependencies & supported python versions
----------------------------------------
coredis is tested against redis versions ``6.0.x``, ``6.2.x`` & ``7.0.x``. The
test matrix status can be reviewed `here <https://github.com/alisaifee/coredis/actions/workflows/main.yml>`__

coredis is additionally tested against:

- :pypi:`hiredis` >= `2.0.0`.
- :pypi:`uvloop` >= `0.15.0`.

:pypi:`hiredis` if available will be used by default as the RESP parser as it provides
significant performance gains in response parsing. For more details see :ref:`api_reference:parsers`.

Supported python versions
^^^^^^^^^^^^^^^^^^^^^^^^^

- 3.8
- 3.9
- 3.10


.. toctree::
    :maxdepth: 2
    :hidden:

    api_reference
    api
    compatibility
    sentinel
    release_notes
    testing
    history
    glossary
