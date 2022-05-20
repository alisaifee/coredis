Redis Response
--------------

Parsers
^^^^^^^

Parser classes provide a way to control how responses from the Redis server
are parsed. coredis ships with two parser classes, the
:class:`~coredis.parsers.PythonParser` and the :class:`~coredis.parsers.HiredisParser`.
By default, coredis will attempt to use the :class:`~coredis.parsers.HiredisParser`
if you have the :pypi:`hiredis` package installed and will fallback to the
:class:`~coredis.parsers.PythonParser` otherwise.

Hiredis is a C library maintained by the core Redis team. Pieter Noordhuis was
kind enough to create Python bindings. Using Hiredis can provide up to a
10x speed improvement in parsing responses from the Redis server. The
performance increase is most noticeable when retrieving many pieces of data,
such as from LRANGE or SMEMBERS operations.


Hiredis is available on PyPI, and can be installed as an extra dependency to
coredis.


.. code-block:: bash

    $ pip install coredis[hiredis]


RESP3
^^^^^
.. versionadded:: 3.1.0

As of redis `6.0.0` clients can use the
:term:`RESP3` protocol which provides support for a much larger set of types (which reduces the need for clients
to "guess" what the type of a command's response should be). Hiredis versions `>=2.0.0`
supports ``RESP3`` and **coredis** provides the option to use it both with hiredis
and the pure python parser.  The structure of responses of from coredis is consistent
between :term:`RESP` (``protocol_version=2``) and :term:`RESP3` (``protocol_version=3``) protocols.

To opt in the :paramref:`~coredis.Redis.protocol_version` constructor parameter
can be set to ``3``.

.. code-block:: python

    r = coredis.Redis(protocol_version=3)


