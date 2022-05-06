Glossary
========
.. glossary::

   Redis
    `Redis <https://redis.io/docs/about/>`__ is an open source, in-memory data
    structure store used as a database, cache, message broker, and streaming engine.
    Redis provides data structures such as strings, hashes, lists, sets, sorted
    sets with range queries, bitmaps hyperloglogs, geospatial indexes, and streams.

   RESP
     Redis clients use Redis serialization protocol (RESP) specification to communicate
     with the Redis server. For more detials see `RESP protocol spec <https://redis.io/docs/reference/protocol-spec/>`__

   RESP3
     RESP3 extends :term:`RESP` to include support for primitives such as double & boolean
     and for container structures such as maps & sets. For more details see the
     `RESP3 Specification <https://github.com/antirez/RESP3/blob/master/spec.md>`__
