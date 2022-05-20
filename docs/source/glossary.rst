Glossary
========
.. glossary::

   PEL
     Pending Entries List is an internal list maintained by redis for each stream
     and consumer group combination. The list is used to surface stream entries
     to consumers groups interested in revisiting entries that were not acknowledged.

   Pipelining
     A technique for improving performance by issuing multiple commands at once
     without waiting for the response for each individual command. For more details
     see the `Redis manual entry on pipelining <https://redis.io/docs/manual/pipelining/>`_.

   Pub/Sub
     Publish/Subscribe messaging paradigm where message senders (publishers) send messages
     to a channel without knowledge of the recipients of the message. Receipients subscribe
     to channels and asynchronously consume messages that they are interested in.
     For more details refer to the `Redis manual entry on Pub/Sub <https://redis.io/docs/manual/pubsub/>`__

   Redis
    `Redis <https://redis.io/docs/about/>`__ is an open source, in-memory data
    structure store used as a database, cache, message broker, and streaming engine.
    Redis provides data structures such as strings, hashes, lists, sets, sorted
    sets with range queries, bitmaps hyperloglogs, geospatial indexes, and streams.

   Redis Cluster
     Redis scales horizontally with a deployment topology called Redis Cluster.
     To learn more please read `Redis Cluster 101 <https://redis.io/docs/manual/scaling/#redis-cluster-101>`__

   RESP
     Redis clients use Redis serialization protocol (RESP) specification to communicate
     with the Redis server. For more detials see `RESP protocol spec <https://redis.io/docs/reference/protocol-spec/>`__

   RESP3
     RESP3 extends :term:`RESP` to include support for primitives such as double & boolean
     and for container structures such as maps & sets. For more details see the
     `RESP3 Specification <https://github.com/antirez/RESP3/blob/master/spec.md>`__

   Sharded Pub/Sub
     Support for Sharded Pub/Sub is available as of :redis-version:`7.0.0` and refers
     to routing messages to cluster nodes by applying the same alogrithm used to distribute
     keys to distribute channels. For more details see the `Redis manual entry on
     Sharded Pub/Sub <https://redis.io/docs/manual/pubsub/#sharded-pubsub>`__

   Streams
     Streams are essentially an abstract append-only in-memory log datastructure
     which can be used for various use cases such as timeseries,
     as queue for a Pub/Sub application architecture. For more details see the
     `Redis manual entry on streams <https://redis.io/docs/manual/data-types/streams/>`__

   Transactions
     Redis Transactions allow the execution of multiple commands as a single
     isolated operation. For more details refer to the
     `Redis manual entry on transactions <https://redis.io/docs/manual/transactions/>`_.
