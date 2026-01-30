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
     see the `Redis docs on pipelining <https://redis.io/docs/develop/using-commands/pipelining/>`_.

   Pub/Sub
     Publish/Subscribe messaging paradigm where message senders (publishers) send messages
     to a channel without knowledge of the recipients of the message. Receipients subscribe
     to channels and asynchronously consume messages that they are interested in.
     For more details refer to the `Redis docs on Pub/Sub <https://redis.io/docs/develop/pubsub/>`__

   Redis
    `Redis <https://redis.io//>`__ is an open source, in-memory data
    structure store used as a database, cache, message broker, and streaming engine.
    Redis provides data structures such as strings, hashes, lists, sets, sorted
    sets with range queries, bitmaps hyperloglogs, geospatial indexes, and streams.

   Redis Cluster
     Redis scales horizontally with a deployment topology called Redis Cluster.
     To learn more please read `Redis Cluster 101 <https://redis.io/docs/operate/oss_and_stack/management/scaling/#redis-cluster-101>`__

   Redis Modules
     Redis modules make it possible to extend Redis functionality using external modules
     that implement new Redis commands with features similar to what can be done inside
     the core itself.

   RESP
     Redis clients use Redis serialization protocol (RESP) specification to communicate
     with the Redis server. For more detials see `RESP protocol spec <https://redis.io/docs/develop/reference/protocol-spec/>`__

   RESP3
     RESP3 extends :term:`RESP` to include support for primitives such as double & boolean
     and for container structures such as maps & sets. For more details see the
     `RESP3 Specification <https://github.com/antirez/RESP3/blob/master/spec.md>`__

   Sharded Pub/Sub
     Support for Sharded Pub/Sub is available as of :redis-version:`7.0.0` and refers
     to routing messages to cluster nodes by applying the same alogrithm used to distribute
     keys to distribute channels. For more details see the `Redis docs on
     Sharded Pub/Sub <https://redis.io/docs/develop/pubsub/#sharded-pubsub>`__

   Server assisted client side caching
     Client side caching is a technique to increase performance by eliminating unnecessary
     round trips to database servers by caching responses from the server within the application's
     memory. This invariably requires some kind of invalidation technique to remove stale entries
     from the application's memory. Server assisted client side caching uses subscriptions to the
     notifications from the redis server whenever a key is mutated to invalidate local cached entries.
     For more details see the `Redis docs on client side caching <https://redis.io/docs/develop/clients/client-side-caching/>`__

   Streams
     Streams are essentially an abstract append-only in-memory log datastructure
     which can be used for various use cases such as timeseries,
     as queue for a Pub/Sub application architecture. For more details see the
     `Redis docs on streams <https://redis.io/docs/develop/data-types/streams/>`__

   Transactions
     Redis Transactions allow the execution of multiple commands as a single
     isolated operation. For more details refer to the
     `Redis docs on transactions <https://redis.io/docs/develop/using-commands/transactions/>`_.
