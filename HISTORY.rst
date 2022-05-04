.. _aredis: https://github.com/NoneGG/aredis

Changelog
=========

v3.4.7
------
Release Date: 2022-05-04

* Chores

  * Update CI to use official 7.0 release for redis
  * Update CI to use 7.0.0-RC4 image for redis-stack

* Bug Fix

  * Fix key spec extraction for commands using kw search

v3.4.6
------
Release Date: 2022-04-30

* Bug Fixes

  * Ensure protocol_version is captured for constructions with from_url
  * Fix command name for module_loadex method


v3.4.5
------
Release Date: 2022-04-22

* Chore

  * Fix incorrect type annotations for primitive callbacks
  * Update test matrix in CI with python 3.11 a7
  * Update documentation to provide a slightly more detailed
    background around the project diversion

* Experiments

  * Add basic support for KeyDB

v3.4.4
------
Release Date: 2022-04-21

* Chore

  * Fix github release workflow

v3.4.3
------
Release Date: 2022-04-21

* Chore

  * Fix github release workflow

v3.4.2
------
Release Date: 2022-04-21

* Bug fix

  * Fix error selecting database when ``decode_responses`` is ``True``
    (`Issue 46 <https://github.com/alisaifee/coredis/issues/46>`_)

v3.4.1
------
Release Date: 2022-04-12

* Chores

  * Remove unmaintained examples & benchmarks
  * Simplify setup/package info with respect to stubs
  * Cleanup documentation landing page

v3.4.0
------
Release Date: 2022-04-11

* Features

  * Updates for breaking changes with ``function_load`` in redis 7.0 rc3
  * Add ``module_loadex`` method

* Bug fix

  * Fix installation error when building from source

v3.3.0
------
Release Date: 2022-04-04

* Features

  * Add explicit key extraction based on key spec for cluster clients

v3.2.0
------
Release Date: 2022-04-02

* Features

  * New APIs:

    * Server:

      * ``Redis.latency_histogram``
      * ``Redis.module_list``
      * ``Redis.module_load``
      * ``Redis.module_unload``

    * Connection:

      * ``Redis.client_no_evict``

    * Cluster:

      * ``Redis.cluster_shards``
      * ``Redis.readonly``
      * ``Redis.readwrite``

  * Micro optimization to use bytestrings for all hardcoded tokens
  * Add type hints for pipeline classes
  * Remove hardcoded pipeline blocked commands

* Bug Fix

  * Disable version checking by default
  * Fix incorrect key names for server commands

* Chores

  * Move publishing steps to CI
  * More typing related cleanups
  * Refactor parsers into a separate module
  * Improve test coverage to cover non decoding clients

v3.1.1
------
Release Date: 2022-03-24

* Bug Fix

  * Fix extracting version/protocol with binary clients

* Features

  * New APIs:

    * ``Redis.cluster_addslotsrange``
    * ``Redis.cluster_delslotsrange``
    * ``Redis.cluster_links``
    * ``Redis.cluster_myid``

v3.1.0
------
Release Date: 2022-03-23

* Features

  * Added support for functions
  * Added runtime checks to bail out early if server version doesn't support the command
  * Deprecate custom cluster methods
  * Issue warning when a deprecated redis command is used
  * Add support for ``RESP3`` protocol

* New APIs:

  * Scripting:

    * ``Redis.fcall``
    * ``Redis.fcall_ro``
    * ``Redis.function_delete``
    * ``Redis.function_dump``
    * ``Redis.function_flush``
    * ``Redis.function_kill``
    * ``Redis.function_list``
    * ``Redis.function_load``
    * ``Redis.function_restore``
    * ``Redis.function_stats``

  * Server:

    * ``Redis.command_docs``
    * ``Redis.command_getkeysandflags``
    * ``Redis.command_list``


v3.0.3
------
Release Date: 2022-03-21

* Bug Fix

  * Fix autoselection of hiredis when available

v3.0.2
------
Release Date: 2022-03-21

* Bug Fix

  * Fix incorrect response type for :meth:`coredis.Redis.exists` (:issue:`24`)

v3.0.1
------
Release Date: 2022-03-21

* Bug Fix

  * Ensure all submodules are included in package (:issue:`23`)
  * Fix conversation of datetime object to pxat value for set command

* Chores

  * Re-add examples folder
  * Tweak type hints
  * Make ``scan_iter`` arguments consistent with ``scan``

v3.0.0
---------
Release Date: 2022-03-20

* Features:

  * Added type hints to all redis commands
  * Added support for experimental runtime type checking
  * Updated APIs upto redis 6.2.0
  * Added experimental features for redis 7.0.0

* New APIs:

  * Generic:

    * ``Redis.copy``
    * ``Redis.migrate``

  * String:

    * ``Redis.lcs``

  * List:

    * ``Redis.blmpop``
    * ``Redis.lmpop``

  * Set:

    * ``Redis.sintercard``

  * Sorted-Set:

    * ``Redis.bzmpop``
    * ``Redis.zintercard``
    * ``Redis.zmpop``

  * Scripting:

    * ``Redis.eval_ro``
    * ``Redis.evalsha_ro``
    * ``Redis.script_debug``

  * Stream:

    * ``Redis.xautoclaim``
    * ``Redis.xgroup_createconsumer``
    * ``Redis.xgroup_delconsumer``
    * ``Redis.xgroup_setid``

  * Server:

    * ``Redis.acl_cat``
    * ``Redis.acl_deluser``
    * ``Redis.acl_dryrun``
    * ``Redis.acl_genpass``
    * ``Redis.acl_getuser``
    * ``Redis.acl_list``
    * ``Redis.acl_load``
    * ``Redis.acl_log``
    * ``Redis.acl_save``
    * ``Redis.acl_setuser``
    * ``Redis.acl_users``
    * ``Redis.acl_whoami``
    * ``Redis.command``
    * ``Redis.command_count``
    * ``Redis.command_getkeys``
    * ``Redis.command_info``
    * ``Redis.failover``
    * ``Redis.latency_doctor``
    * ``Redis.latency_graph``
    * ``Redis.latency_history``
    * ``Redis.latency_latest``
    * ``Redis.latency_reset``
    * ``Redis.memory_doctor``
    * ``Redis.memory_malloc_stats``
    * ``Redis.memory_purge``
    * ``Redis.memory_stats``
    * ``Redis.memory_usage``
    * ``Redis.replicaof``
    * ``Redis.swapdb``

  * Connection:

    * ``Redis.auth``
    * ``Redis.client_caching``
    * ``Redis.client_getredir``
    * ``Redis.client_id``
    * ``Redis.client_info``
    * ``Redis.client_reply``
    * ``Redis.client_tracking``
    * ``Redis.client_trackinginfo``
    * ``Redis.client_unblock``
    * ``Redis.client_unpause``
    * ``Redis.hello``
    * ``Redis.reset``
    * ``Redis.select``

  * Cluster:

    * ``Redis.asking``
    * ``Redis.cluster_bumpepoch``
    * ``Redis.cluster_flushslots``
    * ``Redis.cluster_getkeysinslot``


* Breaking changes:

  * Most redis command API arguments and return types have been
    refactored to be in sync with the official docs.

  * Updated all commands accepting multiple values for an argument
    to use positional var args **only** if the argument is optional.
    For all other cases, use a positional argument accepting an
    ``Iterable``. Affected methods:

    * ``bitop`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``delete`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``exists`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``touch`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``unlink`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``blpop`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``brpop`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``lpush`` -> ``*elements`` -> ``elements: Iterable[ValueT]``
    * ``lpushx`` -> ``*elements`` -> ``elements: Iterable[ValueT]``
    * ``rpush`` -> ``*elements`` -> ``elements: Iterable[ValueT]``
    * ``rpushx`` -> ``*elements`` -> ``elements: Iterable[ValueT]``
    * ``mget`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``sadd`` -> ``*members`` -> ``members: Iterable[ValueT]``
    * ``sdiff`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``sdiffstore`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``sinter`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``sinterstore`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``smismember`` -> ``*members`` -> ``members: Iterable[ValueT]``
    * ``srem`` -> ``*members` -> ``members: Iterable[ValueT]``
    * ``sunion`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``sunionstore`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``geohash`` -> ``*members`` -> ``members: Iterable[ValueT]``
    * ``hdel`` -> ``*fields`` -> ``fields: Iterable[ValueT]``
    * ``hmet`` -> ``*fields`` -> ``fields: Iterable[ValueT]``
    * ``pfcount`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``pfmerge`` -> ``*sourcekeys`` -> ``sourcekeys: Iterable[KeyT]``
    * ``zdiff`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``zdiffstore`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``zinter`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``zinterstore`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``zmscore`` -> ``*members`` -> ``members: Iterable[ValueT]``
    * ``zrem`` -> ``*members`` -> ``members: Iterable[ValueT]``
    * ``zunion`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``zunionstore`` -> ``*keys`` -> ``keys: Iterable[KeyT]``
    * ``xack`` -> ``*identifiers`` -> ``identifiers: Iterable[ValueT]``
    * ``xdel`` -> ``*identifiers`` -> ``identifiers: Iterable[ValueT]``
    * ``xclaim`` -> ``*identifiers`` -> ``identifiers: Iterable[ValueT]``
    * ``script_exists`` -> ``*sha1s`` - > ``sha1s: Iterable[ValueT]``
    * ``client_tracking`` -> ``*prefixes`` - > ``prefixes: Iterable[ValueT]``
    * ``info`` -> ``*sections`` - > ``sections: Iterable[ValueT]``

v2.3.1
------
Release Date: 2022-01-30

* Chore:

  * Standardize doc themes
  * Boo doc themes

v2.3.0
------
Release Date: 2022-01-23

Final release maintaining backward compatibility with `aredis`_

* Chore:

  * Add test coverage for uvloop
  * Add test coverage for hiredis
  * Extract tests to use docker-compose
  * Add tests for basic authentication


v2.2.3
------
Release Date: 2022-01-22

* Bug fix:

  * Fix stalled connection when only username is provided

v2.2.2
------
Release Date: 2022-01-22

* Bug fix:

  * Fix failure to authenticate when just using password

v2.2.1
------
Release Date: 2022-01-21


This release brings in pending pull requests from
the original `aredis`_ repository and updates the signatures
of all implemented methods to be synchronized (as much as possible)
with the official redis documentation.

* Feature (extracted from pull requests in `aredis`_):
  * Add option to provide ``client_name``
  * Add support for username/password authentication
  * Add BlockingConnectionPool

v2.1.0
------
Release Date: 2022-01-15

This release attempts to update missing command
coverage for common datastructures and gets closer
to :pypi:`redis-py` version ``4.1.0``

* Feature:

  * Added string commands ``decrby``, ``getdel`` & ``getex``
  * Added list commands ``lmove``, ``blmove`` & ``lpos``
  * Added set command ``smismember``
  * Added sorted set commands ``zdiff``, ``zdiffstore``, ``zinter``, ``zmscore``,
      ``zpopmin``, ``zpopmax``, ``bzpopmin``, ``bzpopmax`` & ``zrandmember``
  * Added geo commands ``geosearch``, ``geosearchstore``
  * Added hash command ``hrandfield``
  * Added support for object inspection commands ``object_encoding``, ``object_freq``, ``object_idletime`` & ``object_refcount``
  * Added ``lolwut``

* Chore:
  * Standardize linting against black
  * Add API documentation
  * Add compatibility documentation
  * Add CI coverage for redis 6.0


v2.0.1
------
Release Date: 2022-01-15

* Bug Fix:

  * Ensure installation succeeds without gcc


v2.0.0
------
Release Date: 2022-01-05

* Initial import from `aredis`_
* Add support for python 3.10

------

Imported from fork
------------------

The changelog below is imported from `aredis`_


------
v1.1.8
------
* Fixbug: connection is disconnected before idel check, valueError will be raised if a connection(not exist) is removed from connection list
* Fixbug: abstract compat.py to handle import problem of asyncio.future
* Fixbug: When cancelling a task, CancelledError exception is not propagated to client
* Fixbug: XREAD command should accept 0 as a block argument
* Fixbug: In redis cluster mode, XREAD command does not function properly
* Fixbug: slave connection params when there are no slaves

------
v1.1.7
------
* Fixbug: ModuleNotFoundError raised when install aredis 1.1.6 with Python3.6

------
v1.1.6
------
* Fixbug: parsing stream messgae with empty payload will cause error(#116)
* Fixbug: Let ClusterConnectionPool handle skip_full_coverage_check (#118)
* New: threading local issue in coroutine, use contextvars instead of threading local in case of the safety of thread local mechanism being broken by coroutine (#120)
* New: support Python 3.8

------
v1.1.5
------
* new: Dev conn pool max idle time (#111) release connection if max-idle-time exceeded
* update: discard travis-CI
* Fix bug: new stream id used for test_streams

------
v1.1.4
------
* fix bug: fix cluster port parsing for redis 4+(node info)
* fix bug: wrong parse method of scan_iter in cluster mode
* fix bug: When using "zrange" with "desc=True" parameter, it returns a coroutine without "await"
* fix bug: do not use stream_timeout in the PubSubWorkerThread
* opt: add socket_keepalive options
* new: add ssl param in get_redis_link to support ssl mode
* new: add ssl_context to StrictRedis constructor and make it higher priority than ssl parameter

------
v1.1.3
------
* allow use of zadd options for zadd in sorted sets
* fix bug: use inspect.isawaitable instead of typing.Awaitable to judge if an object is awaitable
* fix bug: implicitly disconnection on cancelled error (#84)
* new: add support for `streams`(including commands not officially released, see `streams <http://aredis.readthedocs.io/en/latest/streams.html>`_ )

------
v1.1.2
------
* fix bug: redis command encoding bug
* optimization: sync change on acquring lock from redis-py
* fix bug: decrement connection count on connection disconnected
* fix bug: optimize code proceed single node slots
* fix bug: initiation error of aws cluster client caused by not appropiate function list used
* fix bug: use `ssl_context` instead of ssl_keyfile,ssl_certfile,ssl_cert_reqs,ssl_ca_certs in intialization of connection_pool

------
v1.1.1
------
* fix bug: connection with unread response being released to connection pool will lead to parse error, now this kind of connection will be destructed directly. `#52 <https://github.com/NoneGG/aredis/issues/52>`_
* fix bug: remove Connection.can_read check which may lead to block in awaiting pubsub message. Connection.can_read api will be deprecated in next release. `#56 <https://github.com/NoneGG/aredis/issues/56>`_
* add c extension to speedup crc16, which will speedup cluster slot hashing
* add error handling for asyncio.futures.Cancelled error, which may cause error in response parsing.
* sync optimization of client list made by swilly22 from redis-py
* add support for distributed lock using redis cluster

------
v1.1.0
------
* sync optimization of scripting from redis-py made by `bgreenberg <https://github.com/bgreenberg-eb>`_ `redis-py#867 <https://github.com/andymccurdy/redis-py/pull/867>`_
* sync bug fixed of `geopos` from redis-py made by `categulario <https://github.com/categulario>`_ `redis-py#888 <https://github.com/andymccurdy/redis-py/pull/888>`_
* fix bug which makes pipeline callback function not executed
* fix error caused by byte decode issues in sentinel
* add basic transaction support for single node in cluster
* fix bug of get_random_connection reported by myrfy001

------
v1.0.9
------
* fix bug of pubsub, in some env AssertionError is raised because connection is used again after reader stream being fed eof
* add reponse decoding related options(`encoding` & `decode_responses`), make client easier to use
* add support for command `cluster forget`
* add support for command option `spop count`

------
v1.0.8
------
* fix initialization bug of redis cluster client
* add example to explain how to use `client reply on | off | skip`

------
v1.0.7
------
* introduce loop argument to aredis
* add support for command `cluster slots`
* add support for redis cluster

------
v1.0.6
------
* bitfield set/get/incrby/overflow supported
* new command `hstrlen` supported
* new command `unlink` supported
* new command `touch` supported

------
v1.0.5
------
* fix bug in setup.py when using pip to install aredis

------
v1.0.4
------
* add support for command `pubsub channel`, `pubsub numpat` and `pubsub numsub`
* add support for command `client pause`
* reconsitution of commands to make develop easier(which is transparent to user)

------
v1.0.2
------
* add support for cache (Cache and HerdCache class)
* fix bug of `PubSub.run_in_thread`

------
v1.0.1
------

* add scan_iter, sscan_iter, hscan_iter, zscan_iter and corresponding unit tests
* fix bug of `PubSub.run_in_thread`
* add more examples
* change `Script.register` to `Script.execute`

























