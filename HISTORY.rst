.. _aredis: https://github.com/NoneGG/aredis

Changelog
=========

v6.0.0rc2
---------
Release Date: 2026-01-30

* Feature

  * Add :meth:`~coredis.Redis.xconsumer`  factory method to
    create a single or group stream consumer
  * Add a ``verify_existence`` flag to library
    :func:`coredis.commands.function.wraps` to optionally
    skip local validation and optimistically call it.
  * Improve guarantees of calling subscription methods on PubStub
    instances. Using a ``subscription_timeout`` the acknowledgement
    of subscription to a topic or pattern can be guaranteed before
    proceeding with listening to messages.
  * Cluster pipelines now support ``Script`` instances
  * Allow chaininable ``transform`` method to accept inline
    callbacks to transform the response from the redis server
  * Added ``retry`` chainable method to the response from
    all commands to allow using retry policies individually
    with a requests instead of having to apply it on all
    requests issued by a client.
  * Retry policies now allow infinite retries
    & retry deadlines.
  * ExponentionBackoffRetryPolicy now supports
    `jitter`

* Breaking change

  * Stream consumers can no longer be awaited and must be used
    as context managers to ensure initialization
  * :meth:`coredis.Redis.pubsub`, :meth:`coredis.RedisCluster.pubsub` &
    :meth:`coredis.RedisCluster.sharded_pubsub` now only accept keyword
    arguments.

* Compatibility

  * The clients no longer check if a module is loaded at initialization.
    Therefore a module command issued against a server that doesn't have
    the module will not fail early and instead result in an
    :exc:`~coredis.exceptions.UnknownCommandError` exception by raised.
  * Add support for ``VRANGE`` vector set command (:meth:`~coredis.Redis.vrange`)
  * Add support for ``FT.HYBRID`` search command (:meth:`~coredis.modules.Search.hybrid`)

* Performance

  * Fixed performance regression introduced in ``6.0.0rc1`` with sending commands
    to the socket. Performance over concurrent workloads is now at par with 5.x or better

v6.0.0rc1
---------
Release Date: 2026-01-18

* Features

  * Migrate entire library to :pypi:`anyio`, adding structured concurrency and :pypi:`trio` support.
  * Pipelines auto-execute when exiting their context manager; results are accessible in a type-safe way.
  * ``Library.wraps`` renamed to :func:`coredis.commands.function.wraps` and now supports callbacks.
  * :class:`~coredis.commands.Script` and :class:`~coredis.commands.Function` can now be called with
    optional callbacks to transform the raw response from redis before returning it. This is supported
    by their associated ``wraps`` decorators as well.
  * :meth:`coredis.Redis.lock` & :meth:`coredis.RedisCluster.lock` added as a convenient accessors
    for the Lua-based lock: :class:`coredis.lock.Lock` (Previously found in ``coredis.recipes.LuaLock``).

* Breaking Changes

  * Almost all classes (clients, connection pools, PubSub, pipelines) now require being used with
    their async context managers for initialization/cleanup.
  * Users should replace ``TrackingCache`` with :class:`coredis.cache.LRUCache` when providing a cache
    instance to the clients. Cache size can no longer be bound by byte size and only ``max_keys`` is supported.
  * All connection pools are now blocking.
  * Pipelines

    * Pipelines no longer expose an explicit ``execute()`` method and instead auto-execute when leaving
      their context manager.
    * Removed ``__len__`` and ``__bool__`` methods of :class:`coredis.pipeline.Pipeline`
    * Drop support for explicit management of ``watch``, ``unwatch``, ``multi`` in pipelines. This
      is replaced by the :meth:`coredis.pipeline.Pipeline.watch` async context manager.
  * When defining type stubs for FFI for Lua scripts or library functions, keys can only be distinguished
    from arguments by annotating them with :class:`coredis.typing.KeyT`.

* Removals

  * Drop support for ``RESP2``.
  * Remove ``Monitor`` wrapper.
  * Remove ``RedisGraph`` module support.

v5.6.0
------
Release Date: 2026-01-19

* Bug Fix

  * Fix a potential race condition with the Redis client
    where a non blocking command could be queued on a connection
    that is already acquired for a blocking command.

v5.5.0
------
Release Date: 2026-01-12

* Compatibility

  * Deprecate support for legacy RESP2 protocol

v5.4.0
------
Release Date: 2025-12-17

* Bug Fix

  * Fix recursion error when retrying pipeline commands
    on cluster moved errors

v5.3.0
------
Release Date: 2025-10-10

* Compatibility

  * Mark redis graph module support as deprecated
  * Fix compatibility with beartype >= 0.22
  * Add support for python 3.14

v5.2.0
------
Release Date: 2025-10-01

* Feature

  * Add support for new stream commands (``xackdel`` & ``xdelex``)
    in redis 8.2
  * Add ``vismember`` command for vector sets

* Bug Fix

  * Allow client to gracefully proceed when modules can't be
    enumerated due to ACL restrictions

* Development

  * Switch entirely to pyproject.toml for project metadata
  * Use hatch for build
  * Use uv for development environment


v5.1.0
------
Release Date: 2025-09-10

* Bug Fix

  * Ensure ``ssl_context`` passed in kwargs of ``from_url`` factory
    method is respected.

v5.0.1
------
Release Date: 2025-07-18

* Bug Fix

  * Fix regression caused by ``5.0.0`` which completely broke the use of Sentinel
    with ``decode_responses=False``.

v5.0.0
------
Release Date: 2025-07-16

* Features

  * Add support for using custom types with redis commands
    by registering serializers and deserializers
  * Allow stacking pipeline commands synchronously
  * Expose statically typed responses for pipeline commands


* Compatibility

  * Redis command methods are no longer coroutines and instead
    synchronous methods that return subclasses of ``Awaitable``
    (``CommandRequest``) which can be awaited as before.
  * Add support for redis 8.0 vector set commands
  * Add support for redis 8.0 hash expiry commands
  * Remove deprecated pubsub ``listen`` and threaded worker APIs
  * Remove support for KeyDB

* Performance

  * Streamline client side cache shrinking

v5.0.0rc2
---------
Release Date: 2025-07-10

* Bug Fix

  * Fix duplicate command error in using ``transform`` with pipeline

v5.0.0rc1
---------
Release Date: 2025-07-07

* Features

  * Add support for using custom types with redis commands
    by registering serializers and deserializers
  * Allow stacking pipeline commands syncronously
  * Expose statically types responses for pipeline commands


* Compatibility

  * Add support for redis 8.0 vector set commands
  * Add support for redis 8.0 hash expiry commands
  * Remove deprecated pubsub ``listen`` and threaded worker APIs
  * Remove support for KeyDB

* Performance

  * Streamline client side cache shrinking

v4.24.0
-------
Release Date: 2025-07-05

* Bug Fix

  * Add support for using library functions with pipelines

v4.23.1
-------
Release Date: 2025-06-20

* Bug Fix

  * Ensure pubsub consumer task cleanly exists when the pubsub instance
    has been shutdown with `close` or `aclose`

v4.23.0
-------
Release Date: 2025-06-19

* Bug Fix

  * Fix busy loop in pubsub consumer when it isn't subscribed
    to any channel or pattern.

v4.22.0
-------
Release Date: 2025-05-06

* Bug Fix

  * Disable incompatible ``scan`` method for redis cluster
  * Fix use of ``scan_iter`` with redis cluster with uninitialized client

v4.21.0
-------
Release Date: 2025-05-02

* Features

  * Improve API for pubsub instances to:

    * Be used as Async context manager for automatic cleanup
    * Be iterated on with async for to consume messages
    * Be awaited to ensure initialization
    * subscribe on instantiation

* Bug Fix

  * Correct type narrowing when constructing client with
    ``from_url`` class method

* Compatibility

  * Drop support for python 3.9
  * Remove tests for RESP2

* Deprecations

  * Deprecate ``run_in_thread`` APIs for PubSub & Monitor
  * Deprecate ``listen`` methods for PubSub instances


v4.20.0
-------
Release Date: 2025-03-05

* Bug Fix

  * Ensure ssl context is correctly setup with the
    ``check_hostname`` & ``cert_reqs`` even when a keyfile / cert
    are not provided. This allows clients to connect to a redis
    instance that has ``tls-auth-client=no`` by simply setting
    ``ssl_cert_reqs=none``

v4.19.0
-------
Release Date: 2025-03-02

* Features

* Add ``novalues`` argument to ``hscan``

* Bug Fix

  * Improve handling of ssl parameters (especially from query string)
    when dealing with self signed certificates.

v4.18.0
-------
Release Date: 2024-12-10

* Features

  * Added ability to use credential providers to supply
    authentication details when establishing connections
    to the server
  * Added support for hash field expiry commands introduced
    in redis 7.4:
    * ``hexpire``
    * ``hexpireat``
    * ``hpexpire``
    * ``hpexpireat``
    * ``hpersist``
    * ``hexpiretime``
    * ``hpexpiretime``
    * ``httl``
  * Added support for ``maxage`` parameter in ``client_kill`` method.

* Compatibility

  * Added support for python 3.13 wheels
  * Dropped support for python 3.8
  * Dropped support for pypy 3.8
  * Added test coverage for pypy 3.10
  * Added test coverage for redis 7.4.x
  * Added test coverage against redict servers
  * Added test coverage for Valkey 7.0 & 8.0

v4.18.0rc4
----------
Release Date: 2024-12-09

* Chore

  * Fix artifact download in github release workflow

v4.18.0rc3
----------
Release Date: 2024-12-09

* Chore

  * Fix github release workflow

v4.18.0rc2
----------
Release Date: 2024-12-08

* Compatibility

  * Update redict docker images to use alpine

v4.18.0rc1
----------
Release Date: 2024-12-07

* Features

  * Added ability to use credential providers to supply
    authentication details when establishing connections
    to the server
  * Added support for hash field expiry commands introduced
    in redis 7.4:
    * ``hexpire``
    * ``hexpireat``
    * ``hpexpire``
    * ``hpexpireat``
    * ``hpersist``
    * ``hexpiretime``
    * ``hpexpiretime``
    * ``httl``
  * Added support for ``maxage`` parameter in ``client_kill`` method.

* Compatibility

  * Added support for python 3.13 wheels
  * Dropped support for python 3.8
  * Dropped support for pypy 3.8
  * Added test coverage for pypy 3.10
  * Added test coverage for redis 7.4.x
  * Added test coverage against redict servers
  * Added test coverage for Valkey 7.0 & 8.0

v4.17.0
-------
Release Date: 2024-04-19

Features

* Add explicit exception types for locking recipe

Bug fix

* Fix incorrect use of `CLIENT SET-INFO` for `lib-name` & `lib-ver`
  when redis version < 7.2
* Fix various incorrect type annotations in return types

Compatibility

* Update documentation dependencies
* Update test dependencies (related to pytest)

v4.16.0
-------
Release Date: 2023-08-30

* Bug fix

  * Fix intermittent errors due to mismatched responses when multiple
    couroutines access a new connection pool.

* Compatibility

  * Remove support for python 3.7
  * Remove Redis 6.0 from CI
  * Disable RedisGraph tests in CI as the module is now not part of Redis Stack
  * Fix RESP3 compatibility for RedisSearch
  * Mark `json.resp` as deprecated

v4.16.0rc1
----------
Release Date: 2023-08-11

* Bug fix

  * Fix intermittent errors due to mismatched responses when multiple
    couroutines access a new connection pool.

v4.15.1
-------
Release Date: 2023-08-10

* Bug fix

  * Handle edge case of clearing a closed socket buffer during
    object destruction.

* Chores

  * Re-enable CI for dragonfly

v4.15.0
-------
Release Date: 2023-08-09

* Bug fix

  * Improve cleanup on socket disconnect by clearing
    internal response buffer

* Chores

  * Add typing overload for ``lpop`` method
  * Remove python 3.7 from CI due to EOL.
  * Temporarily disable pre-release python 3.12 from CI
    due to dependency resolution issues.
  * Update development dependencies



v4.14.0
-------
Release Date: 2023-05-22

* Features

  * Improve parsing of ``TS.INFO`` response for
    ``rules`` section
  * Broaden input parameter type annotations when
    expecting a mapping to use ``Mapping`` instead of
    ``Dict``

* Compatibility

  * Update parsing of timeseries module responses
    to be compatible with RESP3 responses

* Chores

  * Update CI to test against 7.2-rc2
  * Update mypy dependency

v4.13.3
-------
Release Date: 2023-05-10

* Feature

  * Add ``json.mset`` command

* Bug Fix

  * Remove caching for ``json.mget``
  * Ensure hint from moved errors are used on next attempt

v4.13.2
-------
Release Date: 2023-05-08

* Feature

  * Add ``json.merge`` command

* Bug fix

  * Fix exception message for unsupported commands

v4.13.1
-------
Release Date: 2023-05-03

* Hack

  * Downgrade sphinx back to 6.x for theme compatibility

v4.13.0
-------
Release Date: 2023-05-03

* Bug Fixes

  * Fix incorrect explicit command execution on cluster
    pipeline retries
  * Fix inconsistent reset api signatures for pipeline
    across standalone & cluster clients
  * Ensure pipeline requests respect explicit timeout
  * Fix incomplete xinfo_streams response for groups/consumers
    details


v4.12.4
-------
Release Date: 2023-04-29

* Chores

  * Expand coverage of modules tests to include RESP2
  * Allow failures for "next" versions of redis in CI

v4.12.3
-------
Release Date: 2023-04-27

* Feature

  * Add pure python wheel to release process

* Chores

  * Improve redis module documentation
  * Update CI to use python 3.11 as default
  * Fix README formatting

v4.12.2
-------
Release Date: 2023-04-22

* Chore

  * Clean up changelog entries

v4.12.1
-------
Release Date: 2023-04-22

* Bug Fix

  * Ensure task cancellation results in proper cleanup
    of a connection that might be blocked

v4.12.0
-------
Release Date: 2023-04-21

* Features

  * Add support for RedisBloom module
  * Add support for ReJSON module
  * Add support for RedisSearch module
  * Add support for RedisTimeSeries module
  * Add support for RedisGraph module
  * Check argument versions for compatibility and
    raise appropriate errors if an argument is used
    on an older server version which doesn't support it.
  * Expose :paramref:`~coredis.Redis.retry_policy` to client constructors
  * Expose :paramref:`~coredis.Redis.noevict` in client constructors
  * Add initial support for redis 7.2

    * Expose :paramref:`~coredis.Redis.notouch` in client constructors
    * Add support for :meth:`~coredis.Redis.client_no_touch`
    * Add support for :meth:`~coredis.Redis.client_setinfo`
    * Add support for :meth:`~coredis.Redis.waitaof`
    * Add new ``withscore`` argument for :meth:`~coredis.Redis.zrank` & :meth:`~coredis.Redis.zrevrank`
    * Add new context manager :meth:`~coredis.Redis.ensure_persistence`
  * Allow adding streams to stream consumers after initialization
  * Improve cluster routing for commands that act on multiple
    slots but are handled by the same node.
  * Allow overriding the default stream_timeout
    when using a pipeline

* Bug Fix

  * Ensure multiple properties returned from info command
    are collapsed into an array
  * Fix leaked connections when using :meth:`~coredis.Redis.ensure_replication`
  * Improve handling of cancellation errors
  * Improve handling of timeout errors
  * Ensure cluster commands routed to random nodes use
    primaries by default
  * Handle pause/resume callbacks from Transport
    and pause sending subsequent commands until
    the transport buffer is resumed.
  * Handle RESP3 response for :meth:`~coredis.Redis.command`
  * Update :meth:`~coredis.ConnectionPool.from_url` &
    :meth:`~coredis.ClusterConnectionPool.from_url` to support
    all constructor arguments

v4.12.0rc1
----------
Release Date: 2023-04-19

* Features

  * Add support for RedisGraph module
  * Allow overriding the default stream_timeout
    when using a pipeline
  * Check argument versions for compatibility and
    raise appropriate errors if an argument is used
    on an older server version which doesn't support it.

* Bug Fix

  * Handle pause/resume callbacks from Transport
    and pause sending subsequent commands until
    the transport buffer is resumed.
  * Handle RESP3 response for :meth:`~coredis.Redis.command`
  * Update :meth:`~coredis.ConnectionPool.from_url` &
    :meth:`~coredis.ClusterConnectionPool.from_url` to support
    all constructor arguments

* Chores

  * Add redis-stack@edge to compatibility matrix in CI


v4.12.0b4
---------
Release Date: 2023-04-10

* Features

  * Add support for RedisSearch module
  * Allow adding streams to stream consumers after initialization

* Chores

  * Update mypy

v4.12.0b3
---------
Release Date: 2023-04-04

* Features

  * Add support for RedisBloom module
  * Add support for ReJSON module
  * Add support for RedisTimeSeries module
  * Improve cluster routing for commands that act on multiple
    slots but are handled by the same node.

* Bug Fix

  * Ensure multiple properties returned from info command
    are collapsed into an array

v4.12.0b2
---------
Release Date: 2023-03-27

* Chores

  * Handle external warnings in tests
  * Improve docstrings & annotations for ensure_persistence
  * Add reruns for test failures in CI
  * Add python 3.12 to CI

v4.12.0b1
---------
Release Date: 2023-03-26

* Features

  * Expose :paramref:`~coredis.Redis.retry_policy` to client constructors
  * Expose :paramref:`~coredis.Redis.noevict` in client constructors
  * Add initial support for redis 7.2

    * Expose :paramref:`~coredis.Redis.notouch` in client constructors
    * Add support for :meth:`~coredis.Redis.client_no_touch`
    * Add support for :meth:`~coredis.Redis.client_setinfo`
    * Add support for :meth:`~coredis.Redis.waitaof`
    * Add new ``withscore`` argument for :meth:`~coredis.Redis.zrank` & :meth:`~coredis.Redis.zrevrank`
    * Add new context manager :meth:`~coredis.Redis.ensure_persistence`


* Bug Fix

  * Fix leaked connections when using :meth:`~coredis.Redis.ensure_replication`
  * Improve handling of cancellation errors
  * Improve handling of timeout errors
  * Ensure cluster commands routed to random nodes use
    primaries by default

v4.11.6
-------
Release Date: 2023-04-22

* Bug Fix

  * Ensure task cancellation results in proper cleanup
    of a connection that might be blocked

v4.11.5
-------
Release Date: 2023-04-04

* Bug Fix

  * Ensure ``protocol_version`` is parsed as an int from url

v4.11.4
-------
Release Date: 2023-04-04

* Bug Fix

  * Ensure ``protocol_version`` is parsed as an int from url


v4.11.3
-------
Release Date: 2023-03-11

* Chores

  * Update cibuildwheels action
  * Update versioneer
  * Migrate setup.cfg to pyproject
  * Parallelize CI wheel build
  * Reintroduce ruff for linting

v4.11.2
-------
Release Date: 2023-03-09

* Chores

  * Add changelog link to pypi

v4.11.1
-------
Release Date: 2023-03-09

* Bug Fix

  * Ensure prebuilt wheels contain compiled extensions

v4.11.0
-------
Release Date: 2023-03-09

* Features

  * Add retries to pubsub subscribers
  * Generate prebuilt wheel for aarch64 + Linux

* Bug Fix

  * Use random nodes with cluster pubsub
  * Trigger a refresh of cluster topology on connection errors
  * Raise ConnectionError on timeout errors when establishing a connection


* Chores

  * Update pyright

v4.10.3
-------
Release Date: 2023-03-08

* Bug Fix

  * Ensure extension compilation goes through without
    beartype available

* Chores

  * Update dependencies
  * Update github actions
  * Use ruff for linting

v4.10.2
-------
Release Date: 2022-12-24

* Chores

  * Update test certificates
  * Update development dependencies

v4.10.1
-------
Release Date: 2022-12-11

* Compatibility

  * Upgrade documentation dependencies
  * Relax version contraint for packaging dependency

v4.10.0
-------
Release Date: 2022-11-21

* Feature

  * Allow using async functions as callbacks for pubsub message
    handlers

v4.9.0
------
Release Date: 2022-11-09

* Feature

  * Update implementation of transactional pipeline and the
    behavior of the ``transaction`` method exposed by they cluster
    client to be consistent with the standalone client.

* Breaking changes

  * Pipeline instances passed into the callable ``func`` parameter
    of the cluster ``transaction`` method will no longer automatically
    queue commands until a call to ``multi`` is issued to be consistent
    with the implementation in the standalone client.

v4.8.3
------
Release Date: 2022-11-04

* Bug Fix

  * Ensure pipeline commands are written to the socket in one
    shot

* Chore

  * Reduce package size by removing test folder
  * Add a post wheel packaging import test

v4.8.2
------
Release Date: 2022-10-31

* Bug Fix

  * Fix wheels for macos

v4.8.1
------
Release Date: 2022-10-29

* Feature

  * Extend ``decoding`` context manager to selecting codec overrides

v4.8.0
------
Release Date: 2022-10-28

* Feature

  * Add a ``decoding`` context manager to control decoding behavior

* Performance

  * Remove validation code paths at decoration time in optimized mode

v4.7.1
------
Release Date: 2022-10-31

* Bug Fix

  * Fix wheels for macos

v4.7.0
------
Release Date: 2022-10-26

* Feature

  * Add optimized mode to allow skipping validation code paths
  * Add ``lastid`` parameter to ``xclaim`` method

* Bug Fix

  * Ensure ``LuaLock`` context manager throws an exception when a
    lock cannot be acquired

* Compatibility

  * Add final python 3.11 wheels

v4.6.0
------
Release Date: 2022-10-10

* Feature

  * Implement early release back to connection pool to allow
    multiple concurrent commands to use the some connection
    thus significantly reducing the need to expand the connection
    pool when using blocking connection pools

* Bug Fix

  * Add a lock when initializing the cluster client to ensure
    concurrent "first access" does not result in corruption of the
    cluster node layout or a thundering herd to initialize the layout

* Compatibility

  * Enable wheel build for python 3.11

* Chores

  * Improve stability of test suite
  * Enable recursive response types for mypy & pyright

v4.5.6
------
Release Date: 2022-08-31

* Bug Fix

  * Remove duplicated initialization calls to connection pool & cache

v4.5.5
------
Release Date: 2022-08-22

* Compatibility

  * Add test coverage for PyPy version 3.7 & 3.9

* Bug Fix

  * Ensure methods expecting iterables for an argument raise a TypeError
    when a single string or byte sequence is used incorrectly.

v4.5.4
------
Release Date: 2022-08-08

* Bug Fix

  * Fix leftover default connection pool construction in
    blocking cluster connection pool

* Chores

  * Reduce excessive matrix in default CI
  * Add scheduled compatibility CI run
  * Cleanup unnecessary asyncio markers in tests
  * Refactor readonly command detection to use
    command flags from redis documentation
  * Issue warning if :meth:`Redis.select` is
    called directly

v4.5.3
------
Release Date: 2022-08-03

* Bug Fix

  * Ensure default cluster connection pools are not recreated
    upon access. (`Issue 92 <https://github.com/alisaifee/coredis/issues/92>`_)

v4.5.2
------
Release Date: 2022-08-03

* Bug Fix

  * Implicitly initialize cluster connection pool when
    pubsub subscribe is called
  * Fix handling of sharded pubsub unsubscribe message
  * Fix unsubscribe all for sharded pubsub

* Compatibility

  * Improve surfacing underlying errors when initializing
    cluster

v4.5.1
------
Release Date: 2022-08-02

* Bug Fix

  * Fix context leak when commands issued
    within ensure_replication and ignore_replies
    context managers fail

* Recipes

  * Fix LUA lock recipe to work with
    clusters with no replicas.
  * Ensure LUA lock recipe waits on replication
    of lock to n/2 replicas if replicas exist in
    the cluster

v4.5.0
------
Release Date: 2022-07-30

* Compatibility

  * Bring back python 3.7 support


v4.4.0
------
Release Date: 2022-07-26

* Breaking changes

  * Default `nodemanager_follow_cluster` to True
* Deprecations

  * Deprecate `readonly` constructor argument in
    cluster client in favor of `read_from_replicas`

  * Remove invalid property setter for noreply mode

* Bug Fixes

  * Fix incorrect behavior of ignore_replies context manager
    as it was not actually setting CLIENT REPLY and simply
    discarding connections
  * Ensure fetching a random connection doesn't deplete the
    node list in the connection pool
  * Ensure connection pools are disconnected on finalization
    to avoid leaking connections

v4.3.1
------
Release Date: 2022-07-23

* Bug Fix

  * Fix incorrect calculation of per node connection pool size
    when readonly=False
  * Ensure max_connection is atleast equal to the number of nodes
    in the cluster and raise a warning when it is not

v4.3.0
------
Release Date: 2022-07-22

* Features

  * Introduced :class:`coredis.pool.BlockingClusterConnectionPool`
  * Allow passing :paramref:`~coredis.Redis.connection_pool_cls`
    and :paramref:`~coredis.RedisCluster.connection_pool_cls` to pick
    the connection pool implementation during client construction

* Breaking Changes

  * :class:`~coredis.RedisCluster` now raises a :exc:`~coredis.exceptions.ConnectionError`
    when a connection can't be acquired due to ``max_connections`` being hit.

v4.2.1
------
Release Date: 2022-07-21

* Compatibility

  * Add support and test coverage for PyPy 3.8.

* Bug Fix

  * Ensure :meth:`coredis.RedisCluster.ensure_replication` can be used
    with :paramref:`~coredis.RedisCluster.ensure_replication.replicas` <
    total number of replicas

v4.2.0
------
Release Date: 2022-07-20

* Bug Fix

  * Fix routing of :meth:`coredis.Redis.script_kill` and
    :meth:`coredis.Redis.function_kill` to only route to primaries
  * Ensure all arguments expecting collections consistently
    use :data:`coredis.typing.Parameters`

* Chores

  * Fix ordering of keyword arguments of :meth:`coredis.Redis.set`
    to be consistent with command documentation
  * Improve documentation regarding request routing and repsonse
    merging for cluster multi node and multi shard commands
  * Sort all literal annotations

v4.1.1
------
Release Date: 2022-07-18

* Bug Fix

  * Ensure lua scripts for lock recipe are included in package

v4.1.0
------
Release Date: 2022-07-18

* Features

  * Reintroduce distributed lock implementation under
    `coredis.recipes.locks`

* Bug Fix

  * Allow initializing a LUA library without loading the code
    when it already exists if replace=False

* Performance

  * Reduce unnecessary calls to parser by using an async Event
    to signal that data is available for parsing

v4.0.2
------
Release Date: 2022-07-16

* Compatibility

  * Relax version checking to only warn if a server reports
    a non standard server version (for example with Redis-like
    databases)
  * Raise an exception when client tracking is not available
    and server assisted caching cannot be used (for example
    with upstash provisioned redis instances)

* Documentation

  * Add more detail about Sharded Pub/Sub

v4.0.1
------
Release Date: 2022-07-16

* Documentation

  * Added section about reliability in handbook
  * Improved cross referencing

v4.0.0
------
Release Date: 2022-07-15

* Features

  * Added support for using ``noreply`` when sending commands (see :ref:`handbook/noreply:no reply mode`)
  * Added support for ensuring replication to ``n`` replicas using :meth:`~coredis.Redis.ensure_replication`.
  * Moved :class:`~coredis.KeyDB` client out of experimental namespace

* Backward incompatible changes

  * Use RESP3 as default protocol version (see :ref:`handbook/response:redis response`)
  * :paramref:`~coredis.RedisCluster.non_atomic_cross_slot` is default behavior for cluster clients
  * Moved exceptions out of root namespace to ``coredis.exceptions``
  * Removed Lock implementations
  * Dropped support for hiredis (see :ref:`history:parsers`)
  * Removed ``StrictRedis`` & ``StrictRedisCluster`` aliases


v3.11.5
-------
Release Date: 2022-07-13

* Chore

  * Remove python 3.11 binary wheel builds

v3.11.4
-------
Release Date: 2022-07-09

* Bug Fix

  * Fix issue with sharded pubsub not handling multiple channel
    subscriptions

v3.11.3
-------
Release Date: 2022-07-07

* Bug Fix

  * Correct implementation of restore command when
    absttl argument is True.

v3.11.2
-------
Release Date: 2022-06-30

* Bug Fix

  * Ignore case when comparing error strings to map to
    exceptions

v3.11.1
-------
Release Date: 2022-06-29

* Bug Fix

  * Fix incorrect handling of :paramref:`~coredis.RedisCluster.non_atomic_cross_slot`
    commands when not all nodes are required for a command

v3.11.0
-------
Release Date: 2022-06-25

* Features

  * Added :paramref:`coredis.Redis.noreply` and :paramref:`coredis.RedisCluster.noreply` option
    to Redis & RedisCluster constructors to allow using the client without waiting for response from the
    server
  * Build wheels for all architectures supported by cibuildwheel


* Deprecations / Removals

  * Remove deprecated sentinel methods
  * Add warnings for :meth:`~coredis.Redis.client_setname`, :meth:`~coredis.Redis.client_reply`
    and :meth:`~coredis.Redis.auth` commands

* Bug Fixes

  * Fix missing :data:`protocol_version` in cluster pipeline code paths

v3.10.1
-------
Release Date: 2022-06-18

* Chores

  * Documentation tweaks

v3.10.0
-------
Release Date: 2022-06-18

* Features

  * Expose ssl parameters in :class:`coredis.RedisCluster` constructor
  * Expose :paramref:`~coredis.Redis.ssl_check_hostname` parameter in Redis/RedisCluster constructors
  * Separate opt-in cache behaviors into protocols leaving :class:`~coredis.cache.AbstractCache`
    as the minimal implementation required
  * Expose cache stats through the :data:`~coredis.cache.TrackingCache.stats` property, returning
    a :class:`~coredis.cache.CacheStats` dataclass.
  * Allow :paramref:`~coredis.cache.TrackingCache.dynamic_confidence` to increase cache confidence up to
    100% instead of capping it at the original :paramref:`~coredis.cache.TrackingCache.confidence` value provided

* Chores

  * Improve documentation for caching
  * Improve test coverage for ssl connections
  * Add test coverage for cluster ssl clients


v3.9.3
------
Release Date: 2022-06-15

* Features

  * Expose :paramref:`~coredis.sentinel.Sentinel.cache` parameter to Sentinel managed clients

* Bug Fix

  * Handle error parsing command not found exception

v3.9.2
------
Release Date: 2022-06-14

* Features

  * Add option to define confidence in cached entries

v3.9.1
------
Release Date: 2022-06-13

* Features

  * Extend coverage of cachable commands
  * Expose option to share TrackingCache between client

v3.9
----
Release Date: 2022-06-12

* Features

  * Add support for client side caching (:ref:`handbook/caching:caching`)

v3.8.12
-------
Release Date: 2022-06-10

* Features

  * Add support for sharded pubsub for redis 7.0.1 (:ref:`handbook/pubsub:cluster pub/sub`)
  * Expose :paramref:`~coredis.Redis.from_url.verify_version` parameter to :meth:`coredis.Redis.from_url`
    factory function

* Experiments

  * Extend CI coverage for keydb & dragonfly

v3.8.11
-------
Release Date: 2022-06-07

* Bug Fixes

  * Fix support for HELLO SETNAME
  * Fix routing of ACL SAVE in cluster mode

* Chores

  * Improved test coverage for server commands

v3.8.10
-------
Release Date: 2022-06-07

* Features

  * New ``nodenames`` parameter added to sentinel_info_cache

* Chores

  * Added redis 7.0 to sentinel test coverage matrix

v3.8.9
------
Release Date: 2022-06-05

* Bug Fix

  * Fix type annotation for hmget

* Experiments

  * Add CI coverage for dragonflydb


v3.8.7
------
Release Date: 2022-06-04

* Features

  * Add support for python 3.11 (b3) builds

* Performance

  * Extract python parser and optionally compile it to native
    code using mypyc

* Bug Fixes

  * Only route PING commands to primaries in cluster mode
  * Ensure connection errors for commands routed to multiple nodes
    are retried in case of cluster reconfiguration
  * Ensure re population of startup nodes is based off latest response
    from cluster


v3.8.6
------
Release Date: 2022-05-26

* Performance

  * Inline buffering of responses in python parser

v3.8.5
------
Release Date: 2022-05-25

* Features

  * Refactor python parser to remove recursion
  * Reduce number of async calls during response parsing
  * Extract command packer and use mypyc to compile it to native code


v3.8.0
------
Release Date: 2022-05-21

* Chores

  * Documentation reorg
  * Improved RESP error <-> exception mapping

* Bug fix

  * Ignore duplicate consumer group error due to groupconsumer
    initialization race condition

v3.7.57 ("Puffles")
-------------------
Release Date: 2022-05-19

* Features

  * Stream consumer clients (:ref:`handbook/streams:simple consumer` and :ref:`handbook/streams:group consumer`)

* Experiments

  * Updated :class:`~coredis.experimental.KeyDB` command coverage
  * :class:`~coredis.experimental.KeyDBCluster` client

v3.6.0
------
Release Date: 2022-05-15

* Features

  * Add option to enable non atomic splitting of commands in cluster
    mode when the commands only deal with keys (delete, exists, touch, unlink)
    (:paramref:`~coredis.RedisCluster.non_atomic_crossslot`)
  * Add support for sharded pub sub in cluster mode (:meth:`~coredis.RedisCluster.sharded_pubsub`)
  * Add support for readonly execution of LUA scripts and redis functions

* Bug Fix

  * Ensure :meth:`~coredis.RedisCluster.script_load` is routed to all nodes in cluster mode
  * Ensure :meth:`~coredis.RedisCluster.evalsha_ro`, :meth:`~coredis.RedisCluster.eval_ro`, :meth:`~coredis.RedisCluster.fcall_ro`
    are included in readonly commands for cluster readonly mode.
  * Change version related warnings to use :exc:`DeprecationWarning`

* Chores

  * General improvements in reliability and correctness of unit tests

v3.5.1
------
Release Date: 2022-05-12

* Bug Fix

  * Fix type annotation for :attr:`coredis.response.types.PubSubMessage.data` to include int
    for server responses to subscribe/unsubscribe/psubscribe/punsubscribe

v3.5.0
------
Release Date: 2022-05-10

* Features

  * Added :meth:`coredis.commands.Library.wraps` and :meth:`coredis.commands.Script.wraps` decorators
    for creating strict signature wrappers for lua scripts and
    functions.
  * Add :meth:`~coredis.commands.Script.__call__` method to :class:`coredis.commands.Script` so it can be called
    directly without having to go through :meth:`coredis.commands.Script.execute`
  * Improve type safety with regards to command methods accepting
    multiple keys or values. These were previously annotated as
    accepting either ``Iterable[KeyT]`` or ``Iterable[ValueT]`` which
    would allow strings or bytes to be passed. These are now changed to
    ``Parameters[KeyT]`` or ``Parameter[ValueT]`` respectively which only
    allow a restricted set of collections and reject strings and bytes.

* Breaking Changes

  * Removed custom client side implementations for cross slot cluster methods.
    These methods will now use the regular cluster implementation and raise
    and error if the keys don't map to the same shard.
  * :paramref:`coredis.Redis.verify_version` on both :class:`~coredis.Redis` &
    :class:`~coredis.RedisCluster` constructors will
    default to ``True`` resulting in warnings being emitted for using
    deprecated methods and preemptive exceptions being raised when calling
    methods against server versions that do not support them.
  * Dropped support for redis server versions less than 6.0
  * A large chunk of utility / private code has been moved into
    private namespaces

* Chores

  * Refactor response transformation to use inlined callbacks
    to improve type safety.

* Bug Fixes

  * Ensure protocol_version, decoding arguments are consistent
    across different construction methods.
  * Synchronize parameters for replacing library code between :class:`coredis.commands.Library`
    constructor and :meth:`coredis.Redis.register_library`

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

v2.3.2
------
Release Date: 2023-01-09

Bug Fix:

    * Fix incorrect argument (key instead of field) used for
      hincrby command

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






