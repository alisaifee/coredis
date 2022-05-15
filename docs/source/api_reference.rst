API Reference
=============

Encoding/Decoding
^^^^^^^^^^^^^^^^^

Param :paramref:`~coredis.Redis.encoding` and :paramref:`~coredis.Redis.decode_responses`
are used to support response encoding.

``encoding`` is used for specifying with which encoding you want responses to be decoded.
``decode_responses`` is used for tell the client whether responses should be decoded.

If ``decode_responses`` is set to ``True`` and no encoding is specified, client will use ``utf-8`` by default.

Typing
^^^^^^
**coredis** provides type hints for the public API. These are tested using
both :pypi:`mypy` and :pypi:`pyright`.

The :class:`Redis` and :class:`RedisCluster` clients are Generic types constrained
by :class:`AnyStr`. The constructors and ``from_url`` factory methods infer
the appropriate specialization automatically.

Without decoding:

.. code-block::

    client = coredis.Redis(
        "localhost", 6379, db=0, decode_responses=False, encoding="utf-8"
    )
    await client.set("string", 1)
    await client.lpush("list", [1])
    await client.hset("hash", {"a": 1})
    await client.sadd("set", ["a"])
    await client.zadd("sset", {"a": 1.0, "b": 2.0})

    str_response = await client.get("string")
    list_response_ = await client.lrange("list", 0, 1)
    hash_response = await client.hgetall("hash")
    set_response = await client.smembers("set")
    sorted_set_members_only_response = await client.zrange("sset", -1, 1)

    reveal_locals()
    # note: Revealed local types are:
    # note:     client: coredis.client.Redis[builtins.bytes]
    # note:     hash_response: builtins.dict*[builtins.bytes*, builtins.bytes*]
    # note:     list_response_: builtins.list*[builtins.bytes*]
    # note:     set_response: builtins.set*[builtins.bytes*]
    # note:     sorted_set_members_only_response: builtins.tuple*[builtins.bytes*, ...]
    # note:     str_response: builtins.bytes*

With decoding:

.. code-block::

    client = coredis.Redis(
        "localhost", 6379, db=0, decode_responses=True, encoding="utf-8"
    )
    await client.set("string", 1)
    await client.lpush("list", [1])
    await client.hset("hash", {"a": 1})
    await client.sadd("set", ["a"])
    await client.zadd("sset", {"a": 1.0, "b": 2.0})

    str_response = await client.get("string")
    list_response_ = await client.lrange("list", 0, 1)
    hash_response = await client.hgetall("hash")
    set_response = await client.smembers("set")
    sorted_set_members_only_response = await client.zrange("sset", -1, 1)

    reveal_locals()
    # note: Revealed local types are:
    # note:     client: coredis.client.Redis[builtins.str]
    # note:     hash_response: builtins.dict*[builtins.str*, builtins.str*]
    # note:     list_response_: builtins.list*[builtins.str*]
    # note:     set_response: builtins.set*[builtins.str*]
    # note:     sorted_set_members_only_response: builtins.tuple*[builtins.str*, ...]
    # note:     str_response: builtins.str*

=====================
Runtime Type checking
=====================

**coredis** optionally wraps all command methods with :pypi:`beartype` decorators to help
detect errors during testing (or if you are b(ea)rave enough, always).

This can be enabled by installing :pypi:`beartype` and setting the :data:`COREDIS_RUNTIME_CHECKS`
environment variable.

As an example:

.. code-block:: bash

    $ COREDIS_RUNTIME_CHECKS=1 python -c "
    import coredis
    import asyncio
    asyncio.new_event_loop().run_until_complete(coredis.Redis().set(1,1))
    """
    Traceback (most recent call last):
      File "<@beartype(coredis.commands.core.CoreCommands.set) at 0x10c403130>", line 33, in set
    beartype.roar.BeartypeCallHintParamViolation: @beartyped coroutine CoreCommands.set() parameter key=1 violates type hint typing.Union[str, bytes], as 1 not str or bytes.


Connections
^^^^^^^^^^^

ConnectionPools manage a set of Connection instances. coredis ships with two
types of Connections. The default, Connection, is a normal TCP socket based
connection. The :class:`~coredis.connection.UnixDomainSocketConnection` allows
for clients running on the same device as the server to connect via a unix domain socket.
To use a :class:`~coredis.connection.UnixDomainSocketConnection` connection,
simply pass the :paramref:`~coredis.Redis.unix_socket_path` argument,
which is a string to the unix domain socket file.

Additionally, make sure the parameter is defined in your redis.conf file. It's
commented out by default.

.. code-block:: python

    r = coredis.Redis(unix_socket_path='/tmp/redis.sock')

You can create your own Connection subclasses as well. This may be useful if
you want to control the socket behavior within an async framework. To
instantiate a client class using your own connection, you need to create
a connection pool, passing your class to the connection_class argument.
Other keyword parameters you pass to the pool will be passed to the class
specified during initialization.

.. code-block:: python

    pool = coredis.ConnectionPool(connection_class=YourConnectionClass,
                                    your_arg='...', ...)

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


=====
RESP3
=====
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


Scripting
^^^^^^^^^
====================
Isolated LUA Scripts
====================
coredis supports the ``EVAL``, ``EVALSHA``, and ``SCRIPT`` commands. However, there are
a number of edge cases that make these commands tedious to use in real world
scenarios. Therefore, coredis exposes a :class:`~coredis.commands.Script`
class that makes scripting much easier to use.

To create a Script instance, use the :meth:`~coredis.Redis.register_script` function on a client
instance passing the LUA code as the first argument. :meth:`coredis.Redis.register_script` returns
a :class:`~coredis.commands.Script` instance that you can use throughout your code.

The following trivial LUA script accepts two parameters: the name of a key and
a multiplier value. The script fetches the value stored in the key, multiplies
it with the multiplier value and returns the result.

.. code-block:: python

    r = coredis.Redis()
    lua = """
    local value = redis.call('GET', KEYS[1])
    value = tonumber(value)
    return value * ARGV[1]"""
    multiply = r.register_script(lua)

`multiply` is now a :class:`~coredis.commands.Script` instance that is
invoked by calling it like a function. Script instances accept the following optional arguments:

* **keys**: A list of key names that the script will access. This becomes the
  KEYS list in LUA.
* **args**: A list of argument values. This becomes the ARGV list in LUA.
* **client**: A coredis Client or Pipeline instance that will invoke the
  script. If client isn't specified, the client that intiially
  created the Script instance (the one that `register_script` was
  invoked from) will be used.

Continuing the example from above:

.. code-block:: python

    await r.set('foo', 2)
    await multiply(keys=['foo'], args=[5])
    # 10

The value of key 'foo' is set to 2. When multiply is invoked, the 'foo' key is
passed to the script along with the multiplier value of 5. LUA executes the
script and returns the result, 10.

Script instances can be executed using a different client instance, even one
that points to a completely different Redis server.

.. code-block:: python

    r2 = coredis.Redis('redis2.example.com')
    await r2.set('foo', 3)
    multiply(keys=['foo'], args=[5], client=r2)
    # 15

The Script object ensures that the LUA script is loaded into Redis's script
cache. In the event of a ``NOSCRIPT`` error, it will load the script and retry
executing it.

Script instances can also be used in pipelines. The pipeline instance should be
passed as the client argument when calling the script. Care is taken to ensure
that the script is registered in Redis's script cache just prior to pipeline
execution.

.. code-block:: python

    pipe = await r.pipeline()
    await pipe.set('foo', 5)
    await multiply(keys=['foo'], args=[5], client=pipe)
    await pipe.execute()
    # [True, 25]

=================
Library Functions
=================

Starting with :redis-version:`7.0` a more sophisticated approach to managing
server side scripts is available through libraries and functions (See `Redis functions <https://redis.io/docs/manual/programmability/functions-intro/>`__.
Instead of managing individual snippets of lua code, you can group related server side
functions under a library. **coredis** exposes all function related redis commands
through :class:`coredis.Redis` and additionally provides an abstraction via the
:class:`~coredis.commands.Library` and :class:`~coredis.commands.Function` classes.

The following ``mylib`` library will be used in the subsequent examples.

.. code-block:: lua

   #!lua name=mylib

   redis.register_function('echo', function(k, a)
       return a[1]
   end)
   redis.register_function('ping', function()
       return "PONG"
   end)
   redis.register_function('get', function(k, a)
       return redis.call("GET", k[1])
   end)
   redis.register_function('hmmget', function(k, a)
       local values = {}
       local fields = {}
       local response = {}
       local i = 1
       local j = 1

       while a[i] do
           fields[j] = a[i]
           i = i + 2
           j = j + 1
       end

       for idx, key in ipairs(k) do
           values = redis.call("HMGET", key, unpack(fields))
           for idx, value in ipairs(values) do
               if not response[idx] and value then
                   response[idx] = value
               end
           end
       end
       for idx, value in ipairs(fields) do
           if not response[idx] then
               response[idx] = a[idx*2]
           end
       end
       return response
   end)

Simple invocation
-----------------

To register the library (assuming it is stored as a file at ``/var/tmp/library.lua``),
use the :meth:`~coredis.Redis.register_library` method (which also returns an instance
of :class:`~coredis.commands.Library` bound to the client and library code).::

    client = coredis.Redis()
    library = await client.register_library("mylib", open("/var/tmp/library.lua").read())

.. danger:: If a library with the same name had already been registered before, calling
   :meth:`~coredis.Redis.register_library` will raise an exception. If you want to
   force registering you can pass ``True`` to :paramref:`~coredis.Redis.register_library.replace`.
   Otherwise, a registered library can be loaded using the :meth:`~coredis.Redis.load_library` method as follows::

    library = await client.load_library("mylib")

You can inspect the functions registered in the library by accessing the :data:`~coredis.commands.Library.functions`
property::

    print(library.functions)
    # {b'echo': <coredis.commands.function.Function object at 0x110a3d670>,
    # b'get': <coredis.commands.function.Function object at 0x1138f3a60>,
    # b'hmget': <coredis.commands.function.Function object at 0x110abab20>,
    # b'ping': <coredis.commands.function.Function object at 0x110845d30>}

And then invoke them (this internally calls the :meth:`~coredis.Redis.fcall` method)::

    await library["echo"]([], ["hello world"])
    # b"hello world"
    await library["ping"]([], [])
    # b"ping"
    await client.set("co", "redis")
    await library["get"](["co"], [])
    # b"redis"

Binding a library to python class
---------------------------------

.. versionadded:: 3.5.0

Using the simple API as shown above gets the job done, but suffers from having an
error prone interface to the underlying lua functions and would normally require
mapping and validation before passing the ``keys`` and ``args`` to the function.

This can be better represented by subclassing :class:`~coredis.commands.Library`
and using the :meth:`~coredis.commands.Library.wraps` decorator to bind python
signatures to redis functions.

Using the same example ``mylib`` lua library, this could be mapped to a python class as follows::

    from typing import List
    import coredis
    from coredis.commands import Library
    from coredis.typing import KeyT, ValueT

    class MyLib(Library):
        NAME = "mylib"  # the name in the class variable is considered by the superclass constructor
        CODE = open("/var/tmp/library.lua").read()  # the code in the class variable is considered by the superclass constructor

        @Library.wraps("echo")
        def echo(self, value: str) -> str: ...

        @Library.wraps("ping")
        def ping(self) -> str: ...

        @Library.wraps("get")
        def get(self, key: KeyT) -> ValueT: ...

        @Library.wraps("hmmget")
        def hmmget(self, *keys: KeyT, **fields_with_defaults: ValueT) -> List[ValueT]: ...
            """
            Return values of ``fields_with_defaults`` on a first come first serve
            basis from the hashes at ``keys``. Since ``fields_with_defaults`` is a mapping
            the keys are mapped to hash fields and the values are used
            as defaults if they are not found in any of the hashes at ``keys``
            """

The above example uses default arguments with :meth:`~coredis.commands.Library.wraps` to show
what is possible by simply using the :data:`coredis.typing.KeyT` annotation to map arugments
of the decorated methods to ``keys`` and the remaining arguments as ``args``. Refer to the
API documentation of :meth:`coredis.commands.Library.wraps` for details on how to customize
the key/argument mapping behavior.

This can now be used as you would expect::

    client = coredis.Redis()
    lib = await MyLib(client, replace=True)
    await lib.ping()
    # b"pong"
    await lib.echo("hello world")
    # b"hello world"
    await client.hset("k1", {"a": 10, "b": 20})
    await client.hset("k2", {"c": 30, "d": 40})

    await lib.hmmget("k1", "k2", a=1, b=2, c=3, d=4, e=5, f=6)
    # [b"10", b"20", b"30", b"40", b"5", b"6"]

Pipelines
^^^^^^^^^^

Pipelines expose an API "similar" to :class:`~coredis.Redis` with the exception
that calling any redis command returns the pipeline instance itself.

To retrieve the actual results of each command queued in the pipeline you must call
:meth:`~coredis.pipeline.Pipeline.execute`

For example:


.. code-block:: python

    async def example(client):
        async with await client.pipeline() as pipe:
            await pipe.delete(['bar'])
            await pipe.set('bar', 'foo')
            await pipe.execute()  # needs to be called explicitly


Here are more examples:


.. code-block:: python

    async def example(client):
        async with await client.pipeline(transaction=True) as pipe:
            # will return self to send another command
            pipe = await (await pipe.flushdb()).set('foo', 'bar')
            # can also directly send command
            await pipe.set('bar', 'foo')
            # commands will be buffered
            await pipe.keys('*')
            res = await pipe.execute()
            # results should be in order corresponding to your command
            assert res == (True, True, True, set([b'bar', b'foo']))

For ease of use, all commands being buffered into the pipeline return the
pipeline object itself. Which enable you to use it like the example provided.

In addition, pipelines can also ensure the buffered commands are executed
atomically as a group. This happens by default. If you want to disable the
atomic nature of a pipeline but still want to buffer commands, you can turn
off transactions.

.. code-block:: python

    pipe = r.pipeline(transaction=False)

A common issue occurs when requiring atomic transactions but needing to
retrieve values in Redis prior for use within the transaction. For instance,
let's assume that the INCR command didn't exist and we need to build an atomic
version of ``INCR`` in Python.

The completely naive implementation could GET the value, increment it in
Python, and ``SET`` the new value back. However, this is not atomic because
multiple clients could be doing this at the same time, each getting the same
value from ``GET``.

Enter the ``WATCH`` command. ``WATCH`` provides the ability to monitor one or more keys
prior to starting a transaction. If any of those keys change prior the
execution of that transaction, the entire transaction will be canceled and a
WatchError will be raised. To implement our own client-side INCR command, we
could do something like this:

.. code-block:: python

    async def example():
        async with await r.pipeline() as pipe:
            while True:
                try:
                    # put a WATCH on the key that holds our sequence value
                    await pipe.watch('OUR-SEQUENCE-KEY')
                    # after WATCHing, the pipeline is put into immediate execution
                    # mode until we tell it to start buffering commands again.
                    # this allows us to get the current value of our sequence
                    current_value = await pipe.get('OUR-SEQUENCE-KEY')
                    next_value = int(current_value) + 1
                    # now we can put the pipeline back into buffered mode with MULTI
                    pipe.multi()
                    await pipe.set('OUR-SEQUENCE-KEY', next_value)
                    # and finally, execute the pipeline (the set command)
                    await pipe.execute()
                    # if a WatchError wasn't raised during execution, everything
                    # we just did happened atomically.
                    break
                except WatchError:
                    # another client must have changed 'OUR-SEQUENCE-KEY' between
                    # the time we started WATCHing it and the pipeline's execution.
                    # our best bet is to just retry.
                    continue

Note that, because the Pipeline must bind to a single connection for the
duration of a WATCH, care must be taken to ensure that the connection is
returned to the connection pool by calling the reset() method. If the
Pipeline is used as a context manager (as in the example above) :meth:`~coredis.Pipeline.reset`
will be called automatically. Of course you can do this the manual way by
explicitly calling :meth:`~coredis.Pipeline.reset`:

.. code-block:: python

    async def example():
        async with await r.pipeline() as pipe:
            while 1:
                try:
                    await pipe.watch('OUR-SEQUENCE-KEY')
                    ...
                    await pipe.execute()
                    break
                except WatchError:
                    continue
                finally:
                    await pipe.reset()

A convenience method named "transaction" exists for handling all the
boilerplate of handling and retrying watch errors. It takes a callable that
should expect a single parameter, a pipeline object, and any number of keys to
be ``WATCH``ed. Our client-side ``INCR`` command above can be written like this,
which is much easier to read:

.. code-block:: python

    async def client_side_incr(pipe):
        current_value = await pipe.get('OUR-SEQUENCE-KEY')
        next_value = int(current_value) + 1
        pipe.multi()
        await pipe.set('OUR-SEQUENCE-KEY', next_value)

    await r.transaction(client_side_incr, 'OUR-SEQUENCE-KEY')
    # [True]


PubSub
^^^^^^

coredis includes a :class:`~coredis.commands.PubSub` class
that subscribes to channels and listens for new messages. Creating a :class:`~coredis.commands.PubSub` instance
can be done through the :meth:`~coredis.Redis.pubsub` or :meth:`~coredis.RedisCluster.pubsub` methods.

.. code-block:: python

    r = coredis.Redis(...)
    p = r.pubsub()

Once a :class:`~coredis.commands.PubSub` instance is created,
channels and patterns can be subscribed to.

.. code-block:: python

    await p.subscribe('my-first-channel', 'my-second-channel', ...)
    await p.psubscribe('my-*', ...)

The :class:`~coredis.commands.PubSub` instance is now subscribed to those channels/patterns. The
subscription confirmations can be seen by reading messages from the :class:`~coredis.commands.PubSub`
instance.

.. code-block:: python

    await p.get_message()
    # {'pattern': None, 'type': 'subscribe', 'channel': 'my-second-channel', 'data': 1L}
    await p.get_message()
    # {'pattern': None, 'type': 'subscribe', 'channel': 'my-first-channel', 'data': 2L}
    await p.get_message()
    # {'pattern': None, 'type': 'psubscribe', 'channel': 'my-*', 'data': 3L}

Every message read from a :class:`~coredis.commands.PubSub` instance
will be a typed dictionary defined as:

.. autoclass:: coredis.response.types.PubSubMessage
   :noindex:
   :no-inherited-members:
   :show-inheritance:


Let's send a message now.

.. code-block:: python

    # the publish method returns the number matching channel and pattern
    # subscriptions. 'my-first-channel' matches both the 'my-first-channel'
    # subscription and the 'my-*' pattern subscription, so this message will
    # be delivered to 2 channels/patterns
    await r.publish('my-first-channel', 'some data')
    # 2
    await p.get_message()
    # {'channel': 'my-first-channel', 'data': 'some data', 'pattern': None, 'type': 'message'}
    await p.get_message()
    # {'channel': 'my-first-channel', 'data': 'some data', 'pattern': 'my-*', 'type': 'pmessage'}

Unsubscribing works just like subscribing. If no arguments are passed to
[p]unsubscribe, all channels or patterns will be unsubscribed from.

.. code-block:: python

    await p.unsubscribe()
    await p.punsubscribe('my-*')
    await p.get_message()
    # {'channel': 'my-second-channel', 'data': 2L, 'pattern': None, 'type': 'unsubscribe'}
    await p.get_message()
    # {'channel': 'my-first-channel', 'data': 1L, 'pattern': None, 'type': 'unsubscribe'}
    await p.get_message()
    # {'channel': 'my-*', 'data': 0L, 'pattern': None, 'type': 'punsubscribe'}

coredis also allows you to register callback functions to handle published
messages. Message handlers take a single argument, the message, which is a
dictionary just like the examples above. To subscribe to a channel or pattern
with a message handler, pass the channel or pattern name as a keyword argument
with its value being the callback function.

When a message is read on a channel or pattern with a message handler, the
message dictionary is created and passed to the message handler. In this case,
a ``None`` value is returned from :meth:`~coredis.commands.PubSub.get_message`
since the message was already handled.

.. code-block:: python

    def my_handler(message):
        print('MY HANDLER: ', message['data'])
    await p.subscribe(**{'my-channel': my_handler})
    # read the subscribe confirmation message
    await p.get_message()
    # {'pattern': None, 'type': 'subscribe', 'channel': 'my-channel', 'data': 1L}
    await r.publish('my-channel', 'awesome data')
    # 1

    # for the message handler to work, we need tell the instance to read data.
    # this can be done in several ways (read more below). we'll just use
    # the familiar get_message() function for now
    await message = p.get_message()
    # 'MY HANDLER:  awesome data'

    # note here that the my_handler callback printed the string above.
    # `message` is None because the message was handled by our handler.
    print(message)
    # None

If your application is not interested in the subscribe/unsubscribe confirmation messages,
you can ignore them by setting :paramref:`~coredis.Redis.pubsub.ignore_subscribe_messages`
to ``True``. This will cause all subscribe/unsubscribe messages to be read, but they won't
bubble up to your application.

.. code-block:: python

    p = r.pubsub(ignore_subscribe_messages=True)
    await p.subscribe('my-channel')
    await p.get_message()  # hides the subscribe message and returns None
    await r.publish('my-channel')
    # 1
    await p.get_message()
    # {'channel': 'my-channel', 'data': 'my data', 'pattern': None, 'type': 'message'}

There are two main strategies for reading messages.

The examples above have been using :meth:`~coredis.commands.PubSub.get_message`.
If there's data available to be read, the method will read it, format the message
and return it or pass it to a message handler. If there's no data to be read, it
will return ``None`` after the configured :paramref:`~coredis.commands.PubSub.get_message.timeout`

.. code-block:: python

    while True:
        message = await p.get_message()
        if message:
            # do something with the message
        await asyncio.sleep(0.001)  # be nice to the system :)

The second option runs an event loop in a separate thread.
:meth:`~coredis.commands.PubSub.run_in_thread` creates a new thread and uses
the event loop in the main thread. The thread instance of
:class:`~coredis.commands.pubsub.PubSubWorkerThread` is returned to the caller
of :meth:`~coredis.commands.PubSub.run_in_thread()`. The caller can use the
:meth:`~coredis.commands.pubsub.PubSubWorkerThread.stop` method on the thread
instance to shut down the event loop and thread. Behind the scenes, this is
simply a wrapper around :meth:`~coredis.commands.PubSub.get_message`
that runs in a separate thread, and use :func:`asyncio.run_coroutine_threadsafe`
to run coroutines.

Note: Since we're running in a separate thread, there's no way to handle
messages that aren't automatically handled with registered message handlers.
Therefore, coredis prevents you from calling :meth:`~coredis.commands.PubSub.run_in_thread`
if you're subscribed to patterns or channels that don't have message handlers attached.

.. code-block:: python

    await p.subscribe(**{'my-channel': my_handler})
    thread = p.run_in_thread(sleep_time=0.001)
    # the event loop is now running in the background processing messages
    # when it's time to shut it down...
    thread.stop()

PubSub instances remember what channels and patterns they are subscribed to. In
the event of a disconnection such as a network error or timeout, the
PubSub instance will re-subscribe to all prior channels and patterns when
reconnecting. Messages that were published while the client was disconnected
cannot be delivered. When you're finished with a PubSub object, call the
:meth:`~coredis.commands.PubSub.close` method to shutdown the connection.

.. code-block:: python

    p = r.pubsub()
    ...
    p.close()

The Pub/Sub support commands ``CHANNELS``, ``NUMSUB`` and ``NUMPAT`` are also
supported:

.. code-block:: python

    await r.pubsub_channels()
    # ['foo', 'bar']
    await r.pubsub_numsub('foo', 'bar')
    # [('foo', 9001), ('bar', 42)]
    await r.pubsub_numsub('baz')
    # [('baz', 0)]
    await r.pubsub_numpat()
    # 1204

===============
Cluster Pub/Sub
===============

The :class:`coredis.RedisCluster` client exposes two ways of building a :term:`Pub/Sub`
application.

1. :meth:`~coredis.RedisCluster.pubsub` returns an instance of :class:`coredis.commands.ClusterPubSub`
which exposes identical functionality to the non clustered client. This is possible
without worrying about sharding as the ``PSUBLISH`` command in clustered redis results
in messages being broadcasted to every node in the cluster. This ofcourse inherently limits the
potential for horizontal scaling.

2. As of :redis-version:`7.0.0` support for :term:`Sharded Pub/Sub` has been added
through the ``SSUBSCRIBE``, ``SUNSUBSCRIBE`` and ``SPUBLISH`` commands which restricts publishing
of messages to individual shards based on the same algorithm used to route keys to shards.
Note that there is no corresponding support for pattern based subscriptions
(as you might have guessed, it wouldn't be possible to shard those).

Access to Sharded Pub/Sub is available through the :meth:`coredis.RedisCluster.sharded_pubsub`
method which exposes the same api and functionality (except for pattern support) as the other previously mentioned `PubSub` classes.

To publish a messages that is meant to be consumed by a Sharded Pub/Sub consumer use
:meth:`coredis.Redis.spublish` instead of :meth:`coredis.Redis.publish`

Distributed Locking
^^^^^^^^^^^^^^^^^^^

There are two kinds of `Lock class` available:

- :class:`~coredis.lock.Lock` [Using :ref:`api_reference:pipelines`]
- :class:`~coredis.lock.LuaLock` [Using :ref:`api_reference:scripting`]

A lock can be acquired using the :meth:`coredis.Redis.lock` or :meth:`coredis.RedisCluster.lock`
methods.

For example:

.. code-block:: python

   async def example():
       client = coredis.Redis()
       await client.flushall()
       lock = client.lock('lalala')
       print(await lock.acquire())
       # True
       print(await lock.acquire(blocking=False))
       # False
       print(await lock.release())
       # None
       try:
           await lock.release()
       except LockError as err:
           print(err)
           # coredis.exceptions.LockError: Cannot release an unlocked lock


============
Cluster Lock
============

:class:`~coredis.lock.ClusterLock` is supposed to solve distributed lock problem
in redis cluster. Since high availability is provided by redis cluster using primary-replica model,
the kind of lock aims to solve the fail-over problem referred in distributed lock
post given by redis official. This implementation isGjjk

Quoting the documentation from the original author of :pypi:`aredis`:

    Why not use Redlock algorithm provided by official directly?

    It is impossible to make a key hashed to different nodes
    in a redis cluster and hard to generate keys
    in a specific rule and make sure they do not migrated in cluster.
    In the worst situation, all key slots may exists in one node.
    Then the availability will be the same as one key in one node.

    For more discussion please see:
    https://github.com/NoneGG/aredis/issues/55

    To gather more ideas i also raise a problem in stackoverflow:
    Not_a_Golfer's solution is awesome, but considering the migration problem, i think this solution may be better.
    https://stackoverflow.com/questions/46438857/how-to-create-a-distributed-lock-using-redis-cluster

    My solution is described below:

    1. random token + SETNX + expire time to acquire a lock in cluster master node

    2. if lock is acquired successfully then check the lock in replica nodes(may there be N replica nodes)
    using READONLY mode, if N/2+1 is synced successfully then break the check and return True,
    time used to check is also accounted into expire time

    3.Use lua script described in redlock algorithm to release lock
    with the client which has the randomly generated token,
    if the client crashes, then wait until the lock key expired.

    Actually you can regard the algorithm as a primary-replica version of redlock,
    which is designed for multi master nodes.

    Please read these article below before using this cluster lock in your app.

    - https://redis.io/topics/distlock
    - http://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
    - http://antirez.com/news/101

.. code-block:: python

    async def example():
        client = coredis.RedisCluster("localhost", 7000)
        await client.flushall()
        lock = client.lock('lalala', lock_class=ClusterLock, timeout=1)
        print(await lock.acquire())
        # True
        print(await lock.acquire(blocking=False))
        # False
        print(await lock.release())
        # None
        try:
            await lock.release()
        except LockError as err:
            print(err)
            # coredis.exceptions.LockError: cannot release an unlocked lock


