Scripting
---------

LUA Scripts
^^^^^^^^^^^
coredis supports the :rediscommand:`EVAL`, :rediscommand:`EVALSHA`, and :rediscommand:`SCRIPT` commands. However, there are
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
  script. If client isn't specified, the client that initially
  created the :class:`coredis.commands.Script` instance (the one that :meth:`~coredis.Redis.register_script` was
  invoked from) will be used.
* **callback**: A custom callback to call on the raw response from redis beforee
  returning it.

Continuing the example from above:

.. code-block:: python

    await r.set('foo', 2)
    await multiply(keys=['foo'], args=[5])
    # 10
    await multiply(keys=['foo'], args=[5], callback=lambda value: float(value))
    # 10.0

The value of key 'foo' is set to 2. When multiply is invoked, the 'foo' key is
passed to the script along with the multiplier value of 5. LUA executes the
script and returns the result, 10.

Script instances can be executed using a different client instance, even one
that points to a completely different Redis server.

.. code-block:: python

    async with coredis.Redis() as r2:
        await r2.set('foo', 3)
        await multiply(keys=['foo'], args=[5], client=r2)
        # 15

The Script object ensures that the LUA script is loaded into Redis's script
cache. In the event of a ``NOSCRIPT`` error, it will load the script and retry
executing it.

Script instances can also be used in pipelines. The pipeline instance should be
passed as the client argument when calling the script. Care is taken to ensure
that the script is registered in Redis's script cache just prior to pipeline
execution.

.. code-block:: python

    async with r.pipeline() as pipe:
        r1 = pipe.set('foo', 5)
        r2 = multiply(keys=['foo'], args=[5], client=pipe)
    assert await r1
    assert 25 == await r2

Library Functions
^^^^^^^^^^^^^^^^^

Starting with :redis-version:`7.0` a more sophisticated approach to managing
server side scripts is available through libraries and functions (See `Redis functions <https://redis.io/docs/develop/programmability/functions-intro/>`__).
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

=================
Simple invocation
=================

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

===================================
Binding a library to a python class
===================================

.. versionadded:: 3.5.0

Using the simple API as shown above gets the job done, but suffers from having an
error prone interface to the underlying lua functions and would normally require
mapping and validation before passing the ``keys`` and ``args`` to the function.

This can be better represented by subclassing :class:`~coredis.commands.Library`
and using the :meth:`~coredis.commands.function.wraps` decorator to bind python
signatures to redis functions.

Using the same example ``mylib`` lua library, this could be mapped to a python class as follows::

    from typing import AnyStr, List
    import coredis
    from coredis.commands import CommandRequest, Library
    from coredis.commands.function import wraps
    from coredis.typing import KeyT, ValueT

    class MyLib(Library[AnyStr]):
        NAME = "mylib"  # the name in the class variable is considered by the superclass constructor
        CODE = open("/var/tmp/library.lua").read()  # the code in the class variable is considered by the superclass constructor

        @wraps()
        def echo(self, value: str) -> CommandRequest[str]: ...  # type: ignore[empty-body]

        @wraps()
        def ping(self) -> CommandRequest[str]: ...  # type: ignore[empty-body]

        @wraps()
        def get(self, key: KeyT) -> CommandRequest[ValueT]: ... # type: ignore[empty-body]

        @wraps()
        def hmmget(self, *keys: KeyT, **fields_with_defaults: ValueT) -> CommandRequest[List[ValueT]]:  # type: ignore[empty-body]
            """
            Return values of ``fields_with_defaults`` on a first come first serve
            basis from the hashes at ``keys``. Since ``fields_with_defaults`` is a mapping
            the keys are mapped to hash fields and the values are used
            as defaults if they are not found in any of the hashes at ``keys``
            """
            ...
The above example uses default arguments with :meth:`~coredis.commands.Library.wraps` to show
what is possible by simply using the :data:`coredis.typing.KeyT` annotation to map arguments
of the decorated methods to ``keys`` and the remaining arguments as ``args``. Refer to the
API documentation of :meth:`coredis.commands.function.wraps` for details on how to customize
the key/argument mapping behavior.

This can now be used as you would expect::

    client = coredis.Redis()
    async with client:
        lib = await MyLib(client, replace=True)
        await lib.ping()
        # b"pong"
        await lib.echo("hello world")
        # b"hello world"
        await client.hset("k1", {"a": 10, "b": 20})
        await client.hset("k2", {"c": 30, "d": 40})

        await lib.hmmget("k1", "k2", a=1, b=2, c=3, d=4, e=5, f=6)
        # [b"10", b"20", b"30", b"40", b"5", b"6"]




Libraries can also be used with pipelines::

    async with coredis.Redis() as client:
        async with client.pipeline() as pipeline:
            lib = await MyLib(pipeline)
