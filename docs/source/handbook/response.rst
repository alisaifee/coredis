Response
--------

RESP
^^^^
As of redis `6.0.0` clients can use the :term:`RESP3` protocol which provides support for a much
larger set of types (which reduces the need for clients to "guess" what the type of a command's response should be).

**coredis** versions up to ``5.x`` provided backward compatibility for ``RESP``, however this is no
longer supported and :term:`RESP3` is always used.

API Responses
^^^^^^^^^^^^^

coredis commands return responses mapped as closely from :term:`RESP3` to python types as possible
(See :ref:`api/typing:response types`).


=======================================
Transforming Responses to Custom Types
=======================================
You can chain the :meth:`~coredis.commands.CommandRequest.transform` method on the response
from a command to convert responses to custom Python types or to enforce more specific type constraints.
The :class:`~coredis.typing.TypeAdapter` handles serialization of parameters passed to the commands
and deserialization of the responses.

For example, using :class:`~decimal.Decimal` for numeric responses:

.. code-block:: python

    from decimal import Decimal
    from coredis import Redis
    from coredis.typing import Serializable, TypeAdapter

    adapter = TypeAdapter()

    # Register a serializer so Decimal types can be passed into redis
    # commands in a type safe manner.
    @adapter.serializer
    def decimal_to_str(value: Decimal) -> str:
        return str(value)

    # registration can be done without using a decorator as well
    # adapter.register_serializer(serializable_type=Decimal, serializer=decimal_to_str)


    # Register a deserializer to retrieve string or byte values as
    # Decimal types
    @adapter.deserializer
    def str_to_decimal(value: str | bytes) -> Decimal:
        return Decimal(value.decode("utf-8") if isinstance(value, bytes) else value)

    # registration can be done without using a decorator as well
    # adapter.register_deserializer(
    #     deserialized_type=Decimal, deserializer=decimal_to_str, deserializable_type=str|bytes
    # )

    client = Redis(type_adapter=adapter, decode_responses=True)
    async with client:
        await client.set("price", Serializable(Decimal("19.99")))
        value = await client.get("price").transform(Decimal)
        assert isinstance(value, Decimal)

=============================
Automatic Collection Handling
=============================

Collections of types that have been registered for serialization or deserialization are implicitly
handled by coredis.

.. code-block:: python

    async with client:
        await client.lpush("prices", [Serializable(Decimal("9.99")), Serializable(Decimal("19.99"))])
        prices = await client.lrange("prices", 0, -1).transform(list[Decimal])
        assert all(isinstance(x, Decimal) for x in prices)

:class:`dict` is similarly managed:

.. code-block:: python

    async with client:
        await client.hset("inventory", {"apples": Serializable(Decimal(5)), "oranges": Serializable(Decimal(10))})
        inventory = await client.hgetall("inventory").transform(dict[str, Decimal])
        assert inventory["apples"] == Decimal(5)

As are :class:`set` and :class:`tuple`

==========================
Inline Transform Callables
==========================
If you prefer not to use :class:`coredis.typing.TypeAdapter` to register serializers & deserializers
you can also provide a callable directly to :meth:`~coredis.commands.CommandRequest.transform`:

.. code-block:: python

    await client.set("value", 1)
    result = await client.get("value").transform(lambda x: float(x))
    assert result == 1.0


.. note:: The :meth:`~coredis.commands.CommandRequest.transform` method will correctly
   resolve the final response type based on the arguments it is provided. For example::

      from typing import TYPE_CHECKING
      async with client:
          await client.set("one", 1)
          a = await client.get("one").transform(Decimal)
          b = await client.get("one").transform(lambda value: float(value))
          if TYPE_CHECKING:
              reveal_locals()
          # note: Revealed local types are:
          # note:     TYPE_CHECKING: builtins.bool
          # note:     a: decimal.Decimal
          # note:     b: builtins.float
          # note:     client: coredis.client.basic.Redis[builtins.bytes]