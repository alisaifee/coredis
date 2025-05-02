from __future__ import annotations

import asyncio
import inspect
import threading
from asyncio import CancelledError
from concurrent.futures import Future
from contextlib import aclosing, suppress
from functools import partial
from types import TracebackType
from typing import TYPE_CHECKING, Any, cast

import async_timeout
from deprecated.sphinx import deprecated, versionadded

from coredis._utils import CaseAndEncodingInsensitiveEnum, b, hash_slot, nativestr
from coredis.commands.constants import CommandName
from coredis.connection import BaseConnection, Connection
from coredis.exceptions import ConnectionError, PubSubError, TimeoutError
from coredis.response.types import PubSubMessage
from coredis.retry import (
    CompositeRetryPolicy,
    ConstantRetryPolicy,
    NoRetryPolicy,
    RetryPolicy,
)
from coredis.typing import (
    AnyStr,
    Awaitable,
    Callable,
    Generator,
    Generic,
    Mapping,
    MutableMapping,
    Parameters,
    ResponsePrimitive,
    ResponseType,
    Self,
    StringT,
    TypeVar,
    ValueT,
)

if TYPE_CHECKING:
    import coredis.client
    import coredis.connection
    import coredis.pool

T = TypeVar("T")


PoolT = TypeVar("PoolT", bound="coredis.pool.ConnectionPool")

#: Callables for message handler callbacks. The callbacks
#:  can be sync or async.
SubscriptionCallback = Callable[[PubSubMessage], Awaitable[None]] | Callable[[PubSubMessage], None]


class PubSubMessageTypes(CaseAndEncodingInsensitiveEnum):
    MESSAGE = b"message"
    PMESSAGE = b"pmessage"
    SMESSAGE = b"smessage"
    SUBSCRIBE = b"subscribe"
    UNSUBSCRIBE = b"unsubscribe"
    PSUBSCRIBE = b"psubscribe"
    PUNSUBSCRIBE = b"punsubscribe"
    SSUBSCRIBE = b"ssubscribe"
    SUNSUBSCRIBE = b"sunsubscribe"


class BasePubSub(Generic[AnyStr, PoolT]):
    PUBLISH_MESSAGE_TYPES = {
        PubSubMessageTypes.MESSAGE.value,
        PubSubMessageTypes.PMESSAGE.value,
    }
    SUBUNSUB_MESSAGE_TYPES = {
        PubSubMessageTypes.SUBSCRIBE.value,
        PubSubMessageTypes.PSUBSCRIBE.value,
        PubSubMessageTypes.UNSUBSCRIBE.value,
        PubSubMessageTypes.PUNSUBSCRIBE.value,
    }
    UNSUBSCRIBE_MESSAGE_TYPES = {
        PubSubMessageTypes.UNSUBSCRIBE.value,
        PubSubMessageTypes.PUNSUBSCRIBE.value,
    }

    channels: MutableMapping[StringT, SubscriptionCallback | None]
    patterns: MutableMapping[StringT, SubscriptionCallback | None]

    def __init__(
        self,
        connection_pool: PoolT,
        ignore_subscribe_messages: bool = False,
        retry_policy: RetryPolicy | None = CompositeRetryPolicy(
            ConstantRetryPolicy((ConnectionError,), 3, 0.1),
            ConstantRetryPolicy((TimeoutError,), 2, 0.1),
        ),
        channels: Parameters[StringT] | None = None,
        channel_handlers: Mapping[StringT, SubscriptionCallback] | None = None,
        patterns: Parameters[StringT] | None = None,
        pattern_handlers: Mapping[StringT, SubscriptionCallback] | None = None,
    ):
        self.initialized = False
        self.connection_pool = connection_pool
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.connection: coredis.connection.Connection | None = None
        self._retry_policy = retry_policy or NoRetryPolicy()
        self._initial_channel_subscriptions = {
            **{nativestr(channel): None for channel in channels or []},
            **{nativestr(k): v for k, v in (channel_handlers or {}).items()},
        }
        self._initial_pattern_subscriptions = {
            **{nativestr(pattern): None for pattern in patterns or []},
            **{nativestr(k): v for k, v in (pattern_handlers or {}).items()},
        }
        self._message_queue: asyncio.Queue[PubSubMessage | None] = asyncio.Queue()
        self._consumer_task: asyncio.Task[None] | None = None
        self.reset()

    @property
    def subscribed(self) -> bool:
        """Indicates if there are subscriptions to any channels or patterns"""
        return bool(self.channels or self.patterns)

    async def initialize(self) -> Self:
        """
        Ensures the pubsub instance is ready to consume messages
        by establishing a connection to the redis server, setting up any
        initial channel or pattern subscriptions that were specified during
        instantiation and starting the consumer background task.

        The method can be called multiple times without any
        risk as it will skip initialization if the consumer is already
        initialized.

        .. important:: This method doesn't need to be called explicitly
           as it will always be called internally before any relevant
           documented interaction.

        :return: the instance itself
        """
        if not self.initialized:
            self.connection = await self.connection_pool.get_connection()
            self.initialized = True
            if self._initial_channel_subscriptions:
                await self.subscribe(**self._initial_channel_subscriptions)
            if self._initial_pattern_subscriptions:
                await self.psubscribe(**self._initial_pattern_subscriptions)
            self.connection.register_connect_callback(self.on_connect)
            if not self._consumer_task or self._consumer_task.done():
                self._consumer_task = asyncio.create_task(self._consumer())
        return self

    async def psubscribe(
        self,
        *patterns: StringT,
        **pattern_handlers: SubscriptionCallback | None,
    ) -> None:
        """
        Subscribes to channel patterns. Patterns supplied as keyword arguments
        expect a pattern name as the key and a callable as the value. A
        pattern's callable will be invoked automatically when a message is
        received on that pattern rather than producing a message via
        :meth:`listen`.
        """
        new_patterns: MutableMapping[StringT, SubscriptionCallback | None] = {}
        new_patterns.update(dict.fromkeys(map(self.encode, patterns)))

        for pattern, handler in pattern_handlers.items():
            new_patterns[self.encode(pattern)] = handler
        await self.execute_command(CommandName.PSUBSCRIBE, *new_patterns.keys())
        # update the patterns dict AFTER we send the command. we don't want to
        # subscribe twice to these patterns, once for the command and again
        # for the reconnection.
        self.patterns.update(new_patterns)

    async def punsubscribe(self, *patterns: StringT) -> None:
        """
        Unsubscribes from the supplied patterns. If empty, unsubscribe from
        all patterns.
        """
        await self.execute_command(CommandName.PUNSUBSCRIBE, *patterns)

    async def subscribe(
        self,
        *channels: StringT,
        **channel_handlers: SubscriptionCallback | None,
    ) -> None:
        """
        Subscribes to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via :meth:`listen` or
        :meth:`get_message`.
        """

        new_channels: MutableMapping[StringT, SubscriptionCallback | None] = {}
        new_channels.update(dict.fromkeys(map(self.encode, channels)))

        for channel, handler in channel_handlers.items():
            new_channels[self.encode(channel)] = handler
        await self.execute_command(CommandName.SUBSCRIBE, *new_channels.keys())
        # update the channels dict AFTER we send the command. we don't want to
        # subscribe twice to these channels, once for the command and again
        # for the reconnection.
        self.channels.update(new_channels)

    async def unsubscribe(self, *channels: StringT) -> None:
        """
        Unsubscribes from the supplied channels. If empty, unsubscribe from
        all channels
        """

        await self.execute_command(CommandName.UNSUBSCRIBE, *channels)

    @deprecated(
        """
        Use :meth:`get_message` with :paramref:`get_message.timeout` as `None`
        or the instance itself as an async iterator to infinitely consume messages
        
        .. code:: python
        
            pubsub = client.pubsub()
            
            # instead of 
            while True:
                message = await pubsub.listen()
                
            # do 
            async for message in pubsub:
                ....
            # or 
            while True:
                message = await pubsub.get_message(timeout=None)
        
        If you were using this method to simply pull messages and/or
        ensure callbacks were being triggered when the message arrives
        this isn't necessary anymore and simply creating a pubsub instance
        and registering the handlers is sufficient to ensure the messages
        are fetched and callbacks are triggered.
        
        .. code:: python
        
           # instead of
           pubsub = client.pubsub()
           await pubsub.subscribe(**{"topic-a": topic_a_handler})
           while True:
               await pubsub.listen()
               
           # do
           pubsub = await client.pubsub(
             channel_handlers={"topic-a": topic_a_handler}
           )
           # when done 
           await pubsub.aclose()
           
               
        """,
        version="4.21.0",
    )
    async def listen(self) -> PubSubMessage | None:
        """
        Listens for messages on channels this client has been subscribed to
        """

        await self.initialize()
        if self.subscribed:
            return self._filter_ignored_messages(await self._message_queue.get())
        return None

    async def get_message(
        self,
        ignore_subscribe_messages: bool = False,
        timeout: int | float | None = None,
    ) -> PubSubMessage | None:
        """
        Gets the next message if one is available, otherwise None.

        :param ignore_subscribe_messages: Whether to skip subscription
         acknowledgement messages
        :param timeout: Number of seconds to wait for a message to be available
         on the connection. If the ``None`` the command will block forever.
        """

        try:
            await self.initialize()
            async with async_timeout.timeout(timeout):
                return self._filter_ignored_messages(
                    await self._message_queue.get(), ignore_subscribe_messages
                )
        except asyncio.TimeoutError:
            return None

    async def on_connect(self, connection: BaseConnection) -> None:
        """
        Re-subscribe to any channels and patterns previously subscribed to

        :meta private:
        """

        if self.channels:
            await self.subscribe(
                **{
                    k.decode(self.connection_pool.encoding) if isinstance(k, bytes) else k: v
                    for k, v in self.channels.items()
                }
            )

        if self.patterns:
            await self.psubscribe(
                **{
                    k.decode(self.connection_pool.encoding) if isinstance(k, bytes) else k: v
                    for k, v in self.patterns.items()
                }
            )

    def encode(self, value: StringT) -> StringT:
        """
        Encodes the value so that it's identical to what we'll read off the
        connection

        :meta private:
        """

        if self.connection_pool.decode_responses and isinstance(value, bytes):
            value = nativestr(value, self.connection_pool.encoding)
        elif not self.connection_pool.decode_responses and isinstance(value, str):
            value = b(value, self.connection_pool.encoding)

        return value

    async def execute_command(
        self, command: bytes, *args: ValueT, **options: ValueT
    ) -> ResponseType | None:
        """
        Executes a publish/subscribe command

        :meta private:
        """
        await self.initialize()

        if self.connection is None:
            self.connection = await self.connection_pool.get_connection()
            self.connection.register_connect_callback(self.on_connect)
        assert self.connection
        return await self._execute(self.connection, self.connection.send_command, command, *args)

    async def parse_response(
        self, block: bool = True, timeout: float | None = None
    ) -> ResponseType:
        """
        Parses the response from a publish/subscribe command

        :meta private:
        """
        await self.initialize()

        assert self.connection
        coro = self._execute(
            self.connection,
            partial(
                self.connection.fetch_push_message,
                block=block,
                push_message_types=self.SUBUNSUB_MESSAGE_TYPES | self.PUBLISH_MESSAGE_TYPES,
            ),
        )

        try:
            return await asyncio.wait_for(coro, timeout if (timeout and timeout > 0) else None)
        except asyncio.TimeoutError:
            return None

    async def handle_message(self, response: ResponseType) -> PubSubMessage | None:
        """
        Parses a pub/sub message. If the channel or pattern was subscribed to
        with a message handler, the handler is invoked instead of a parsed
        message being returned.

        :meta private:
        """
        r = cast(list[ResponsePrimitive], response)
        message_type = b(r[0])
        message_type_str = nativestr(r[0])
        message: PubSubMessage

        if message_type in self.SUBUNSUB_MESSAGE_TYPES:
            message = PubSubMessage(
                type=message_type_str,
                pattern=cast(StringT, r[1]) if message_type[0] == ord(b"p") else None,
                # This field is populated in all cases for backward compatibility
                # as older versions were incorrectly populating the channel
                # with the pattern on psubscribe/punsubscribe responses.
                channel=cast(StringT, r[1]),
                data=cast(int, r[2]),
            )

        elif message_type in self.PUBLISH_MESSAGE_TYPES:
            if message_type == PubSubMessageTypes.PMESSAGE:
                message = PubSubMessage(
                    type="pmessage",
                    pattern=cast(StringT, r[1]),
                    channel=cast(StringT, r[2]),
                    data=cast(StringT, r[3]),
                )
            else:
                message = PubSubMessage(
                    type="message",
                    pattern=None,
                    channel=cast(StringT, r[1]),
                    data=cast(StringT, r[2]),
                )
        else:
            raise PubSubError(f"Unknown message type {message_type_str}")  # noqa

        # if this is an unsubscribe message, remove it from memory
        if message_type in self.UNSUBSCRIBE_MESSAGE_TYPES:
            if message_type == PubSubMessageTypes.PUNSUBSCRIBE:
                subscribed_dict = self.patterns
            else:
                subscribed_dict = self.channels
            subscribed_dict.pop(message["channel"], None)

        if message_type in self.PUBLISH_MESSAGE_TYPES:
            handler = None
            if message_type == PubSubMessageTypes.PMESSAGE and message["pattern"]:
                handler = self.patterns.get(message["pattern"], None)
            elif message["channel"]:
                handler = self.channels.get(message["channel"], None)

            if handler:
                handler_response = handler(message)
                if inspect.isawaitable(handler_response):
                    await handler_response
                return None
        return message

    @deprecated(
        """
        Registered handlers are called in a background async task automatically 
        as soon as this pubsub instance is initialized. To achieve identical results 
        just subscribe to the channels or patterns with the appropriate handlers
        and call :meth:`aclose` when done.
        
        
        .. code:: python
        
            pubsub = client.pubsub()
            await client.subscribe(topic=topic_handler)
            await client.psubscribe(topic=pattern_handler)
            # when done
            await pubsub.aclose() 
        """,
        version="4.21.0",
    )
    def run_in_thread(self, poll_timeout: float = 1.0) -> PubSubWorkerThread:
        """
        Run the listeners in a thread. For each message received on a
        subscribed channel or pattern the registered handlers will be invoked.

        To stop listening invoke :meth:`~coredis.commands.pubsub.PubSubWorkerThread.stop`
        on the returned instance
        """
        for channel, handler in self.channels.items():
            if handler is None:
                raise PubSubError(f"Channel: {channel!r} has no handler registered")

        for pattern, handler in self.patterns.items():
            if handler is None:
                raise PubSubError(f"Pattern: {pattern!r} has no handler registered")
        thread = PubSubWorkerThread(self, poll_timeout=poll_timeout)
        thread.start()

        return thread

    async def _consumer(self) -> None:
        while True:
            try:
                if self.subscribed:
                    if response := await self._retry_policy.call_with_retries(
                        lambda: self.parse_response(block=True),
                        failure_hook=self.reset_connections,
                    ):
                        self._message_queue.put_nowait(await self.handle_message(response))
                else:
                    await asyncio.sleep(0)
            except asyncio.CancelledError:
                break
            except ConnectionError:
                await asyncio.sleep(0)

    def _filter_ignored_messages(
        self,
        message: PubSubMessage | None,
        ignore_subscribe_messages: bool = False,
    ) -> PubSubMessage | None:
        if (
            message
            and b(message["type"]) in self.SUBUNSUB_MESSAGE_TYPES
            and (self.ignore_subscribe_messages or ignore_subscribe_messages)
        ):
            return None
        return message

    async def _execute(
        self,
        connection: BaseConnection,
        command: Callable[..., Awaitable[None]] | Callable[..., Awaitable[ResponseType]],
        *args: ValueT,
    ) -> ResponseType | None:
        try:
            return await command(*args)
        except asyncio.CancelledError:
            # do not retry if coroutine is cancelled
            if await connection.can_read():  # noqa
                connection.disconnect()
            raise

    def __await__(self) -> Generator[Any, None, Self]:
        return self.initialize().__await__()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> PubSubMessage:
        await self.initialize()
        while self.subscribed:
            if message := await self.get_message():
                return message
            else:
                continue
        raise StopAsyncIteration()

    async def __aenter__(self) -> Self:
        await self.initialize()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """
        Unsubscribe from any channels or patterns & close and return
        connections to the pool
        """
        if self.connection:
            await self.unsubscribe()
            await self.punsubscribe()
        self.close()

    def close(self) -> None:
        self.reset()

    def __del__(self) -> None:
        self.reset()

    def reset(self) -> None:
        """
        Clear subscriptions and disconnect and release any
        connection(s) back to the connection pool.

        :meta private:
        """
        if self.connection:
            self.connection.disconnect()
            self.connection.clear_connect_callbacks()
            self.connection_pool.release(self.connection)
            self.connection = None
        if self._consumer_task:
            try:
                self._consumer_task.cancel()
            except RuntimeError:  # noqa
                pass
            self._consumer_task = None

        self.channels = {}
        self.patterns = {}
        self.initialized = False

    async def reset_connections(self, exc: BaseException | None = None) -> None:
        pass


class PubSub(BasePubSub[AnyStr, "coredis.pool.ConnectionPool"]):
    """
    Pub/Sub implementation to be used with :class:`coredis.Redis`
    that is returned by :meth:`coredis.Redis.pubsub`

    An instance of this class is both an async context manager (to
    ensure that proper clean up of connections & subscriptions happens automatically)
    and an async iterator to consume messages from channels or patterns that it is
    subscribed to.

    Recommended use::

        client = coredis.Redis(decode_responses=True)
        async for message in client.pubsub(
          ignore_subscribe_messages=True,
          channels=["channel-1", "channel-2"]
        ):
            match message["channel"]:
                case "channel-1":
                    print("first", message["data"])
                case "channel-2":
                    print("second", message["data"])

    Or to explicitly subscribe::

        client = coredis.Redis(decode_responses=True)
        pubsub = client.pubsub()
        async with pubsub:
            await pubsub.subscribe("channel-1")
            assert (await pubsub.get_message())["channel"] == "channel-1"
            async for message in pubsub:
                print(message["data"])

    For more details see :ref:`handbook/pubsub:pubsub`
    """


class ClusterPubSub(BasePubSub[AnyStr, "coredis.pool.ClusterConnectionPool"]):
    """
    Pub/Sub implementation to be used with :class:`coredis.RedisCluster`
    that is returned by :meth:`coredis.RedisCluster.pubsub`

    .. note:: This implementation does not particularly benefit from having
       multiple nodes in a cluster as it subscribes to messages sent to channels
       using ``PUBLISH`` which in cluster mode results in the message being
       broadcasted to every node in the cluster. For this reason the subscribing
       client can subscribe to any node in the cluster to receive messages sent to
       any channel - which inherently limits the potential for scaling.

       :redis-version:`7.0` introduces the concept of Sharded Pub/Sub which
       can be accessed by instead using :meth:`coredis.RedisCluster.sharded_pubsub`
       which uses the implementation in :class:`coredis.commands.ShardedPubSub`.

    An instance of this class is both an async context manager (to
    ensure that proper clean up of connections & subscriptions happens automatically)
    and an async iterator to consume messages from channels or patterns that it is
    subscribed to.

    For more details see :ref:`handbook/pubsub:cluster pub/sub`

    """

    async def execute_command(
        self, command: bytes, *args: ValueT, **options: ValueT
    ) -> ResponseType | None:
        await self.initialize()
        assert self.connection
        return await self._execute(self.connection, self.connection.send_command, command, *args)

    async def initialize(self) -> Self:
        """
        Ensures the pubsub instance is ready to consume messages
        by establishing a connection to a random cluster node, setting up any
        initial channel or pattern subscriptions that were specified during
        instantiation and starting the consumer background task.

        The method can be called multiple times without any
        risk as it will skip initialization if the consumer is already
        initialized.

        .. important:: This method doesn't need to be called explicitly
           as it will always be called internally before any relevant
           documented interaction.

        :return: the instance itself
        """
        if not self.initialized:
            if self.connection is None:
                await self.reset_connections(None)
            self.initialized = True
            if self._initial_channel_subscriptions:
                await self.subscribe(**self._initial_channel_subscriptions)
            if self._initial_pattern_subscriptions:
                await self.psubscribe(**self._initial_pattern_subscriptions)
            if not self._consumer_task or self._consumer_task.done():
                self._consumer_task = asyncio.create_task(self._consumer())
        return self

    async def reset_connections(self, exc: BaseException | None = None) -> None:
        if self.connection:
            self.connection.disconnect()
            self.connection_pool.initialized = False

        await self.connection_pool.initialize()

        self.connection = await self.connection_pool.get_connection(b"pubsub")
        self.connection.register_connect_callback(self.on_connect)


@versionadded(version="3.6.0")
class ShardedPubSub(BasePubSub[AnyStr, "coredis.pool.ClusterConnectionPool"]):
    """
    Sharded Pub/Sub implementation to be used with :class:`coredis.RedisCluster`
    that is returned by :meth:`coredis.RedisCluster.sharded_pubsub`

    For details about the server architecture refer to the `Redis manual entry
    on Sharded Pub/sub <https://redis.io/docs/manual/pubsub/#sharded-pubsub>`__.

    New in :redis-version:`7.0.0`

    .. warning:: Sharded PubSub only supports subscription by channel and does
       **NOT** support pattern based subscriptions.

    An instance of this class is both an async context manager (to
    ensure that proper clean up of connections & subscriptions happens automatically)
    and an async iterator to consume messages from channels that it is subscribed to.

    For more details see :ref:`handbook/pubsub:sharded pub/sub`
    """

    PUBLISH_MESSAGE_TYPES = {
        PubSubMessageTypes.MESSAGE.value,
        PubSubMessageTypes.SMESSAGE.value,
    }
    SUBUNSUB_MESSAGE_TYPES = {
        PubSubMessageTypes.SSUBSCRIBE.value,
        PubSubMessageTypes.SUNSUBSCRIBE.value,
    }
    UNSUBSCRIBE_MESSAGE_TYPES = {PubSubMessageTypes.SUNSUBSCRIBE.value}

    def __init__(
        self,
        connection_pool: coredis.pool.ClusterConnectionPool,
        ignore_subscribe_messages: bool = False,
        retry_policy: RetryPolicy | None = None,
        read_from_replicas: bool = False,
        channels: Parameters[StringT] | None = None,
        channel_handlers: Mapping[StringT, SubscriptionCallback] | None = None,
    ):
        self.shard_connections: dict[str, Connection] = {}
        self.channel_connection_mapping: dict[StringT, Connection] = {}
        self.pending_tasks: dict[str, asyncio.Task[ResponseType]] = {}
        self.read_from_replicas = read_from_replicas
        super().__init__(
            connection_pool,
            ignore_subscribe_messages,
            retry_policy,
            channels=channels,
            channel_handlers=channel_handlers,
        )

    async def subscribe(
        self,
        *channels: StringT,
        **channel_handlers: SubscriptionCallback | None,
    ) -> None:
        """
        :param channels: The shard channels to subscribe to.
        :param channel_handlers: Channels supplied as keyword arguments expect
         a channel name as the key and a callable as the value. A channel's
         callable will be invoked automatically when a message is received on
         that channel rather than producing a message via :meth:`listen` or
         :meth:`get_message`.
        """

        await self.initialize()
        new_channels: MutableMapping[StringT, SubscriptionCallback | None] = {}
        new_channels.update(dict.fromkeys(map(self.encode, channels)))

        for channel, handler in channel_handlers.items():
            new_channels[self.encode(channel)] = handler
        for new_channel in new_channels.keys():
            await self.execute_command(CommandName.SSUBSCRIBE, new_channel, sharded=True)
        self.channels.update(new_channels)

    async def unsubscribe(self, *channels: StringT) -> None:
        """
        :param channels: The shard channels to unsubscribe from. If None are provided,
         this will effectively unsubscribe the client from all channels
         previously subscribed to.
        """

        for channel in channels or list(self.channels.keys()):
            await self.execute_command(CommandName.SUNSUBSCRIBE, channel, sharded=True)

    async def psubscribe(
        self,
        *patterns: StringT,
        **pattern_handlers: SubscriptionCallback | None,
    ) -> None:
        """
        Not available in sharded pubsub

        :meta private:
        """
        raise NotImplementedError("Sharded PubSub does not support subscription by pattern")

    async def punsubscribe(self, *patterns: StringT) -> None:
        """
        Not available in sharded pubsub

        :meta private:
        """
        raise NotImplementedError("Sharded PubSub does not support subscription by pattern")

    async def execute_command(
        self, command: bytes, *args: ValueT, **options: ValueT
    ) -> ResponseType | None:
        await self.initialize()

        assert isinstance(args[0], (bytes, str))
        channel = nativestr(args[0])
        slot = hash_slot(b(channel))
        node = self.connection_pool.nodes.node_from_slot(slot)
        if node and node.node_id:
            key = node.node_id
            if self.shard_connections.get(key) is None:
                self.shard_connections[key] = await self.connection_pool.get_connection(
                    b"pubsub",
                    channel=channel,
                    node_type="replica" if self.read_from_replicas else "primary",
                )
                # register a callback that re-subscribes to any channels we
                # were listening to when we were disconnected
                self.shard_connections[key].register_connect_callback(self.on_connect)

            self.channel_connection_mapping[args[0]] = self.shard_connections[key]
            assert self.shard_connections[key]
            return await self._execute(
                self.shard_connections[key],
                self.shard_connections[key].send_command,
                command,
                *args,
            )
        raise PubSubError(f"Unable to determine shard for channel {args[0]!r}")

    async def initialize(self) -> Self:
        """
        Ensures the sharded pubsub instance is ready to consume messages
        by ensuring the connection pool is initialized, setting up any
        initial channel subscriptions that were specified during
        instantiation and starting the consumer background task.

        The method can be called multiple times without any
        risk as it will skip initialization if the consumer is already
        initialized.

        .. important:: This method doesn't need to be called explicitly
           as it will always be called internally before any relevant
           documented interaction.

        :return: the instance itself
        """
        if not self.initialized:
            await self.connection_pool.initialize()
            self.initialized = True
            if self._initial_channel_subscriptions:
                await self.subscribe(**self._initial_channel_subscriptions)
            if not self._consumer_task or self._consumer_task.done():
                self._consumer_task = asyncio.create_task(self._consumer())
        return self

    async def reset_connections(self, exc: BaseException | None = None) -> None:
        for connection in self.shard_connections.values():
            connection.disconnect()
            connection.clear_connect_callbacks()
            self.connection_pool.release(connection)
        self.shard_connections.clear()
        for _, task in self.pending_tasks.items():
            if not task.done():
                task.cancel()
                with suppress(CancelledError):
                    await task
        self.pending_tasks.clear()
        self.connection_pool.disconnect()
        self.connection_pool.reset()
        self.connection_pool.initialized = False
        await self.connection_pool.initialize()
        for channel in self.channels:
            slot = hash_slot(b(channel))
            node = self.connection_pool.nodes.node_from_slot(slot)
            if node and node.node_id:
                key = node.node_id
                self.shard_connections[key] = await self.connection_pool.get_connection(
                    b"pubsub",
                    channel=channel,
                    node_type="replica" if self.read_from_replicas else "primary",
                )
                # register a callback that re-subscribes to any channels we
                # were listening to when we were disconnected
                self.shard_connections[key].register_connect_callback(self.on_connect)
                self.channel_connection_mapping[channel] = self.shard_connections[key]

    async def parse_response(
        self, block: bool = True, timeout: float | None = None
    ) -> ResponseType:
        if not self.shard_connections:
            raise RuntimeError(
                "pubsub connection not set: did you forget to call subscribe() or psubscribe()?"
            )
        result = None
        # Check any stashed results first.
        if self.pending_tasks:
            for node_id, task in list(self.pending_tasks.items()):
                self.pending_tasks.pop(node_id)
                if task.done():
                    result = task.result()
                    break
                else:
                    done, pending = await asyncio.wait(
                        [task],
                        timeout=0.001,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if done:
                        result = done.pop().result()
                        break
                    else:
                        task.cancel()
                        with suppress(CancelledError):
                            await task
        # If there were no pending results check the shards
        if not result:
            broken_connections = [c for c in self.shard_connections.values() if not c.is_connected]
            if broken_connections:
                for connection in broken_connections:
                    try:
                        await connection.connect()
                    except:  # noqa
                        raise ConnectionError("Shard connections not stable")
            tasks: dict[str, asyncio.Task[ResponseType]] = {
                node_id: asyncio.create_task(
                    connection.fetch_push_message(
                        push_message_types=self.SUBUNSUB_MESSAGE_TYPES | self.PUBLISH_MESSAGE_TYPES,
                    ),
                )
                for node_id, connection in self.shard_connections.items()
                if node_id not in self.pending_tasks
            }
            if tasks:
                done, pending = await asyncio.wait(
                    tasks.values(),
                    timeout=timeout if (timeout and timeout > 0) else None,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if done:
                    done_task = done.pop()
                    result = done_task.result()

                # Stash any other tasks for the next iteration
                for task in list(done) + list(pending):
                    for node_id, scheduled in tasks.items():
                        if task == scheduled:
                            self.pending_tasks[node_id] = task
        return result

    async def on_connect(self, connection: BaseConnection) -> None:
        """
        Re-subscribe to any channels previously subscribed to

        :meta private:
        """
        for channel, handler in self.channels.items():
            if self.channel_connection_mapping[channel] == connection:
                await self.subscribe(
                    **{
                        (
                            channel.decode(self.connection_pool.encoding)
                            if isinstance(channel, bytes)
                            else channel
                        ): handler
                    }
                )

    def reset(self) -> None:
        for connection in self.shard_connections.values():
            connection.disconnect()
            connection.clear_connect_callbacks()
            self.connection_pool.release(connection)
        for _, task in self.pending_tasks.items():
            task.cancel()
        self.pending_tasks.clear()
        self.shard_connections.clear()
        self.channels = {}
        self.patterns = {}
        self.initialized = False

    async def aclose(self) -> None:
        """
        Unsubscribe from any channels & close and return
        connections to the pool
        """
        if self.shard_connections:
            await self.unsubscribe()
        self.close()


class PubSubWorkerThread(threading.Thread):
    def __init__(
        self,
        pubsub: BasePubSub[Any, Any],
        poll_timeout: float = 1.0,
    ):
        super().__init__()
        self._pubsub = pubsub
        self._poll_timeout = poll_timeout
        self._running = False
        self._loop = asyncio.get_running_loop()
        self._future: Future[None] | None = None

    async def _run(self) -> None:
        async with aclosing(self._pubsub) as pubsub:
            try:
                while pubsub.subscribed:
                    await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=self._poll_timeout
                    )
            except CancelledError:
                self._running = False

    def run(self) -> None:
        """
        :meta private:
        """
        if self._running:
            return
        self._running = True
        self._future = asyncio.run_coroutine_threadsafe(self._run(), self._loop)

    def stop(self) -> None:
        """
        Stop the worker thread from processing any more messages
        """
        if self._future:
            self._future.cancel()
