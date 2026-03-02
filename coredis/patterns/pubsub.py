from __future__ import annotations

import inspect
import math
import time
from collections import defaultdict
from contextlib import AsyncExitStack, asynccontextmanager
from typing import TYPE_CHECKING, Any, cast

from anyio import (
    TASK_STATUS_IGNORED,
    AsyncContextManagerMixin,
    Event,
    create_memory_object_stream,
    create_task_group,
    move_on_after,
    sleep,
)
from anyio.abc import TaskStatus
from deprecated.sphinx import versionadded, versionchanged

from coredis._utils import b, hash_slot, nativestr
from coredis.commands.constants import CommandName
from coredis.connection import BaseConnection, ClusterConnection, TCPLocation
from coredis.constants.pubsub import (
    PUBLISH_MESSAGE_TYPES,
    SUBSCRIBE_MESSAGE_TYPES,
    SUBUNSUB_MESSAGE_TYPES,
    UNSUBSCRIBE_MESSAGE_TYPES,
    PubSubMessageTypes,
)
from coredis.exceptions import ConnectionError, PubSubError
from coredis.response.types import PubSubMessage
from coredis.retry import (
    CompositeRetryPolicy,
    ConstantRetryPolicy,
    ExponentialBackoffRetryPolicy,
    NoRetryPolicy,
    RetryPolicy,
)
from coredis.typing import (
    AnyStr,
    AsyncGenerator,
    Awaitable,
    Callable,
    Generic,
    Mapping,
    MutableMapping,
    Parameters,
    RedisValueT,
    ResponseType,
    Self,
    StringT,
    TypeVar,
)

if TYPE_CHECKING:
    import coredis.pool

T = TypeVar("T")
PoolT = TypeVar("PoolT", bound="coredis.pool.BaseConnectionPool[Any]")
#: Callables for message handler callbacks. The callbacks
#:  can be sync or async.
SubscriptionCallback = Callable[[PubSubMessage], Awaitable[None]] | Callable[[PubSubMessage], None]


class BasePubSub(AsyncContextManagerMixin, Generic[AnyStr, PoolT]):
    channels: MutableMapping[StringT, SubscriptionCallback | None]
    patterns: MutableMapping[StringT, SubscriptionCallback | None]

    def __init__(
        self,
        connection_pool: PoolT,
        ignore_subscribe_messages: bool = False,
        retry_policy: RetryPolicy | None = CompositeRetryPolicy(
            ExponentialBackoffRetryPolicy(
                (ConnectionError,), retries=None, base_delay=0.1, max_delay=16, jitter=True
            ),
            ConstantRetryPolicy((TimeoutError,), retries=2, delay=0.1),
        ),
        channels: Parameters[StringT] | None = None,
        channel_handlers: Mapping[StringT, SubscriptionCallback] | None = None,
        patterns: Parameters[StringT] | None = None,
        pattern_handlers: Mapping[StringT, SubscriptionCallback] | None = None,
        subscription_timeout: float = 1,
        max_idle_seconds: float = 15,
    ):
        """
        :param connection_pool: Connection pool used to acquire
         a connection to use for the pubsub consumer
        :param ignore_subscribe_messages: Whether to skip subscription
         acknowledgement messages
        :param retry_policy: An explicit retry policy to use in the subscriber.
        :param channels: channels that the constructed Pubsub instance should
         automatically subscribe to
        :param channel_handlers: Mapping of channels to automatically subscribe to
         and the associated handlers that will be invoked when a message is received
         on the specific channel.
        :param patterns: patterns that the constructed Pubsub instance should
         automatically subscribe to
        :param pattern_handlers: Mapping of patterns to automatically subscribe to
         and the associated handlers that will be invoked when a message is received
         on channel matching the pattern.
        :param subscription_timeout: Maximum amount of time in seconds to wait for
         acknowledgement of subscriptions.
        :param max_idle_seconds: Maximum duration (in seconds) to tolerate no
         messages from the server before performing a keepalive check with a
         ``PING``.
        """
        self.connection_pool = connection_pool
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self._connection: coredis.BaseConnection | None = None
        self._retry_policy = retry_policy or NoRetryPolicy()
        self._initial_channel_subscriptions = {
            **{nativestr(channel): None for channel in channels or []},
            **{nativestr(k): v for k, v in (channel_handlers or {}).items()},
        }
        self._initial_pattern_subscriptions = {
            **{nativestr(pattern): None for pattern in patterns or []},
            **{nativestr(k): v for k, v in (pattern_handlers or {}).items()},
        }
        self._send_stream, self._receive_stream = create_memory_object_stream[PubSubMessage | None](
            math.inf
        )
        self._subscribed = Event()
        # TODO: there might be some benefit with regards to cleanup
        #  to extend the same functionality to unsubscribe but the
        #  it's not currently obvious why.
        self._subscription_waiters: dict[StringT, list[Event]] = defaultdict(list)
        self._subscription_timeout: float = subscription_timeout

        self._last_checkin: float = 0
        self._max_idle_seconds = max_idle_seconds
        self.channels = {}
        self.patterns = {}

    @property
    def connection(self) -> BaseConnection:
        if not self._connection:
            raise Exception("Connection not initialized correctly!")
        return self._connection

    @property
    def subscribed(self) -> bool:
        """Indicates if there are subscriptions to any channels or patterns"""
        return bool(self.channels or self.patterns)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> PubSubMessage:
        while self._subscribed.is_set():
            if message := await self.get_message():
                return message
        raise StopAsyncIteration()

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        async with (
            self._send_stream,
            self._receive_stream,
            create_task_group() as tg,
        ):
            await tg.start(self.run)
            # initialize subscriptions
            if self._initial_channel_subscriptions:
                await self.subscribe(**self._initial_channel_subscriptions)
            if self._initial_pattern_subscriptions:
                await self.psubscribe(**self._initial_pattern_subscriptions)
            yield self
            # TODO: evaluate whether a call to reset is necessary
            #  at the moment this is only working as a side effect
            #  of only supporting RESP3
            await self.unsubscribe()
            await self.punsubscribe()
            self.channels.clear()
            self.patterns.clear()
            self._subscription_waiters.clear()
            self._current_scope.cancel()

    async def run(self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED) -> None:
        started = False

        async def _run() -> None:
            nonlocal started
            async with self.connection_pool.acquire() as self._connection:
                async with create_task_group() as tg:
                    self._current_scope = tg.cancel_scope
                    tg.start_soon(self._consumer)
                    tg.start_soon(self._keepalive)
                    if not started:
                        task_status.started()
                        started = True
                    else:  # resubscribe
                        if self.channels:
                            await self.subscribe(*self.channels.keys())
                        if self.patterns:
                            await self.psubscribe(*self.patterns.keys())

        await self._retry_policy.call_with_retries(_run)

    async def _keepalive(self) -> None:
        while True:
            if (idle := time.monotonic() - self._last_checkin) >= self._max_idle_seconds:
                if self._connection and await self._connection.create_request(CommandName.PING) in {
                    b"PONG",
                    "PONG",
                }:
                    self._last_checkin = time.monotonic()
            await sleep(max(1, self._max_idle_seconds - idle))

    async def _consumer(self) -> None:
        while True:
            async for response in self.connection.push_messages:
                self._last_checkin = time.monotonic()
                msg = await self._handle_message(response)
                self._send_stream.send_nowait(msg)

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
        :meth:`get_message` or the iterator.
        """
        new_patterns: MutableMapping[StringT, SubscriptionCallback | None] = {}
        new_patterns.update(dict.fromkeys(map(self._encode, patterns)))

        for handled_pattern, handler in pattern_handlers.items():
            new_patterns[self._encode(handled_pattern)] = handler

        waiters: dict[StringT, Event] = {pattern: Event() for pattern in new_patterns}
        for pattern, event in waiters.items():
            self._subscription_waiters[pattern].append(event)

        await self._execute_command(CommandName.PSUBSCRIBE, *new_patterns.keys())
        await self._ensure_subscriptions(waiters)
        # update the patterns dict AFTER we send the command. we don't want to
        # subscribe twice to these patterns, once for the command and again
        # for the reconnection.
        self.patterns.update(new_patterns)

    async def punsubscribe(self, *patterns: StringT) -> None:
        """
        Unsubscribes from the supplied patterns. If empty, unsubscribe from
        all patterns.
        """
        await self._execute_command(CommandName.PUNSUBSCRIBE, *patterns)

    async def subscribe(
        self,
        *channels: StringT,
        **channel_handlers: SubscriptionCallback | None,
    ) -> None:
        """
        Subscribes to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via :meth:`get_message`
        or the iterator.
        """

        new_channels: MutableMapping[StringT, SubscriptionCallback | None] = {}
        new_channels.update(dict.fromkeys(map(self._encode, channels)))

        for handled_channel, handler in channel_handlers.items():
            new_channels[self._encode(handled_channel)] = handler

        waiters: dict[StringT, Event] = {channel: Event() for channel in new_channels}

        for channel, event in waiters.items():
            self._subscription_waiters[channel].append(event)

        await self._execute_command(CommandName.SUBSCRIBE, *new_channels.keys())

        await self._ensure_subscriptions(waiters)
        self.channels.update(new_channels)

    async def unsubscribe(self, *channels: StringT) -> None:
        """
        Unsubscribes from the supplied channels. If empty, unsubscribe from
        all channels
        """

        await self._execute_command(CommandName.UNSUBSCRIBE, *channels)

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

        with move_on_after(timeout):
            return self._filter_ignored_messages(
                await self._receive_stream.receive(), ignore_subscribe_messages
            )
        return None

    def _encode(self, value: StringT) -> StringT:
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

    async def _execute_command(self, command: bytes, *args: RedisValueT) -> None:
        """
        Executes a publish/subscribe command

        TODO: evaluate whether this should remain async
        """
        self.connection.send_command(command, *args)

    async def _handle_message(self, response: list[ResponseType]) -> PubSubMessage | None:
        """
        Parses a pub/sub message. If the channel or pattern was subscribed to
        with a message handler, the handler is invoked instead of a parsed
        message being returned.

        :meta private:
        """
        message_type = b(response[0])
        message_type_str = nativestr(response[0])
        message: PubSubMessage

        if message_type in SUBUNSUB_MESSAGE_TYPES:
            target = cast(StringT, response[1])
            message = PubSubMessage(
                type=message_type_str,
                pattern=target if message_type[0] == ord(b"p") else None,
                # This field is populated in all cases for backward compatibility
                # as older versions were incorrectly populating the channel
                # with the pattern on psubscribe/punsubscribe responses.
                channel=target,
                data=cast(int, response[2]),
            )
            if message_type in SUBSCRIBE_MESSAGE_TYPES:
                if waiters := self._subscription_waiters.get(target, []):
                    waiters.pop(-1).set()

        elif message_type in PUBLISH_MESSAGE_TYPES:
            if message_type == PubSubMessageTypes.PMESSAGE:
                message = PubSubMessage(
                    type="pmessage",
                    pattern=cast(StringT, response[1]),
                    channel=cast(StringT, response[2]),
                    data=cast(StringT, response[3]),
                )
            else:
                message = PubSubMessage(
                    type="message",
                    pattern=None,
                    channel=cast(StringT, response[1]),
                    data=cast(StringT, response[2]),
                )
        else:
            raise PubSubError(f"Unknown message type {message_type_str}")  # noqa

        # if this is an unsubscribe message, remove it from memory
        if message_type in UNSUBSCRIBE_MESSAGE_TYPES:
            if message_type == PubSubMessageTypes.PUNSUBSCRIBE:
                subscribed_dict = self.patterns
            else:
                subscribed_dict = self.channels
            subscribed_dict.pop(message["channel"], None)

        if message_type in PUBLISH_MESSAGE_TYPES:
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
        if not (self.channels or self.patterns):
            self._subscribed = Event()

        return message

    def _filter_ignored_messages(
        self,
        message: PubSubMessage | None,
        ignore_subscribe_messages: bool = False,
    ) -> PubSubMessage | None:
        if (
            message
            and b(message["type"]) in SUBUNSUB_MESSAGE_TYPES
            and (self.ignore_subscribe_messages or ignore_subscribe_messages)
        ):
            return None
        return message

    async def _ensure_subscriptions(self, waiters: dict[StringT, Event]) -> None:
        with move_on_after(self._subscription_timeout) as cancel_scope:
            for target, event in waiters.items():
                await event.wait()
        if cancel_scope.cancelled_caught:
            raise TimeoutError(f"Subscription timed out after {self._subscription_timeout} seconds")

        self._subscribed.set()


@versionchanged(
    version="6.0.0",
    reason="The class supports the async context manager protocol and must always be used as such",
)
class PubSub(BasePubSub[AnyStr, "coredis.pool.ConnectionPool[BaseConnection]"]):
    """
    Pub/Sub implementation to be used with :class:`~coredis.Redis`
    that is returned by :meth:`~coredis.Redis.pubsub`

    An instance of this class is both an async context manager (to
    ensure that proper clean up of connections & subscriptions happens automatically)
    and an async iterator to consume messages from channels or patterns that it is
    subscribed to.

    Recommended use::

        async with coredis.Redis(decode_responses=True) as client:
            async with client.pubsub(
              ignore_subscribe_messages=True,
              channels=["channel-1", "channel-2"]
            ) as pubsub:
                async for message in pubsub:
                    match message["channel"]:
                        case "channel-1":
                            print("first", message["data"])
                        case "channel-2":
                            print("second", message["data"])

    Or to explicitly subscribe::

        async with coredis.Redis(decode_responses=True) as client:
            async with client.pubsub() as pubsub:
                await pubsub.subscribe("channel-1")
                assert (await pubsub.get_message())["channel"] == "channel-1"
                async for message in pubsub:
                    print(message["data"])

    For more details see :ref:`handbook/pubsub:pubsub`
    """


@versionchanged(
    version="6.0.0",
    reason="The class supports the async context manager protocol and must always be used as such",
)
class ClusterPubSub(BasePubSub[AnyStr, "coredis.pool.ClusterConnectionPool"]):
    """
    Pub/Sub implementation to be used with :class:`~coredis.RedisCluster`
    that is returned by :meth:`~coredis.RedisCluster.pubsub`

    .. note:: This implementation does not particularly benefit from having
       multiple nodes in a cluster as it subscribes to messages sent to channels
       using ``PUBLISH`` which in cluster mode results in the message being
       broadcasted to every node in the cluster. For this reason the subscribing
       client can subscribe to any node in the cluster to receive messages sent to
       any channel - which inherently limits the potential for scaling.

       :redis-version:`7.0` introduces the concept of Sharded Pub/Sub which
       can be accessed by instead using :meth:`~coredis.RedisCluster.sharded_pubsub`
       which uses the implementation in :class:`~coredis.commands.ShardedPubSub`.

    An instance of this class is both an async context manager (to
    ensure that proper clean up of connections & subscriptions happens automatically)
    and an async iterator to consume messages from channels or patterns that it is
    subscribed to.

    For more details see :ref:`handbook/pubsub:cluster pub/sub`

    """

    async def run(self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED) -> None:
        started = False

        async def _run() -> None:
            nonlocal started
            async with self.connection_pool.acquire() as self._connection:
                async with create_task_group() as tg:
                    self._current_scope = tg.cancel_scope
                    tg.start_soon(self._consumer)
                    tg.start_soon(self._keepalive)
                    if not started:
                        task_status.started()
                        started = True
                    else:  # resubscribe
                        if self.channels:
                            await self.subscribe(*self.channels.keys())
                        if self.patterns:
                            await self.psubscribe(*self.patterns.keys())

        await self._retry_policy.call_with_retries(_run)


@versionchanged(
    version="6.0.0",
    reason="The class supports the async context manager protocol and must always be used as such",
)
@versionadded(version="3.6.0")
class ShardedPubSub(BasePubSub[AnyStr, "coredis.pool.ClusterConnectionPool"]):
    """
    Sharded Pub/Sub implementation to be used with :class:`~coredis.RedisCluster`
    that is returned by :meth:`~coredis.RedisCluster.sharded_pubsub`

    For details about the server architecture refer to the `Redis manual entry
    on Sharded Pub/sub <https://redis.io/docs/develop/pubsub/#sharded-pubsub>`__

    New in :redis-version:`7.0.0`

    .. warning:: Sharded PubSub only supports subscription by channel and does
       **NOT** support pattern based subscriptions.

    An instance of this class is both an async context manager (to
    ensure that proper clean up of connections & subscriptions happens automatically)
    and an async iterator to consume messages from channels that it is subscribed to.

    For more details see :ref:`handbook/pubsub:sharded pub/sub`
    """

    def __init__(
        self,
        connection_pool: coredis.pool.ClusterConnectionPool,
        ignore_subscribe_messages: bool = False,
        retry_policy: RetryPolicy | None = None,
        read_from_replicas: bool = False,
        channels: Parameters[StringT] | None = None,
        channel_handlers: Mapping[StringT, SubscriptionCallback] | None = None,
        subscription_timeout: float = 1,
        max_idle_seconds: float = 15,
    ):
        """
        :param connection_pool: Connection pool used to acquire
         a connection to use for the pubsub consumer
        :param ignore_subscribe_messages: Whether to skip subscription
         acknowledgement messages
        :param retry_policy: An explicit retry policy to use in the subscriber.
        :param channels: channels that the constructed Pubsub instance should
         automatically subscribe to
        :param channel_handlers: Mapping of channels to automatically subscribe to
         and the associated handlers that will be invoked when a message is received
         on the specific channel.
        :param subscription_timeout: Maximum amount of time in seconds to wait for
         acknowledgement of subscriptions.
        :param max_idle_seconds: Maximum duration (in seconds) to tolerate no
         messages from the cluster before performing a keepalive check with a
         `PING``.
        """
        self.shard_connections: dict[str, BaseConnection] = {}
        self.node_channel_mapping: dict[str, list[StringT]] = {}
        self.read_from_replicas = read_from_replicas
        self._last_checkins: dict[TCPLocation, float] = defaultdict(lambda: 0)
        super().__init__(
            connection_pool,
            ignore_subscribe_messages,
            retry_policy,
            channels=channels,
            channel_handlers=channel_handlers,
            subscription_timeout=subscription_timeout,
            max_idle_seconds=max_idle_seconds,
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
         that channel rather than producing a message via :meth:`get_message`
         or the iterator.
        """

        new_channels: MutableMapping[StringT, SubscriptionCallback | None] = {}
        new_channels.update(dict.fromkeys(map(self._encode, channels)))

        for handled_channel, handler in channel_handlers.items():
            new_channels[self._encode(handled_channel)] = handler

        waiters: dict[StringT, Event] = {channel: Event() for channel in new_channels}

        for channel, event in waiters.items():
            self._subscription_waiters[channel].append(event)
        for channel in new_channels:
            await self._execute_command(CommandName.SSUBSCRIBE, channel)
        await self._ensure_subscriptions(waiters)
        self.channels.update(new_channels)

    async def unsubscribe(self, *channels: StringT) -> None:
        """
        :param channels: The shard channels to unsubscribe from. If None are provided,
         this will effectively unsubscribe the client from all channels
         previously subscribed to.
        """

        for channel in channels or list(self.channels.keys()):
            await self._execute_command(CommandName.SUNSUBSCRIBE, channel)

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

    async def _execute_command(self, command: bytes, *args: RedisValueT) -> None:
        assert isinstance(args[0], (bytes, str))
        channel = nativestr(args[0])
        slot = hash_slot(b(channel))
        node = self.connection_pool.cluster_layout.node_for_slot(slot)
        if node and node.node_id:
            key = node.node_id
            self.shard_connections[key].send_command(command, *args)
            return
        raise PubSubError(f"Unable to determine shard for channel {args[0]!r}")

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncGenerator[Self]:
        async with self._send_stream, self._receive_stream, create_task_group() as tg:
            await tg.start(self.run)
            # initialize subscriptions
            if self._initial_channel_subscriptions:
                await self.subscribe(**self._initial_channel_subscriptions)
            yield self
            await self.unsubscribe()
            self.channels.clear()
            self._current_scope.cancel()
            self._reset()

    async def run(self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED) -> None:
        started = False

        async def _run() -> None:
            nonlocal started
            stack = AsyncExitStack()
            self.shard_connections = {
                node.node_id: await stack.enter_async_context(
                    self.connection_pool.acquire(node=node)
                )
                for node in self.connection_pool.cluster_layout.primaries
                if node.node_id
            }
            async with create_task_group() as tg:
                self._current_scope = tg.cancel_scope
                [
                    tg.start_soon(self._shard_consumer, connection)
                    for connection in self.shard_connections.values()
                ]
                [
                    tg.start_soon(self._shard_keepalive, connection)
                    for connection in self.shard_connections.values()
                ]
                if not started:
                    task_status.started()
                    started = True
                elif self.channels:  # resubscribe
                    await self.subscribe(*self.channels.keys())

        await self._retry_policy.call_with_retries(_run)

    async def _shard_consumer(self, connection: BaseConnection) -> None:
        assert isinstance(connection, ClusterConnection)
        async for message in connection.push_messages:
            self._last_checkins[connection.location] = time.monotonic()
            self._send_stream.send_nowait(await self._handle_message(message))

    async def _shard_keepalive(self, connection: BaseConnection) -> None:
        assert isinstance(connection, ClusterConnection)
        while True:
            if (
                idle := time.monotonic() - self._last_checkins[connection.location]
            ) >= self._max_idle_seconds:
                if await connection.create_request(CommandName.PING) in {b"PONG", "PONG"}:
                    self._last_checkins[connection.location] = time.monotonic()
            await sleep(max(1, self._max_idle_seconds - idle))

    def _reset(self) -> None:
        self.shard_connections.clear()
        self.channels = {}
        self.patterns = {}
        self.initialized = False
        self._subscription_waiters.clear()
        self._subscribed = Event()
