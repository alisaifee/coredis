from __future__ import annotations

import asyncio
import inspect
import threading
from asyncio import CancelledError
from concurrent.futures import Future
from contextlib import suppress
from functools import partial
from typing import TYPE_CHECKING, Any, cast

from deprecated.sphinx import versionadded

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
    Dict,
    Generic,
    List,
    MutableMapping,
    Optional,
    ResponsePrimitive,
    ResponseType,
    StringT,
    TypeVar,
    Union,
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
SubscriptionCallback = Union[
    Callable[[PubSubMessage], Awaitable[None]], Callable[[PubSubMessage], None]
]


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

    channels: MutableMapping[StringT, Optional[SubscriptionCallback]]
    patterns: MutableMapping[StringT, Optional[SubscriptionCallback]]

    def __init__(
        self,
        connection_pool: PoolT,
        ignore_subscribe_messages: bool = False,
        retry_policy: Optional[RetryPolicy] = CompositeRetryPolicy(
            ConstantRetryPolicy((ConnectionError,), 3, 0.1),
            ConstantRetryPolicy((TimeoutError,), 2, 0.1),
        ),
    ):
        self.connection_pool = connection_pool
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.connection: Optional[coredis.connection.Connection] = None
        self._retry_policy = retry_policy or NoRetryPolicy()
        self.reset()

    async def _ensure_encoding(self) -> None:
        if hasattr(self, "encoding"):
            return

        await self.connection_pool.initialize()
        conn = await self.connection_pool.get_connection(b"pubsub")

        try:
            self.encoding = conn.encoding
            self.decode_responses = conn.decode_responses
        finally:
            self.connection_pool.release(conn)

    def __del__(self) -> None:
        self.reset()

    def reset(self) -> None:
        """
        Clear subscriptions and disconnect and release any
        connection(s) back to the connection pool.
        """
        if self.connection:
            self.connection.disconnect()
            self.connection.clear_connect_callbacks()
            self.connection_pool.release(self.connection)
            self.connection = None
        self.channels = {}
        self.patterns = {}

    def close(self) -> None:
        self.reset()

    async def reset_connections(self, exc: Optional[BaseException] = None) -> None:
        pass

    async def on_connect(self, connection: BaseConnection) -> None:
        """
        Re-subscribe to any channels and patterns previously subscribed to

        :meta private:
        """

        if self.channels:
            await self.subscribe(
                **{
                    k.decode(self.encoding) if isinstance(k, bytes) else k: v
                    for k, v in self.channels.items()
                }
            )

        if self.patterns:
            await self.psubscribe(
                **{
                    k.decode(self.encoding) if isinstance(k, bytes) else k: v
                    for k, v in self.patterns.items()
                }
            )

    def encode(self, value: StringT) -> StringT:
        """
        Encodes the value so that it's identical to what we'll read off the
        connection

        :meta private:
        """

        if self.decode_responses and isinstance(value, bytes):
            value = nativestr(value, self.encoding)
        elif not self.decode_responses and isinstance(value, str):
            value = b(value, self.encoding)

        return value

    @property
    def subscribed(self) -> bool:
        """Indicates if there are subscriptions to any channels or patterns"""

        return bool(self.channels or self.patterns)

    async def execute_command(
        self, command: bytes, *args: ValueT, **options: ValueT
    ) -> Optional[ResponseType]:
        """
        Executes a publish/subscribe command

        :meta private:
        """

        # NOTE: don't parse the response in this function -- it could pull a
        # legitimate message off the stack if the connection is already
        # subscribed to one or more channels

        await self._ensure_encoding()

        if self.connection is None:
            self.connection = await self.connection_pool.get_connection()
            self.connection.register_connect_callback(self.on_connect)
        assert self.connection
        return await self._execute(
            self.connection, self.connection.send_command, command, *args
        )

    async def _execute(
        self,
        connection: BaseConnection,
        command: Union[
            Callable[..., Awaitable[None]], Callable[..., Awaitable[ResponseType]]
        ],
        *args: ValueT,
    ) -> Optional[ResponseType]:
        try:
            return await command(*args)
        except asyncio.CancelledError:
            # do not retry if coroutine is cancelled
            if await connection.can_read():  # noqa
                connection.disconnect()
            raise

    async def parse_response(
        self, block: bool = True, timeout: Optional[float] = None
    ) -> ResponseType:
        """
        Parses the response from a publish/subscribe command

        :meta private:
        """
        assert self.connection
        coro = self._execute(
            self.connection,
            partial(
                self.connection.fetch_push_message,
                push_message_types=self.SUBUNSUB_MESSAGE_TYPES
                | self.PUBLISH_MESSAGE_TYPES,
            ),
        )

        try:
            return await asyncio.wait_for(
                coro, timeout if (timeout and timeout > 0) else None
            )
        except asyncio.TimeoutError:
            return None

    async def psubscribe(
        self,
        *patterns: StringT,
        **pattern_handlers: Optional[SubscriptionCallback],
    ) -> None:
        """
        Subscribes to channel patterns. Patterns supplied as keyword arguments
        expect a pattern name as the key and a callable as the value. A
        pattern's callable will be invoked automatically when a message is
        received on that pattern rather than producing a message via
        :meth:`listen`.
        """
        await self._ensure_encoding()

        new_patterns: MutableMapping[StringT, Optional[SubscriptionCallback]] = {}
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
        Unsubscribes from the supplied patterns. If empy, unsubscribe from
        all patterns.
        """
        await self._ensure_encoding()
        await self.execute_command(CommandName.PUNSUBSCRIBE, *patterns)

    async def subscribe(
        self,
        *channels: StringT,
        **channel_handlers: Optional[SubscriptionCallback],
    ) -> None:
        """
        Subscribes to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via :meth:`listen` or
        :meth:`get_message`.
        """

        await self._ensure_encoding()

        new_channels: MutableMapping[StringT, Optional[SubscriptionCallback]] = {}
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

        await self._ensure_encoding()
        await self.execute_command(CommandName.UNSUBSCRIBE, *channels)

    async def listen(self) -> Optional[PubSubMessage]:
        """
        Listens for messages on channels this client has been subscribed to
        """

        if self.subscribed:
            response = await self.parse_response(block=True)
            if response:
                return await self.handle_message(response)
        return None

    async def get_message(
        self,
        ignore_subscribe_messages: bool = False,
        timeout: Optional[Union[int, float]] = None,
    ) -> Optional[PubSubMessage]:
        """
        Gets the next message if one is available, otherwise None.

        :param ignore_subscribe_messages: Whether to skip subscription
         acknowledgement messages
        :param timeout: Number of seconds to wait for a message to be available
         on the connection. If the ``None`` the command will block forever.
        """

        response = await self._retry_policy.call_with_retries(
            lambda: self.parse_response(block=False, timeout=timeout),
            failure_hook=self.reset_connections,
        )

        if response:
            return await self.handle_message(response, ignore_subscribe_messages)

        return None

    async def handle_message(
        self, response: ResponseType, ignore_subscribe_messages: bool = False
    ) -> Optional[PubSubMessage]:
        """
        Parses a pub/sub message. If the channel or pattern was subscribed to
        with a message handler, the handler is invoked instead of a parsed
        message being returned.

        :meta private:
        """
        r = cast(List[ResponsePrimitive], response)
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
            subscribed_dict.pop(message["channel"])

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
        else:
            # this is a subscribe/unsubscribe message. ignore if we don't
            # want them

            if ignore_subscribe_messages or self.ignore_subscribe_messages:
                return None

        return message

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


class PubSub(BasePubSub[AnyStr, "coredis.pool.ConnectionPool"]):
    """
    Pub/Sub implementation to be used with :class:`coredis.Redis`
    that is returned by :meth:`coredis.Redis.pubsub`
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


    """

    async def execute_command(
        self, command: bytes, *args: ValueT, **options: ValueT
    ) -> Optional[ResponseType]:
        await self.connection_pool.initialize()

        if self.connection is None:
            await self.reset_connections(None)

        assert self.connection
        return await self._execute(
            self.connection, self.connection.send_command, command, *args
        )

    async def reset_connections(self, exc: Optional[BaseException] = None) -> None:
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
        retry_policy: Optional[RetryPolicy] = None,
        read_from_replicas: bool = False,
    ):
        self.shard_connections: Dict[str, Connection] = {}
        self.channel_connection_mapping: Dict[StringT, Connection] = {}
        self.pending_tasks: Dict[str, asyncio.Task[ResponseType]] = {}
        self.read_from_replicas = read_from_replicas
        super().__init__(connection_pool, ignore_subscribe_messages, retry_policy)

    async def subscribe(
        self,
        *channels: StringT,
        **channel_handlers: Optional[SubscriptionCallback],
    ) -> None:
        """
        :param channels: The shard channels to subscribe to.
        :param channel_handlers: Channels supplied as keyword arguments expect
         a channel name as the key and a callable as the value. A channel's
         callable will be invoked automatically when a message is received on
         that channel rather than producing a message via :meth:`listen` or
         :meth:`get_message`.
        """

        await self._ensure_encoding()
        new_channels: MutableMapping[StringT, Optional[SubscriptionCallback]] = {}
        new_channels.update(dict.fromkeys(map(self.encode, channels)))

        for channel, handler in channel_handlers.items():
            new_channels[self.encode(channel)] = handler
        for new_channel in new_channels.keys():
            await self.execute_command(
                CommandName.SSUBSCRIBE, new_channel, sharded=True
            )
        self.channels.update(new_channels)

    async def unsubscribe(self, *channels: StringT) -> None:
        """
        :param channels: The shard channels to unsubscribe from. If None are provided,
         this will effectively unsubscribe the client from all channels
         previously subscribed to.
        """

        await self._ensure_encoding()
        for channel in channels or list(self.channels.keys()):
            await self.execute_command(CommandName.SUNSUBSCRIBE, channel, sharded=True)

    async def psubscribe(
        self,
        *patterns: StringT,
        **pattern_handlers: Optional[SubscriptionCallback],
    ) -> None:
        """
        Not available in sharded pubsub

        :meta private:
        """
        raise NotImplementedError(
            "Sharded PubSub does not support subscription by pattern"
        )

    async def punsubscribe(self, *patterns: StringT) -> None:
        """
        Not available in sharded pubsub

        :meta private:
        """
        raise NotImplementedError(
            "Sharded PubSub does not support subscription by pattern"
        )

    async def execute_command(
        self, command: bytes, *args: ValueT, **options: ValueT
    ) -> Optional[ResponseType]:
        await self.connection_pool.initialize()

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

    async def reset_connections(self, exc: Optional[BaseException] = None) -> None:
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
        self, block: bool = True, timeout: Optional[float] = None
    ) -> ResponseType:
        if not self.shard_connections:
            raise RuntimeError(
                "pubsub connection not set: "
                "did you forget to call subscribe() or psubscribe()?"
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
            broken_connections = [
                c for c in self.shard_connections.values() if not c.is_connected
            ]
            if broken_connections:
                for connection in broken_connections:
                    try:
                        await connection.connect()
                    except:  # noqa
                        raise ConnectionError("Shard connections not stable")
            tasks: Dict[str, asyncio.Task[ResponseType]] = {
                node_id: asyncio.create_task(
                    self._execute(
                        connection,
                        partial(
                            connection.fetch_push_message,
                            push_message_types=self.SUBUNSUB_MESSAGE_TYPES
                            | self.PUBLISH_MESSAGE_TYPES,
                        ),
                    )
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
                        channel.decode(self.encoding)
                        if isinstance(channel, bytes)
                        else channel: handler
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
        self._future: Optional[Future[None]] = None

    async def _run(self) -> None:
        pubsub = self._pubsub
        try:
            while pubsub.subscribed:
                await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=self._poll_timeout
                )
        except CancelledError:
            await asyncio.gather(pubsub.unsubscribe(), pubsub.punsubscribe())
            pubsub.close()
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
