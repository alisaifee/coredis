from __future__ import annotations

import asyncio
import threading
from asyncio import CancelledError
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, cast

from coredis._utils import nativestr
from coredis.commands.constants import CommandName
from coredis.connection import BaseConnection
from coredis.exceptions import ConnectionError, PubSubError, TimeoutError
from coredis.response.types import PubSubMessage
from coredis.typing import (
    AnyStr,
    Awaitable,
    Callable,
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


class PubSub(Generic[AnyStr]):
    """
    PubSub provides publish, subscribe and listen support to Redis channels.

    After subscribing to one or more channels, the listen() method will block
    until a message arrives on one of the subscribed channels. That message
    will be returned and it's safe to start listening again.
    """

    PUBLISH_MESSAGE_TYPES = ("message", "pmessage")
    UNSUBSCRIBE_MESSAGE_TYPES = ("unsubscribe", "punsubscribe")
    channels: MutableMapping[StringT, Optional[Callable[[PubSubMessage], None]]]
    patterns: MutableMapping[StringT, Optional[Callable[[PubSubMessage], None]]]

    def __init__(
        self,
        connection_pool: coredis.pool.ConnectionPool,
        ignore_subscribe_messages: bool = False,
    ):
        self.connection_pool = connection_pool
        self.ignore_subscribe_messages = ignore_subscribe_messages
        self.connection: Optional[coredis.connection.Connection] = None
        self.reset()

    async def _ensure_encoding(self) -> None:
        if hasattr(self, "encoding"):
            return

        conn = await self.connection_pool.get_connection("pubsub")
        try:
            self.encoding = conn.encoding
            self.decode_responses = conn.decode_responses
        finally:
            self.connection_pool.release(conn)

    def __del__(self) -> None:
        try:
            # if this object went out of scope prior to shutting down
            # subscriptions, close the connection manually before
            # returning it to the connection pool
            self.reset()
        except Exception:
            pass

    def reset(self) -> None:
        if self.connection:
            self.connection.disconnect()
            self.connection.clear_connect_callbacks()
            self.connection_pool.release(self.connection)
            self.connection = None
        self.channels = {}
        self.patterns = {}

    def close(self) -> None:
        self.reset()

    async def on_connect(self, _: BaseConnection) -> None:
        """Re-subscribe to any channels and patterns previously subscribed to"""

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
        """

        if self.decode_responses and isinstance(value, bytes):
            value = value.decode(self.encoding)
        elif not self.decode_responses and isinstance(value, str):
            value = value.encode(self.encoding)

        return value

    @property
    def subscribed(self) -> bool:
        """Indicates if there are subscriptions to any channels or patterns"""

        return bool(self.channels or self.patterns)

    async def execute_command(
        self, command: bytes, *args: ValueT, **options: ValueT
    ) -> Optional[ResponseType]:
        """Executes a publish/subscribe command"""

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

            if await connection.can_read():
                # disconnect if buffer is not empty in case of error
                # when connection is reused
                connection.disconnect()

            raise
        except (ConnectionError, TimeoutError) as e:
            connection.disconnect()

            if not connection.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            # Connect manually here. If the Redis server is down, this will
            # fail and raise a ConnectionError as desired.
            await connection.connect()
            # the ``on_connect`` callback should haven been called by the
            # connection to resubscribe us to any channels and patterns we were
            # previously listening to

            return await command(*args)

    async def parse_response(
        self, block: bool = True, timeout: float = 0.0
    ) -> ResponseType:
        """Parses the response from a publish/subscribe command"""
        connection = self.connection

        if connection is None:
            raise RuntimeError(
                "pubsub connection not set: "
                "did you forget to call subscribe() or psubscribe()?"
            )
        coro = self._execute(connection, connection.read_response)

        if not block and timeout > 0:
            try:
                return await asyncio.wait_for(coro, timeout)
            except Exception:
                return None

        return await coro

    async def psubscribe(
        self,
        *patterns: StringT,
        **pattern_handlers: Optional[Callable[[PubSubMessage], None]],
    ) -> None:
        """
        Subscribes to channel patterns. Patterns supplied as keyword arguments
        expect a pattern name as the key and a callable as the value. A
        pattern's callable will be invoked automatically when a message is
        received on that pattern rather than producing a message via
        ``listen()``.
        """
        await self._ensure_encoding()

        new_patterns: MutableMapping[
            StringT, Optional[Callable[[PubSubMessage], None]]
        ] = {}
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
        **channel_handlers: Optional[Callable[[PubSubMessage], None]],
    ) -> None:
        """
        Subscribes to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via ``listen()`` or
        ``get_message()``.
        """

        await self._ensure_encoding()

        new_channels: MutableMapping[
            StringT, Optional[Callable[[PubSubMessage], None]]
        ] = {}
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
                return self.handle_message(response)
        return None

    async def get_message(
        self, ignore_subscribe_messages: bool = False, timeout: Union[int, float] = 0
    ) -> Optional[PubSubMessage]:
        """
        Gets the next message if one is available, otherwise None.

        If timeout is specified, the system will wait for `timeout` seconds
        before returning. Timeout should be specified as a floating point
        number.
        """
        response = await self.parse_response(block=False, timeout=timeout)

        if response:
            return self.handle_message(response, ignore_subscribe_messages)

        return None

    def handle_message(
        self, response: ResponseType, ignore_subscribe_messages: bool = False
    ) -> Optional[PubSubMessage]:
        """
        Parses a pub/sub message. If the channel or pattern was subscribed to
        with a message handler, the handler is invoked instead of a parsed
        message being returned.
        """
        r = cast(List[ResponsePrimitive], response)
        message_type = nativestr(r[0])

        if message_type == "pmessage":
            message = PubSubMessage(
                type=message_type,
                pattern=cast(StringT, r[1]),
                channel=cast(StringT, r[2]),
                data=cast(StringT, r[3]),
            )
        else:
            message = PubSubMessage(
                type=message_type,
                pattern=None,
                channel=cast(StringT, r[1]),
                data=cast(StringT, r[2]),
            )

        # if this is an unsubscribe message, remove it from memory
        if message_type in self.UNSUBSCRIBE_MESSAGE_TYPES:
            if message_type == "punsubscribe":
                subscribed_dict = self.patterns
            else:
                subscribed_dict = self.channels
            try:
                del subscribed_dict[message["channel"]]
            except KeyError:
                pass

        if message_type in self.PUBLISH_MESSAGE_TYPES:
            handler = None
            if message_type == "pmessage" and message["pattern"]:
                handler = self.patterns.get(message["pattern"], None)
            elif message["channel"]:
                handler = self.channels.get(message["channel"], None)

            if handler:
                handler(message)
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

        To stop listening invoke :meth:`PubSubWorkerThread.stop` on the returned
        instead of :class:`PubSubWorkerThread`.
        """
        for channel, handler in self.channels.items():
            if handler is None:
                raise PubSubError(f"Channel: '{channel!r}' has no handler registered")

        for pattern, handler in self.patterns.items():
            if handler is None:
                raise PubSubError(f"Pattern: '{pattern!r}' has no handler registered")
        thread = PubSubWorkerThread(
            self, poll_timeout=poll_timeout, loop=asyncio.get_running_loop()
        )
        thread.start()

        return thread


class PubSubWorkerThread(threading.Thread):
    def __init__(
        self,
        pubsub: PubSub[Any],
        loop: asyncio.events.AbstractEventLoop,
        poll_timeout: float = 1.0,
    ):
        super().__init__()
        self._pubsub = pubsub
        self._poll_timeout = poll_timeout
        self._running = False
        self._loop = loop or asyncio.get_running_loop()
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
        if self._running:
            return
        self._running = True
        self._future = asyncio.run_coroutine_threadsafe(self._run(), self._loop)

    def stop(self) -> None:
        if self._future:
            self._future.cancel()


class ClusterPubSub(PubSub[AnyStr]):
    """Wrappers for the PubSub class"""

    async def execute_command(
        self, command: bytes, *args: ValueT, **options: ValueT
    ) -> Optional[ResponseType]:
        """
        Executes a publish/subscribe command.

        NOTE: The code was initially taken from redis-py and tweaked to make it work within a
        cluster.
        """
        # NOTE: don't parse the response in this function -- it could pull a
        # legitimate message off the stack if the connection is already
        # subscribed to one or more channels
        await self.connection_pool.initialize()

        if self.connection is None:
            self.connection = await self.connection_pool.get_connection(
                "pubsub",
                channel=str(args[0]),
            )
            # register a callback that re-subscribes to any channels we
            # were listening to when we were disconnected
            self.connection.register_connect_callback(self.on_connect)

        assert self.connection
        return await self._execute(
            self.connection, self.connection.send_command, command, *args
        )
