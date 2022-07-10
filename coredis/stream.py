from __future__ import annotations

from typing import Any

from coredis._utils import EncodingInsensitiveDict, nativestr
from coredis.client import Client
from coredis.exceptions import (
    ResponseError,
    StreamConsumerInitializationError,
    StreamDuplicateConsumerGroupError,
)
from coredis.response.types import StreamEntry
from coredis.tokens import PureToken
from coredis.typing import (
    AnyStr,
    ClassVar,
    Dict,
    Generator,
    Generic,
    KeyT,
    List,
    Optional,
    Parameters,
    Set,
    StringT,
    Tuple,
    TypedDict,
    ValueT,
)


class StreamParameters(TypedDict):
    #: Starting ``identifier`` for the consumer. If not present it will start
    #: from the latest entry
    identifier: StringT


class State(TypedDict, total=False):
    identifier: Optional[StringT]
    pending: Optional[bool]


class Consumer(Generic[AnyStr]):
    state: Dict[KeyT, State]
    DEFAULT_START_ID: ClassVar[bytes] = b"0-0"

    def __init__(
        self,
        client: Client[AnyStr],
        streams: Parameters[KeyT],
        buffer_size: int = 0,
        timeout: Optional[int] = 0,
        **stream_parameters: StreamParameters,
    ):
        """
        Standalone stream consumer that starts reading from the latest entry
        of each stream provided in :paramref:`streams`.

        The latest entry is determined by calling :meth:`coredis.Redis.xinfo_stream`
        and using the :data:`last-entry` attribute
        at the point of initializing the consumer instance or on first fetch (whichever comes
        first). If the stream(s) do not exist at the time of consumer creation, the
        consumer will simply start from the minimum identifier (``0-0``)

        :param client: The redis client to use
        :param streams: the stream identifiers to consume from
        :param buffer_size: Size of buffer (per stream) to maintain. This
         translates to the maximum number of stream entries that are fetched
         on each request to redis.
        :param timeout: Maximum amount of time in milliseconds to block for new
         entries to appear on the streams the consumer is reading from.
        :param stream_parameters: Mapping of optional parameters to use
         by stream for the streams provided in :paramref:`streams`.
        """
        self.client: Client[AnyStr] = client
        self.streams: Set[KeyT] = set(streams)
        self.state: Dict[StringT, State] = EncodingInsensitiveDict(
            {stream: stream_parameters.get(nativestr(stream), {}) for stream in streams}
        )
        self.buffer: Dict[AnyStr, List[StreamEntry]] = EncodingInsensitiveDict({})
        self.buffer_size = buffer_size
        self.timeout = timeout
        self._initialized = False

    def chunk_streams(self) -> List[Dict[ValueT, StringT]]:
        import coredis.client

        if isinstance(self.client, coredis.client.RedisCluster):
            return [
                {
                    stream: self.state[stream].get("identifier", None)
                    or self.DEFAULT_START_ID
                }
                for stream in self.streams
            ]
        else:
            return [
                {
                    stream: self.state[stream].get("identifier", None)
                    or self.DEFAULT_START_ID
                    for stream in self.streams
                }
            ]

    async def initialize(self) -> "Consumer[AnyStr]":
        if self._initialized:
            return self

        for stream in self.streams:
            try:
                if info := await self.client.xinfo_stream(stream):  # type: ignore[has-type]
                    if last_entry := info["last-entry"]:
                        self.state[stream].setdefault(
                            "identifier", last_entry.identifier  # type: ignore[has-type]
                        )
            except ResponseError:
                pass
        self._initialized = True
        return self

    def __await__(self) -> Generator[Any, None, Consumer[AnyStr]]:
        return self.initialize().__await__()

    def __aiter__(self) -> Consumer[AnyStr]:
        """
        Returns the instance of the consumer itself which can be iterated over
        """
        return self

    async def __anext__(self) -> Tuple[AnyStr, StreamEntry]:
        """
        Returns the next available stream entry available from any of
        :paramref:`Consumer.streams`.

        :raises: :exc:`StopIteration` if no more entries are available
        """
        stream, entry = await self.get_entry()
        if not (stream and entry):
            raise StopAsyncIteration()
        return stream, entry

    async def get_entry(self) -> Tuple[Optional[AnyStr], Optional[StreamEntry]]:
        """
        Fetches the next available entry from the streams specified in
        :paramref:`Consumer.streams`. If there were any entries
        previously fetched and buffered, they will be returned before
        making a new request to the server.
        """
        await self.initialize()
        cur = None
        cur_stream = None
        for stream, buffer_entries in list(self.buffer.items()):
            if buffer_entries:
                cur_stream, cur = stream, self.buffer[stream].pop(0)
                break
        else:
            consumed_entries: Dict[AnyStr, Tuple[StreamEntry, ...]] = {}
            for chunk in self.chunk_streams():
                consumed_entries.update(
                    await self.client.xread(
                        chunk,
                        count=self.buffer_size + 1,
                        block=self.timeout
                        if (self.timeout and self.timeout > 0)
                        else None,
                    )
                    or {}
                )
            for stream, entries in consumed_entries.items():
                if entries:
                    if not cur:
                        cur = entries[0]
                        cur_stream = stream
                        if entries[1:]:
                            self.buffer.setdefault(stream, []).extend(entries[1:])
                    else:
                        self.buffer.setdefault(stream, []).extend(entries)
        if cur and cur_stream:
            self.state[cur_stream]["identifier"] = cur.identifier
        return cur_stream, cur


class GroupConsumer(Consumer[AnyStr]):
    DEFAULT_START_ID: ClassVar[bytes] = b">"

    def __init__(
        self,
        client: Client[AnyStr],
        streams: Parameters[KeyT],
        group: StringT,
        consumer: StringT,
        buffer_size: int = 0,
        auto_create: bool = True,
        auto_acknowledge: bool = False,
        start_from_backlog: bool = False,
        timeout: Optional[int] = None,
        **stream_parameters: StreamParameters,
    ):
        """
        A member of a stream consumer group. The consumer has an identical
        interface as :class:`coredis.stream.Consumer`.

        :param client: The redis client to use
        :param streams: The stream identifiers to consume from
        :param group: The name of the group this consumer is part of
        :param consumer: The unique name (within :paramref:`group`) of the consumer
        :param auto_create: If True the group will be created upon initialization
         or first fetch if it doesn't already exist.
        :param auto_acknowledge: If ``True`` the stream entries fetched will be fetched
         without needing to be acknowledged with :meth:`coredis.Redis.xack` to remove
         them from the pending entries list.
        :param start_from_backlog: If ``True`` the consumer will start by fetching any pending
         entries from the pending entry list before considering any new messages
         not seen by any other consumer in the :paramref:`group`
        :param buffer_size: Size of buffer (per stream) to maintain. This
         translates to the maximum number of stream entries that are fetched
         on each request to redis.
        :param timeout: Maximum amount of time to block for new
         entries to appear on the streams the consumer is reading from.
        :param stream_parameters: Mapping of optional parameters to use
         by stream for the streams provided in :paramref:`streams`.
        """
        super().__init__(
            client,  # type: ignore[arg-type]
            streams,
            buffer_size,
            timeout,
            **stream_parameters,
        )
        self.group = group
        self.consumer = consumer
        self.auto_create = auto_create
        self.auto_acknowledge = auto_acknowledge
        self.start_from_backlog = start_from_backlog

    async def initialize(self) -> "GroupConsumer[AnyStr]":
        if not self._initialized:
            group_presence: Dict[KeyT, bool] = {
                stream: False for stream in self.streams
            }
            for stream in self.streams:
                try:
                    group_presence[stream] = (
                        len(
                            [
                                info
                                for info in [
                                    EncodingInsensitiveDict(d)
                                    for d in await self.client.xinfo_groups(stream)
                                ]
                                if nativestr(info["name"]) == self.group
                            ]
                        )
                        == 1
                    )
                    if group_presence[stream] and self.start_from_backlog:
                        self.state[stream]["pending"] = True
                        self.state[stream]["identifier"] = "0-0"
                except ResponseError:
                    self.state[stream].setdefault("identifier", ">")

            if not (self.auto_create or all(group_presence.values())):
                missing_streams = self.streams - {
                    k for k in group_presence if not group_presence[k]
                }
                raise StreamConsumerInitializationError(
                    f"Consumer group: {self.group!r} does not exist for streams: {missing_streams}"
                )
            for stream in self.streams:
                if self.auto_create and not group_presence.get(stream):
                    try:
                        await self.client.xgroup_create(
                            stream, self.group, PureToken.NEW_ID, mkstream=True
                        )
                    except StreamDuplicateConsumerGroupError:  # noqa
                        pass
                self.state[stream].setdefault("identifier", ">")

            self._initialized = True
        return self

    def __await__(self) -> Generator[Any, None, GroupConsumer[AnyStr]]:
        return self.initialize().__await__()

    def __aiter__(self) -> GroupConsumer[AnyStr]:
        """
        Returns the instance of the consumer itself which can be iterated over
        """
        return self

    async def get_entry(self) -> Tuple[Optional[AnyStr], Optional[StreamEntry]]:
        """
        Fetches the next available entry from the streams specified in
        :paramref:`GroupConsumer.streams`. If there were any entries
        previously fetched and buffered, they will be returned before
        making a new request to the server.
        """
        await self.initialize()

        cur = None
        cur_stream = None
        for stream, buffer_entries in list(self.buffer.items()):
            if buffer_entries:
                cur_stream, cur = stream, self.buffer[stream].pop(0)
                break
        else:
            consumed_entries: Dict[AnyStr, Tuple[StreamEntry, ...]] = {}
            for chunk in self.chunk_streams():
                consumed_entries.update(
                    await self.client.xreadgroup(
                        self.group,
                        self.consumer,
                        count=self.buffer_size + 1,
                        block=self.timeout
                        if (self.timeout and self.timeout > 0)
                        else None,
                        noack=self.auto_acknowledge,
                        streams=chunk,
                    )
                    or {}
                )
            for stream, entries in consumed_entries.items():
                if entries:
                    if not cur:
                        cur = entries[0]
                        cur_stream = stream
                        if entries[1:]:
                            self.buffer.setdefault(stream, []).extend(entries[1:])

                    else:
                        self.buffer.setdefault(stream, []).extend(entries)
                    if self.state[stream].get("pending"):
                        self.state[stream]["identifier"] = entries[-1].identifier
                else:
                    if self.state[stream].get("pending"):
                        self.state[stream].pop("identifier", None)
                        self.state[stream].pop("pending", None)
                        if not cur:
                            return await self.get_entry()
        return cur_stream, cur
