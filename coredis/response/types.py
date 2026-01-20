from __future__ import annotations

from collections.abc import Set

from coredis.typing import (
    Literal,
    Mapping,
    NamedTuple,
    OrderedDict,
    RedisValueT,
    StringT,
    TypedDict,
)

#: Response from `CLIENT INFO <https://redis.io/commands/client-info>`__
#:
#: - ``id``: a unique 64-bit client ID
#: - ``addr``: address/port of the client
#: - ``laddr``: address/port of local address client connected to (bind address)
#: - ``fd``: file descriptor corresponding to the socket
#: - ``name``: the name set by the client with CLIENT SETNAME
#: - ``age``: total duration of the connection in seconds
#: - ``idle``: idle time of the connection in seconds
#: - ``flags``: client flags
#: - ``db``: current database ID
#: - ``sub``: number of channel subscriptions
#: - ``psub``: number of pattern matching subscriptions
#: - ``multi``: number of commands in a MULTI/EXEC context
#: - ``qbuf``: query buffer length (0 means no query pending)
#: - ``qbuf-free``: free space of the query buffer (0 means the buffer is full)
#: - ``argv-mem``: incomplete arguments for the next command (already extracted from query buffer)
#: - ``multi-mem``: memory is used up by buffered multi commands. Added in Redis 7.0
#: - ``obl``: output buffer length
#: - ``oll``: output list length (replies are queued in this list when the buffer is full)
#: - ``omem``: output buffer memory usage
#: - ``tot-mem``: total memory consumed by this client in its various buffers
#: - ``events``: file descriptor events
#: - ``cmd``: last command played
#: - ``user``: the authenticated username of the client
#: - ``redir``: client id of current client tracking redirection
#: - ``resp``: client RESP protocol version. Added in Redis 7.0
ClientInfo = TypedDict(
    "ClientInfo",
    {
        "id": int,
        "addr": str,
        "laddr": str,
        "fd": int,
        "name": str,
        "age": int,
        "idle": int,
        "flags": str,
        "db": int,
        "sub": int,
        "psub": int,
        "multi": int,
        "qbuf": int,
        "qbuf-free": int,
        "argv-mem": int,
        "multi-mem": int,
        "obl": int,
        "oll": int,
        "omem": int,
        "tot-mem": int,
        "events": str,
        "cmd": str,
        "user": str,
        "redir": int,
        "resp": str,
    },
)

#: Script/Function flags
#: See: `<https://redis.io/topics/lua-api#a-namescriptflagsa-script-flags>`__
ScriptFlag = Literal[
    "no-writes",
    "allow-oom",
    "allow-stale",
    "no-cluster",
    b"no-writes",
    b"allow-oom",
    b"allow-stale",
    b"no-cluster",
]


class FunctionDefinition(TypedDict):
    """
    Function definition as returned by `FUNCTION LIST <https://redis.io/commands/function-list>`__
    """

    #: the name of the function
    name: StringT
    #: the description of the function
    description: StringT
    #: function flags
    flags: set[ScriptFlag]


class LibraryDefinition(TypedDict):
    """
    Library definition as returned by `FUNCTION LIST <https://redis.io/commands/function-list>`__
    """

    #: the name of the library
    name: StringT
    #: the engine used by the library
    engine: Literal["LUA"]
    #: Mapping of function names to functions in the library
    functions: dict[StringT, FunctionDefinition]
    #: The library's source code
    library_code: StringT | None


class ScoredMember(NamedTuple):
    """
    Member of a sorted set
    """

    #: The sorted set member name
    member: StringT
    #: Score of the member
    score: float


class GeoCoordinates(NamedTuple):
    """
    A longitude/latitude pair identifying a location
    """

    #: Longitude
    longitude: float
    #: Latitude
    latitude: float


ScoredMembers = tuple[ScoredMember, ...]


class GeoSearchResult(NamedTuple):
    """
    Structure of a geo query
    """

    #: Place name
    name: StringT
    #: Distance
    distance: float | None
    #: GeoHash
    geohash: int | None
    #: Lat/Lon
    coordinates: GeoCoordinates | None


#: Definition of a redis command
#: See: `<https://redis.io/topics/key-specs>`__
#:
#: - ``name``: This is the command's name in lowercase
#: - ``arity``: Arity is the number of arguments a command expects.
#: - ``flags``: See `<https://redis.io/commands/command#flags>`__
#: - ``first-key``: This value identifies the position of the command's first key name argumen
#: - ``last-key``: This value identifies the position of the command's last key name argument
#: - ``step``: This value is the step, or increment, between the first key and last key values
#:   where the keys are.
#: - ``acl-categories``: This is an array of simple strings that are the ACL categories to which
#:   the command belongs
#: - ``tips``: Helpful information about the command
#: - ``key-specification``: This is an array consisting of the command's key specifications
#: - ``sub-commands``: This is an array containing all of the command's subcommands, if any
Command = TypedDict(
    "Command",
    {
        "name": str,
        "arity": int,
        "flags": Set[str],
        "first-key": int,
        "last-key": int,
        "step": int,
        "acl-categories": Set[str] | None,
        "tips": Set[str] | None,
        "key-specifications": Set[Mapping[str, int | str | Mapping]] | None,  # type: ignore
        "sub-commands": tuple[str, ...] | None,
    },
)


class RoleInfo(NamedTuple):
    """
    Redis instance role information
    """

    #:
    role: str
    #:
    offset: int | None = None
    #:
    status: str | None = None
    #:
    slaves: tuple[dict[str, str | int], ...] | None = None
    #:
    masters: tuple[str, ...] | None = None


class StreamEntry(NamedTuple):
    """
    Structure representing an entry in a redis stream
    """

    identifier: StringT
    field_values: OrderedDict[StringT, StringT]


#: Details of a stream
#: See: `<https://redis.io/commands/xinfo-stream>`__
StreamInfo = TypedDict(
    "StreamInfo",
    {
        "first-entry": StreamEntry | None,
        "last-entry": StreamEntry | None,
        "length": int,
        "radix-tree-keys": int,
        "radix-tree-nodes": int,
        "groups": int | list[dict],  # type: ignore
        "last-generated-id": str,
        "max-deleted-entry-id": str,
        "recorded-first-entry-id": str,
        "entries-added": int,
        "entries-read": int,
        "entries": tuple[StreamEntry, ...] | None,
    },
)


class StreamPending(NamedTuple):
    """
    Summary response from
    `XPENDING <https://redis.io/commands/xpending#summary-form-of-xpending>`__
    """

    pending: int
    minimum_identifier: StringT
    maximum_identifier: StringT
    consumers: OrderedDict[StringT, int]


class StreamPendingExt(NamedTuple):
    """
    Extended form response from
    `XPENDING <https://redis.io/commands/xpending#extended-form-of-xpending>`__
    """

    identifier: StringT
    consumer: StringT
    idle: int
    delivered: int


#: Response from `SLOWLOG GET <https://redis.io/commands/slowlog-get>`__
class SlowLogInfo(NamedTuple):
    #: A unique progressive identifier for every slow log entry.
    id: int
    #: The unix timestamp at which the logged command was processed.
    start_time: int
    #: The amount of time needed for its execution, in microseconds.
    duration: int
    #: The array composing the arguments of the command.
    command: tuple[StringT, ...]
    #: Client IP address and port
    client_addr: tuple[StringT, int]
    #: Client name
    client_name: str


class LCSMatch(NamedTuple):
    """
    An instance of an LCS match
    """

    #: Start/end offset of the first string
    first: tuple[int, int]
    #: Start/end offset of the second string
    second: tuple[int, int]
    #: Length of the match
    length: int | None


class LCSResult(NamedTuple):
    """
    Results from `LCS <https://redis.io/commands/lcs>`__
    """

    #: matches
    matches: tuple[LCSMatch, ...]
    #: Length of longest match
    length: int


class ClusterNode(TypedDict):
    host: str
    port: int
    node_id: str | None
    server_type: Literal["master", "slave"] | None


class ClusterNodeDetail(TypedDict):
    id: str
    flags: tuple[str, ...]
    host: str
    port: int
    master: str | None
    ping_sent: int
    pong_recv: int
    link_state: str
    slots: list[int]
    migrations: list[dict[str, RedisValueT]]


class PubSubMessage(TypedDict):
    #: One of the following:
    #:
    #: subscribe
    #:   Server response when a client subscribes to a channel(s)
    #: unsubscribe
    #:   Server response when a client unsubscribes from a channel(s)
    #: psubscribe
    #:   Server response when a client subscribes to a pattern(s)
    #: punsubscribe
    #:   Server response when a client unsubscribes from a pattern(s)
    #: ssubscribe
    #:   Server response when a client subscribes to a shard channel(s)
    #: sunsubscribe
    #:   Server response when a client unsubscribes from a shard channel(s)
    #: message
    #:   A message received from subscribing to a channel
    #: pmessage
    #:   A message received from subscribing to a pattern
    type: str
    #: The channel subscribed to or unsubscribed from or the channel a message was published to
    channel: StringT
    #: The pattern that was subscribed to or unsubscribed from or to which a received message was
    #: routed to
    pattern: StringT | None
    #: - If ``type`` is one of ``{message, pmessage}`` this is the actual published message
    #: - If ``type`` is one of
    #:   ``{subscribe, psubscribe, ssubscribe, unsubscribe, punsubscribe, sunsubscribe}``
    #:   this will be an :class:`int` corresponding to the  number of channels and patterns that the
    #:   connection is currently subscribed to.
    data: int | StringT


class VectorData(TypedDict):
    #: The quantization type as a string (``fp32``, ``bin`` or ``q8``)
    quantization: str
    #: Raw bytes representation of the vector
    blob: bytes
    #: The L2 norm of the vector before normalization
    l2_norm: float
    #: If the vector is quantized as q8, the quantization range
    quantization_range: float
