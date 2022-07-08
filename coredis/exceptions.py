from __future__ import annotations

import re

from coredis.typing import Optional, Set, Tuple, ValueT


class RedisError(Exception):
    """
    Base exception from which all other exceptions in coredis
    derive from.
    """


class CommandSyntaxError(RedisError):
    """
    Raised when a redis command is called with an invalid syntax
    """

    def __init__(self, arguments: Set[str], message: str) -> None:
        self.arguments: Set[str] = arguments
        super().__init__(message)


class CommandNotSupportedError(RedisError):
    """
    Raised when the target server doesn't support a command due to
    version mismatch
    """

    def __init__(self, cmd: str, current_version: str) -> None:
        super().__init__(
            self, f"{cmd} is not supported on server version {current_version}"
        )


class ConnectionError(RedisError):
    pass


class ProtocolError(ConnectionError):
    """
    Raised on errors related to ser/deser protocol parsing
    """


class TimeoutError(RedisError):
    pass


class BusyLoadingError(ConnectionError):
    pass


class InvalidResponse(RedisError):
    pass


class ResponseError(RedisError):
    pass


class DataError(RedisError):
    pass


class NoKeyError(RedisError):
    """
    Raised when a key provided in the command is missing
    """


class ReplicationError(RedisError):
    """
    Raised when then replication requirements were not met
    """

    def __init__(self, command: bytes, replicas: int, timeout: int):
        super().__init__(
            f"Command {str(command)} did not replicate to {replicas} replicas within {timeout} ms."
        )


class NotBusyError(ResponseError):
    """
    Raised when script kill or function kill have nothing to terminate
    """


class UnblockedError(ResponseError):
    """
    Raised if a blocked client is unblocked forcefully
    """


class WrongTypeError(ResponseError):
    """
    Raised when an operation is performed on a key
    containing a datatype that doesn't support the operation
    """


class PubSubError(RedisError):
    pass


class WatchError(RedisError):
    pass


class NoScriptError(ResponseError):
    pass


class ExecAbortError(ResponseError):
    pass


class ReadOnlyError(ResponseError):
    pass


class LockError(RedisError, ValueError):
    """Errors acquiring or releasing a lock"""

    # NOTE: For backwards compatability, this class derives from ValueError.
    # This was originally chosen to behave like threading.Lock.


class RedisClusterException(Exception):
    """Base exception for the RedisCluster client"""


class ClusterError(RedisError):
    """
    Cluster errors occurred multiple times, resulting in an exhaustion of the
    command execution ``TTL``
    """


class ClusterCrossSlotError(ResponseError):
    """Raised when keys in request don't hash to the same slot"""

    def __init__(
        self,
        message: Optional[str] = None,
        command: Optional[bytes] = None,
        keys: Optional[Tuple[ValueT, ...]] = None,
    ) -> None:
        super().__init__(message or "Keys in request don't hash to the same slot")
        self.command = command
        self.keys = keys


class ClusterRoutingError(RedisClusterException):
    """Raised when keys in request can't be routed to destination nodes"""


class ClusterDownError(ClusterError, ResponseError):
    """
    Error indicated ``CLUSTERDOWN`` error received from cluster.

    By default Redis Cluster nodes stop accepting queries if they detect there
    is at least a hash slot uncovered (no available node is serving it).
    This way if the cluster is partially down (for example a range of hash
    slots are no longer covered) the entire cluster eventually becomes
    unavailable. It automatically returns available as soon as all the slots
    are covered again.
    """

    def __init__(self, resp: str) -> None:
        self.args = (resp,)
        self.message = resp


class ClusterTransactionError(ClusterError):
    def __init__(self, msg: str) -> None:
        self.msg = msg


class ClusterResponseError(ClusterError):
    """
    Raised when application logic to combine multi node
    cluster responses has errors.
    """


class AskError(ResponseError):
    """
    Error indicated ``ASK`` error received from cluster.

    When a slot is set as ``MIGRATING``, the node will accept all queries that
    pertain to this hash slot, but only if the key in question exists,
    otherwise the query is forwarded using a -ASK redirection to the node that
    is target of the migration.

    src node: ``MIGRATING`` to dst node
        get > ``ASK`` error
        ask dst node > ``ASKING`` command
    dst node: ``IMPORTING`` from src node
        asking command only affects next command
        any op will be allowed after asking command
    """

    def __init__(self, resp: str) -> None:
        self.args = (resp,)
        self.message = resp
        slot_id, new_node = resp.split(" ")
        host, port = new_node.rsplit(":", 1)
        self.slot_id = int(slot_id)
        self.node_addr = self.host, self.port = host, int(port)


class TryAgainError(ResponseError):
    """
    Error indicated ``TRYAGAIN`` error received from cluster.
    Operations on keys that don't exist or are - during resharding - split
    between the source and destination nodes, will generate a -``TRYAGAIN`` error.
    """


class MovedError(AskError):
    """
    Error indicated ``MOVED`` error received from cluster.
    A request sent to a node that doesn't serve this key will be replayed with
    a ``MOVED`` error that points to the correct node.
    """


class AuthenticationError(ResponseError):
    """
    Base class for authentication errors
    """


class AuthenticationFailureError(AuthenticationError):
    """
    Raised when authentication parameters were provided
    but were invalid
    """


class AuthenticationRequiredError(AuthenticationError):
    """
    Raised when authentication parameters are required
    but not provided
    """


class AuthorizationError(RedisError):
    """
    Base class for authorization errors
    """


class FunctionError(RedisError):
    """
    Raised for errors relating to redis functions
    """


class SentinelConnectionError(ConnectionError):
    pass


class PrimaryNotFoundError(SentinelConnectionError):
    """
    Raised when a primary cannot be located in a
    sentinel managed redis
    """


class ReplicaNotFoundError(SentinelConnectionError):
    """
    Raised when a replica cannot be located in a
    sentinel managed redis
    """


class UnknownCommandError(ResponseError):
    """
    Raised when the server returns an error response relating
    to an unknown command.
    """

    ERROR_REGEX = re.compile("unknown command (.*?)")
    #: Name of command requested
    command: str

    def __init__(self, message: str) -> None:
        command_match = self.ERROR_REGEX.findall(message)
        if command_match:
            self.command = command_match.pop()
        super().__init__(self, message)


class StreamConsumerError(RedisError):
    """
    Base exception for stream consumer related errors
    """


class StreamConsumerGroupError(StreamConsumerError):
    """
    Base exception for consumer group related errors
    """


class StreamDuplicateConsumerGroupError(StreamConsumerGroupError):
    """
    Raised when and attempt to create a stream consumer
    group fails because it already exists
    """


class StreamConsumerInitializationError(StreamConsumerError):
    """
    Raised when a stream consumer could not be initialized
    based on the configuration provided
    """
