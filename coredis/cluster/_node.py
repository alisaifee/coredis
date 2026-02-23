from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any

from coredis.connection import ClusterConnection, TCPLocation
from coredis.typing import Literal, Unpack

if TYPE_CHECKING:
    from coredis import Redis
    from coredis.pool import ConnectionPoolParams


@dataclasses.dataclass(unsafe_hash=True)
class ClusterNodeLocation(TCPLocation):
    """
    Represents a cluster node (primary or replica) in a redis cluster
    """

    server_type: Literal["primary", "replica"] | None = None
    node_id: str | None = None

    @property
    def name(self) -> str:
        return f"{self.host}:{self.port}"

    def as_client(
        self, **client_args: Unpack[ConnectionPoolParams[ClusterConnection]]
    ) -> Redis[Any]:
        from coredis import Redis
        from coredis.pool import ConnectionPool

        pool = ConnectionPool[ClusterConnection](
            connection_class=ClusterConnection,
            location=TCPLocation(self.host, self.port),
            decode_responses=client_args.get("decode_responses", False),
            encoding=client_args.get("encoding", "utf-8"),
            username=client_args.get("username"),
            password=client_args.get("password"),
            credential_provider=client_args.get("credential_provider"),
            ssl_context=client_args.get("ssl_context"),
            stream_timeout=client_args.get("stream_timeout"),
            connect_timeout=client_args.get("connect_timeout"),
        )
        return Redis(connection_pool=pool)
