from __future__ import annotations

from coredis.typing import ManagedNode, Unpack

from ._base import BaseConnectionParams
from ._tcp import Connection


class ClusterConnection(Connection):
    "Manages TCP communication to and from a Redis Cluster node"

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        *,
        read_from_replicas: bool = False,
        **kwargs: Unpack[BaseConnectionParams],
    ) -> None:
        self._read_from_replicas = read_from_replicas
        super().__init__(
            host=host,
            port=port,
            **kwargs,
        )
        self.node = ManagedNode(host=host, port=port)

    async def perform_handshake(self) -> None:
        """
        Read only cluster connections need to explicitly
        request readonly during handshake
        """
        await super().perform_handshake()
        if self._read_from_replicas:
            assert (await self.create_request(b"READONLY", decode=False)) == b"OK"

    def describe(self) -> str:
        return f"ClusterConnection<path={self.host},db={self.port}>"

    @property
    def location(self) -> str:
        return f"host={self.host},port={self.port}"
