from __future__ import annotations

from coredis.typing import Unpack

from ._base import BaseConnectionParams
from ._tcp import TCPConnection, TCPLocation


class ClusterConnection(TCPConnection):
    "Manages TCP communication to and from a Redis Cluster node"

    def __init__(
        self,
        location: TCPLocation,
        *,
        read_from_replicas: bool = False,
        **kwargs: Unpack[BaseConnectionParams],
    ) -> None:
        self._read_from_replicas = read_from_replicas
        super().__init__(location, **kwargs)

    async def perform_handshake(self) -> None:
        """
        Read only cluster connections need to explicitly
        request readonly during handshake
        """
        await super().perform_handshake()
        if self._read_from_replicas:
            assert (await self.create_request(b"READONLY", decode=False)) == b"OK"

    def describe(self) -> str:
        return f"ClusterConnection<path={self.location.host},db={self.location.port}>"
