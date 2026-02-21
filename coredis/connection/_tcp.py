from __future__ import annotations

import dataclasses
import socket

from anyio import connect_tcp, fail_after
from anyio.abc import ByteStream, SocketAttribute

from coredis.typing import Unpack

from ._base import BaseConnection, BaseConnectionParams, Location


@dataclasses.dataclass(unsafe_hash=True)
class TCPLocation(Location):
    """Location of a redis instance listening on a tcp port"""

    #: hostname of the server
    host: str
    #: the port the server is listening on
    port: int

    def __repr__(self) -> str:
        return f"<host={self.host},port={self.port}>"


class TCPConnection(BaseConnection):
    location: TCPLocation

    def __init__(
        self,
        location: TCPLocation,
        *,
        socket_keepalive: bool | None = None,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        **kwargs: Unpack[BaseConnectionParams],
    ):
        super().__init__(location, **kwargs)
        self._socket_keepalive = socket_keepalive
        self._socket_keepalive_options: dict[int, int | bytes] = socket_keepalive_options or {}
        # FIXME: this is only for backward compatibility as 6.0 still had
        # host/port in TCP connections
        self.host = self.location.host
        self.port = self.location.port

    async def _connect(self) -> ByteStream:
        with fail_after(self._connect_timeout):
            if self._ssl_context:
                connection: ByteStream = await connect_tcp(
                    self.location.host,
                    self.location.port,
                    tls=True,
                    ssl_context=self._ssl_context,
                    tls_standard_compatible=False,
                )
            else:
                connection = await connect_tcp(self.location.host, self.location.port)
            sock = connection.extra(SocketAttribute.raw_socket, default=None)
            if sock is not None:
                if self._socket_keepalive:  # TCP_KEEPALIVE
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    for k, v in self._socket_keepalive_options.items():
                        sock.setsockopt(socket.SOL_TCP, k, v)
            return connection

    def describe(self) -> str:
        return f"Connection<host={self.location.host},port={self.location.port},db={self._db}>"
