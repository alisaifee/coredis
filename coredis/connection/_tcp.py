from __future__ import annotations

import socket

from anyio import connect_tcp, fail_after
from anyio.abc import ByteStream, SocketAttribute
from typing_extensions import NotRequired, Unpack

from ._base import BaseConnection, BaseConnectionParams


class Connection(BaseConnection):
    class Params(BaseConnectionParams):
        """
        :meta private:
        """

        host: NotRequired[str]
        port: NotRequired[int]

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        *,
        socket_keepalive: bool | None = None,
        socket_keepalive_options: dict[int, int | bytes] | None = None,
        **kwargs: Unpack[BaseConnectionParams],
    ):
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self._socket_keepalive = socket_keepalive
        self._socket_keepalive_options: dict[int, int | bytes] = socket_keepalive_options or {}

    async def _connect(self) -> ByteStream:
        with fail_after(self._connect_timeout):
            if self._ssl_context:
                connection: ByteStream = await connect_tcp(
                    self.host,
                    self.port,
                    tls=True,
                    ssl_context=self._ssl_context,
                    tls_standard_compatible=False,
                )
            else:
                connection = await connect_tcp(self.host, self.port)
            sock = connection.extra(SocketAttribute.raw_socket, default=None)
            if sock is not None:
                if self._socket_keepalive:  # TCP_KEEPALIVE
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    for k, v in self._socket_keepalive_options.items():
                        sock.setsockopt(socket.SOL_TCP, k, v)
            return connection

    def describe(self) -> str:
        return f"Connection<host={self.host},port={self.port},db={self._db}>"

    @property
    def location(self) -> str:
        return f"host={self.host},port={self.port}"
