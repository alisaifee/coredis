from __future__ import annotations

from anyio import connect_unix, fail_after
from anyio.abc import ByteStream

from coredis.typing import Unpack

from ._base import BaseConnection, BaseConnectionParams


class UnixDomainSocketConnection(BaseConnection):
    class Params(BaseConnectionParams):
        """
        :meta private:
        """

        path: str

    def __init__(self, path: str = "", **kwargs: Unpack[BaseConnectionParams]) -> None:
        super().__init__(**kwargs)
        self.path = path

    async def _connect(self) -> ByteStream:
        with fail_after(self._connect_timeout):
            return await connect_unix(self.path)

    def describe(self) -> str:
        return f"UnixDomainSocketConnection<path={self.path},db={self._db}>"

    @property
    def location(self) -> str:
        return f"path={self.path}"
