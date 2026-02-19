from __future__ import annotations

import dataclasses

from anyio import connect_unix, fail_after
from anyio.abc import ByteStream

from coredis.typing import Unpack

from ._base import BaseConnection, BaseConnectionParams, Location


@dataclasses.dataclass(unsafe_hash=True)
class UnixDomainSocketLocation(Location):
    """Location of a redis instance listening on a unix domain socket"""

    #: The absolute path of the socket
    path: str


class UnixDomainSocketConnection(BaseConnection):
    location: UnixDomainSocketLocation

    def __init__(
        self,
        location: UnixDomainSocketLocation,
        **kwargs: Unpack[BaseConnectionParams],
    ):
        super().__init__(location, **kwargs)
        # FIXME: this is only for backward compatibility as 6.0 still had
        # path in uds connections
        self.path = self.location.path

    async def _connect(self) -> ByteStream:
        with fail_after(self._connect_timeout):
            return await connect_unix(self.location.path)

    def describe(self) -> str:
        return f"UnixDomainSocketConnection<path={self.location.path},db={self._db}>"
