from __future__ import annotations

from coredis.typing import Unpack

from ._base import BaseConnectionParams
from ._tcp import TCPConnection, TCPLocation


class SentinelManagedConnection(TCPConnection):
    def __init__(
        self, location: TCPLocation, primary_name: str, **kwargs: Unpack[BaseConnectionParams]
    ):
        self.primary_name = primary_name
        super().__init__(location=location, **kwargs)

    def __repr__(self) -> str:
        if self.location:
            host_info = f",host={self.location.host},port={self.location.port}"
        else:
            host_info = ""
        return f"{type(self).__name__}<service={self.primary_name}{host_info}>"
