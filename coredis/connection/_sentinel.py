from __future__ import annotations

from typing import cast

from coredis.exceptions import StalePrimaryError
from coredis.typing import ResponseType, Unpack

from ._base import BaseConnectionParams
from ._tcp import TCPConnection, TCPLocation


class SentinelManagedConnection(TCPConnection):
    def __init__(
        self,
        location: TCPLocation,
        primary_name: str,
        is_primary: bool,
        **kwargs: Unpack[BaseConnectionParams],
    ):
        self.primary_name = primary_name
        self.is_primary = is_primary
        super().__init__(location=location, **kwargs)
        if self.is_primary:
            self.register_connect_callback(self._verify_primary_role)

    async def _verify_primary_role(self, connection: SentinelManagedConnection) -> None:
        role = cast(list[ResponseType], await connection.create_request(b"ROLE", decode=True))
        if not role[0] == "master":
            raise StalePrimaryError(
                f"Primary {self.primary_name} at {self.location} reports a role of {role[0]!r}"
            )

    def __repr__(self) -> str:
        host_info = f",host={self.location.host},port={self.location.port}"
        return f"{type(self).__name__}<service={self.primary_name}{host_info}>"
