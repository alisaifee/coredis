from __future__ import annotations

from ._base import BaseConnection, BaseConnectionParams, Location
from ._cluster import ClusterConnection
from ._sentinel import SentinelManagedConnection
from ._tcp import TCPConnection, TCPLocation
from ._uds import UnixDomainSocketConnection, UnixDomainSocketLocation

#: For backward compatibility
Connection = TCPConnection
__all__ = [
    "BaseConnection",
    "BaseConnectionParams",
    "ClusterConnection",
    "Connection",
    "Location",
    "SentinelManagedConnection",
    "TCPConnection",
    "TCPLocation",
    "UnixDomainSocketConnection",
    "UnixDomainSocketLocation",
]
