from __future__ import annotations

import dataclasses
import math
import time


@dataclasses.dataclass
class ConnectionStatistics:
    """
    Connection Statistics
    """

    created_at: float = dataclasses.field(init=False, default_factory=lambda: time.perf_counter())
    connected_at: float = dataclasses.field(init=False, default=-math.inf)
    first_byte_received_at: float = dataclasses.field(init=False, default=-math.inf)
    last_byte_received_at: float = dataclasses.field(init=False, default=-math.inf)
    last_byte_written_at: float = dataclasses.field(init=False, default=-math.inf)
    last_request_created_at: float = dataclasses.field(init=False, default=-math.inf)
    last_request_resolved_at: float = dataclasses.field(init=False, default=-math.inf)
    bytes_read: int = 0
    bytes_written: int = 0
    requests_created: int = 0
    requests_resolved: int = 0

    @property
    def rtt(self) -> float:
        """
        Approximate RTT to the node, measured by connection
        establishment duration.
        """
        if math.isinf(self.connected_at):
            return math.nan
        return self.connected_at - self.created_at

    @property
    def ttfb(self) -> float:
        """
        Time to receive first byte since the connection was established
        """
        if math.isinf(self.first_byte_received_at):
            return math.nan
        return self.first_byte_received_at - self.connected_at

    @property
    def requests_pending(self) -> float:
        """
        Total requests still awaiting a response to be resolved
        """
        return self.requests_created - self.requests_resolved

    def connected(self) -> None:
        if math.isinf(self.connected_at):
            self.connected_at = time.perf_counter()

    def data_received(self, num_bytes: int) -> None:
        now = time.perf_counter()
        if math.isinf(self.first_byte_received_at):
            self.first_byte_received_at = now
        self.last_byte_received_at = now
        self.bytes_read += num_bytes

    def data_sent(self, num_bytes: int) -> None:
        now = time.perf_counter()
        self.bytes_written += num_bytes
        self.last_byte_written_at = now

    def request_created(self) -> None:
        now = time.perf_counter()
        self.last_request_created_at = now
        self.requests_created += 1

    def request_resolved(self) -> None:
        now = time.perf_counter()
        self.last_request_resolved_at = now
        self.requests_resolved += 1
