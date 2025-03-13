from __future__ import annotations

import asyncio

import pytest

from coredis._sidecar import Sidecar
from tests.conftest import targets

pytestmark = pytest.mark.asyncio


@targets("redis_basic", "redis_basic_blocking", "redis_basic_raw")
class TestSidecar:
    async def test_noop_sidecar(self, client):
        sidecar = Sidecar(set(), health_check_interval_seconds=1)
        assert sidecar.connection is None
        await sidecar.start(client)
        assert sidecar.connection is not None
        await asyncio.sleep(0.1)
        sidecar.stop()
        assert sidecar.last_checkin > 0
        assert sidecar.connection is None

    async def test_pubsub_sidecar(self, client, _s):
        sidecar = Sidecar({b"subscribe", b"message"}, health_check_interval_seconds=1)
        assert sidecar.connection is None
        await sidecar.start(client)
        assert sidecar.connection is not None
        await sidecar.connection.send_command(b"SUBSCRIBE", b"fubar")
        await client.publish("fubar", "test")
        m1 = await sidecar.messages.get()
        m2 = await sidecar.messages.get()
        assert m1[0] == b"subscribe"
        assert m2[0] == b"message"
        sidecar.stop()

    async def test_sidecar_reconnect(self, client, _s):
        sidecar = Sidecar(set(), health_check_interval_seconds=1)
        assert sidecar.connection is None
        await sidecar.start(client)
        assert sidecar.connection is not None
        sidecar.connection.disconnect()
        assert not sidecar.connection.is_connected
        await asyncio.sleep(0.5)
        assert sidecar.connection is not None
        assert sidecar.connection.is_connected
        sidecar.stop()

    async def test_finalization(self, client, cloner):
        running_tasks = asyncio.all_tasks()

        async def scoped_client():
            clone = await cloner(client)
            sidecar = Sidecar(set(), health_check_interval_seconds=1)
            await sidecar.start(clone)

        await scoped_client()
        await asyncio.sleep(0.1)
        assert set() == asyncio.all_tasks() - running_tasks
