from __future__ import annotations

import asyncio

from tests.conftest import targets


@targets("redis_basic", "redis_basic_blocking")
class TestMonitor:
    async def test_explicit_fetch(self, client, cloner):
        monitored = await cloner(client)
        await monitored.ping()
        async with await client.monitor() as monitor:
            response = await asyncio.gather(monitor.get_command(), monitored.get("test"))
            assert response[0].command == "GET"
            response = await asyncio.gather(monitor.get_command(), monitored.get("test2"))
            assert response[0].command == "GET"
        assert not monitor.monitoring

    async def test_iterator(self, client):
        async def delayed():
            await asyncio.sleep(0.1)
            return await client.get("test")

        async def collect():
            results = []
            async for command in client.monitor():
                results.append(command)
                break
            return results

        results = await asyncio.gather(delayed(), collect())
        assert results[1][0].command in ["HELLO", "GET"]

    async def test_monitor_request_handler(self, client, mocker):
        cmds = set()

        monitor = await client.monitor(lambda cmd: cmds.add(cmd.command))
        await asyncio.sleep(0.01)
        await client.ping()
        await asyncio.sleep(0.01)
        await monitor.aclose()
        assert "PING" in cmds
        await asyncio.sleep(0.01)
        await client.get("test")
        await asyncio.sleep(0.01)
        assert "GET" not in cmds
