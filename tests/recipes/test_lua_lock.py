from __future__ import annotations

import time

import pytest

from coredis.exceptions import LockError
from coredis.recipes.locks import LuaLock
from tests.conftest import targets


@pytest.mark.asyncio
@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_basic_resp2",
    "redis_cluster",
    "redis_cluster_raw",
    "redis_cluster_resp2",
)
class TestLock:
    async def test_lock(self, client, _s):
        lock = LuaLock(client, "foo", blocking=False)
        assert await lock.acquire()
        assert await client.get("foo") == _s(lock.local.get())
        assert await client.ttl("foo") == -1
        await lock.release()
        assert await client.get("foo") is None

    async def test_competing_locks(self, client):
        lock1 = LuaLock(client, "foo", blocking=False)
        lock2 = LuaLock(client, "foo", blocking=False)
        assert await lock1.acquire()
        assert not await lock2.acquire()
        await lock1.release()
        assert await lock2.acquire()
        assert not await lock1.acquire()
        await lock2.release()

    async def test_timeout(self, client):
        lock = LuaLock(client, "foo", timeout=10, blocking=False)
        assert await lock.acquire()
        assert 8 < await client.ttl("foo") <= 10
        await lock.release()

    async def test_float_timeout(self, client):
        lock = LuaLock(
            client,
            "foo",
            blocking=False,
            timeout=9.5,
        )
        assert await lock.acquire()
        assert 8 < await client.pttl("foo") <= 9500
        await lock.release()

    async def test_blocking_timeout(self, client):
        lock1 = LuaLock(client, "foo", blocking=False)
        assert await lock1.acquire()
        lock2 = LuaLock(
            client,
            "foo",
            blocking_timeout=0.2,
        )
        start = time.time()
        assert not await lock2.acquire()
        assert (time.time() - start) > 0.2
        await lock1.release()

    async def test_context_manager(self, client, _s):
        # blocking_timeout prevents a deadlock if the lock can't be acquired
        # for some reason
        async with LuaLock(
            client,
            "foo",
            blocking_timeout=0.2,
        ) as lock:
            assert await client.get("foo") == _s(lock.local.get())
        assert await client.get("foo") is None

    async def test_high_sleep_raises_error(self, client):
        "If sleep is higher than timeout, it should raise an error"
        with pytest.raises(LockError):
            LuaLock(
                client,
                "foo",
                timeout=1,
                sleep=2,
            )

    async def test_releasing_unlocked_lock_raises_error(self, client):
        lock = LuaLock(
            client,
            "foo",
        )
        with pytest.raises(LockError):
            await lock.release()

    async def test_releasing_lock_no_longer_owned_raises_error(self, client):
        lock = LuaLock(client, "foo", blocking=False)
        await lock.acquire()
        # manually change the token
        await client.set("foo", "a")
        with pytest.raises(LockError):
            await lock.release()
        # even though we errored, the token is still cleared
        assert lock.local.get() is None

    async def test_extend_lock(self, client):
        lock = LuaLock(
            client,
            "foo",
            blocking=False,
            timeout=10,
        )
        assert await lock.acquire()
        assert 8000 < await client.pttl("foo") <= 10000
        assert await lock.extend(10)
        assert 16000 < await client.pttl("foo") <= 20000
        await lock.release()

    async def test_extend_lock_float(self, client):
        lock = LuaLock(
            client,
            "foo",
            blocking=False,
            timeout=10.0,
        )
        assert await lock.acquire()
        assert 8000 < await client.pttl("foo") <= 10000
        assert await lock.extend(10.0)
        assert 16000 < await client.pttl("foo") <= 20000
        await lock.release()

    async def test_extending_unlocked_lock_raises_error(self, client):
        lock = LuaLock(
            client,
            "foo",
            timeout=10,
        )
        with pytest.raises(LockError):
            await lock.extend(10)

    async def test_extending_lock_with_no_timeout_raises_error(self, client):
        lock = LuaLock(client, "foo", blocking=False)
        await client.flushdb()
        assert await lock.acquire()
        with pytest.raises(LockError):
            await lock.extend(10)
        await lock.release()

    async def test_extending_lock_no_longer_owned_raises_error(self, client):
        lock = LuaLock(client, "foo", blocking=False)
        await client.flushdb()
        assert await lock.acquire()
        await client.set("foo", "a")
        with pytest.raises(LockError):
            await lock.extend(10)
