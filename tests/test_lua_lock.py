from __future__ import annotations

import time
import uuid

import pytest

from coredis.exceptions import LockError
from coredis.lock import Lock
from tests.conftest import targets


@pytest.fixture
def lock_name():
    return uuid.uuid4().hex


@targets(
    "redis_basic",
    "redis_basic_raw",
    "redis_cluster",
    "redis_cluster_noreplica",
    "redis_cluster_raw",
)
class TestLock:
    async def test_client_construction(self, client, _s, lock_name):
        lock = client.lock(lock_name, blocking=False)
        assert await lock.acquire()
        await lock.release()
        assert await client.get(lock_name) is None

    async def test_lock(self, client, _s, lock_name):
        lock = Lock(client, lock_name, blocking=False)
        assert await lock.acquire()
        assert await client.get(lock_name) == _s(lock.local.get())
        assert await client.ttl(lock_name) == -1
        await lock.release()
        assert await client.get(lock_name) is None

    async def test_competing_locks(self, client, lock_name):
        lock1 = Lock(client, lock_name, blocking=False)
        lock2 = Lock(client, lock_name, blocking=False)
        assert await lock1.acquire()
        assert not await lock2.acquire()
        await lock1.release()
        assert await lock2.acquire()
        assert not await lock1.acquire()
        await lock2.release()

    async def test_timeout(self, client, lock_name):
        lock = Lock(client, lock_name, timeout=10, blocking=False)
        assert await lock.acquire()
        assert 8 < await client.ttl(lock_name) <= 10
        await lock.release()

    async def test_float_timeout(self, client, lock_name):
        lock = Lock(
            client,
            lock_name,
            blocking=False,
            timeout=9.5,
        )
        assert await lock.acquire()
        assert 8 < await client.pttl(lock_name) <= 9500
        await lock.release()

    async def test_blocking_timeout(self, client, lock_name):
        lock1 = Lock(client, lock_name, blocking=False)
        assert await lock1.acquire()
        lock2 = Lock(
            client,
            lock_name,
            blocking_timeout=0.2,
        )
        start = time.time()
        assert not await lock2.acquire()
        assert (time.time() - start) > 0.2
        await lock1.release()

    async def test_context_manager(self, client, _s, lock_name):
        # blocking_timeout prevents a deadlock if the lock can't be acquired
        # for some reason
        async with Lock(
            client,
            lock_name,
            blocking_timeout=0.2,
        ) as lock:
            assert await client.get(lock_name) == _s(lock.local.get())
        assert await client.get(lock_name) is None

    async def test_high_sleep_raises_error(self, client, lock_name):
        "If sleep is higher than timeout, it should raise an error"
        with pytest.raises(LockError):
            Lock(
                client,
                lock_name,
                timeout=1,
                sleep=2,
            )

    async def test_releasing_unlocked_lock_raises_error(self, client, lock_name):
        lock = Lock(
            client,
            lock_name,
        )
        with pytest.raises(LockError):
            await lock.release()

    async def test_releasing_lock_no_longer_owned_raises_error(self, client, lock_name):
        lock = Lock(client, lock_name, blocking=False)
        await lock.acquire()
        # manually change the token
        await client.set(lock_name, "a")
        with pytest.raises(LockError):
            await lock.release()
        # even though we errored, the token is still cleared
        assert lock.local.get() is None

    async def test_extend_lock(self, client, lock_name):
        lock = Lock(
            client,
            lock_name,
            blocking=False,
            timeout=10,
        )
        assert await lock.acquire()
        assert 8000 < await client.pttl(lock_name) <= 10000
        assert await lock.extend(10)
        assert 16000 < await client.pttl(lock_name) <= 20000
        await lock.release()

    async def test_extend_lock_float(self, client, lock_name):
        lock = Lock(
            client,
            lock_name,
            blocking=False,
            timeout=10.0,
        )
        assert await lock.acquire()
        assert 8000 < await client.pttl(lock_name) <= 10000
        assert await lock.extend(10.0)
        assert 16000 < await client.pttl(lock_name) <= 20000
        await lock.release()

    async def test_extending_unlocked_lock_raises_error(self, client, lock_name):
        lock = Lock(
            client,
            lock_name,
            timeout=10,
        )
        with pytest.raises(LockError):
            await lock.extend(10)

    async def test_extending_lock_with_no_timeout_raises_error(self, client, lock_name):
        lock = Lock(client, lock_name, blocking=False)
        await client.flushdb()
        assert await lock.acquire()
        with pytest.raises(LockError):
            await lock.extend(10)
        await lock.release()

    async def test_extending_lock_no_longer_owned_raises_error(self, client, lock_name):
        lock = Lock(client, lock_name, blocking=False)
        await client.flushdb()
        assert await lock.acquire()
        await client.set(lock_name, "a")
        with pytest.raises(LockError):
            await lock.extend(10)
