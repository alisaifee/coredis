from __future__ import annotations

import socket

import pytest
from anyio import create_task_group, move_on_after, sleep
from anyio.abc import SocketAttribute

from coredis import Connection, UnixDomainSocketConnection
from coredis.credentials import UserPassCredentialProvider


async def test_connect_tcp(redis_basic):
    conn = Connection()
    assert conn.host == "127.0.0.1"
    assert conn.port == 6379
    assert str(conn) == "Connection<host=127.0.0.1,port=6379,db=0>"
    async with create_task_group() as tg:
        await tg.start(conn.run)
        request = conn.create_request(b"PING")
        res = await request
        assert res == b"PONG"
        assert conn._connection is not None
        tg.cancel_scope.cancel()


async def test_connect_cred_provider(redis_auth_server):
    conn = Connection(
        credential_provider=UserPassCredentialProvider(password="sekret"),
        host="localhost",
        port=6389,
    )
    async with create_task_group() as tg:
        await tg.start(conn.run)
        request = conn.create_request(b"PING")
        res = await request
        assert res == b"PONG"
        tg.cancel_scope.cancel()


@pytest.mark.os("linux")
async def test_connect_tcp_keepalive_options(redis_basic):
    conn = Connection(
        socket_keepalive=True,
        socket_keepalive_options={socket.TCP_KEEPINTVL: 1, socket.TCP_KEEPCNT: 3},
    )
    async with create_task_group() as tg:
        await tg.start(conn.run)
        sock = conn.connection.extra(SocketAttribute.raw_socket)
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 1
        for k, v in ((socket.TCP_KEEPINTVL, 1), (socket.TCP_KEEPCNT, 3)):
            assert sock.getsockopt(socket.SOL_TCP, k) == v
        tg.cancel_scope.cancel()


@pytest.mark.os("darwin")
async def test_connect_tcp_keepalive_options_mac(redis_basic):
    conn = Connection(
        socket_keepalive=True,
        socket_keepalive_options={socket.TCP_KEEPINTVL: 1, socket.TCP_KEEPCNT: 3},
    )
    async with create_task_group() as tg:
        await tg.start(conn.run)
        sock = conn.connection.extra(SocketAttribute.raw_socket)
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 8
        for k, v in ((socket.TCP_KEEPINTVL, 1), (socket.TCP_KEEPCNT, 3)):
            assert sock.getsockopt(socket.SOL_TCP, k) == v
        tg.cancel_scope.cancel()


@pytest.mark.parametrize("option", ["UNKNOWN", 999])
async def test_connect_tcp_wrong_socket_opt_raises(option, redis_basic):
    conn = Connection(socket_keepalive=True, socket_keepalive_options={option: 1})
    with pytest.raises((socket.error, TypeError)):
        await conn._connect()


# only test during dev
async def test_connect_unix_socket(redis_uds):
    path = "/tmp/coredis.redis.sock"
    conn = UnixDomainSocketConnection(path)
    async with create_task_group() as tg:
        await tg.start(conn.run)
        assert conn.path == path
        assert str(conn) == f"UnixDomainSocketConnection<path={path},db=0>"
        req = conn.create_request(b"PING")
        res = await req
        assert res == b"PONG"
        assert conn._connection is not None
        tg.cancel_scope.cancel()


async def test_stream_timeout(redis_basic):
    conn = Connection(stream_timeout=0.01)
    async with create_task_group() as tg:
        await tg.start(conn.run)
        req = conn.create_request(b"debug", "sleep", 0.05)
        with pytest.raises(TimeoutError):
            await req
        tg.cancel_scope.cancel()


async def test_request_cancellation(redis_basic):
    conn = Connection()
    async with create_task_group() as tg:
        await tg.start(conn.run)
        request = conn.create_request(b"blpop", 1, "key", 1, disconnect_on_cancellation=True)
        with move_on_after(0.01):
            await request
        await sleep(0.01)
        assert not conn.is_connected


async def test_shared_pool(redis_basic):
    clone = redis_basic.__class__(
        decode_responses=redis_basic.decode_responses,
        encoding=redis_basic.encoding,
        connection_pool=redis_basic.connection_pool,
    )
    async with clone:
        assert await clone.client_id() == await redis_basic.client_id()


async def test_shared_pool_cluster(redis_cluster):
    clone = redis_cluster.__class__(
        decode_responses=redis_cluster.decode_responses,
        encoding=redis_cluster.encoding,
        connection_pool=redis_cluster.connection_pool,
    )
    assert clone.connection_pool is redis_cluster.connection_pool
    await redis_cluster.get("key{a}")
    await redis_cluster.get("key{b}")
    before = {c for c in redis_cluster.connection_pool._connections._queue}
    async with clone:
        await clone.get("key{a}")
        await clone.get("key{b}")
        after = {c for c in clone.connection_pool._connections._queue}
        assert before == after


async def test_termination(redis_basic):
    conn = Connection()
    async with create_task_group() as tg:
        await tg.start(conn.run)
        assert conn.is_connected
        conn.terminate()
        await sleep(0.01)
        assert not conn.is_connected


async def test_connection_rerun(redis_basic):
    conn = Connection()
    async with create_task_group() as tg:
        await tg.start(conn.run)
        assert conn.is_connected
        with pytest.raises(RuntimeError, match="cannot be reused"):
            await tg.start(conn.run)
        tg.cancel_scope.cancel()
    async with create_task_group() as tg:
        with pytest.raises(RuntimeError, match="cannot be reused"):
            await tg.start(conn.run)
