from __future__ import annotations

import socket

import pytest
from anyio import create_task_group
from anyio.abc import SocketAttribute

from coredis import Connection, ConnectionPool, UnixDomainSocketConnection
from coredis.credentials import UserPassCredentialProvider
from coredis.exceptions import TimeoutError

pytestmark = pytest.mark.anyio


async def test_connect_tcp(redis_basic):
    conn = Connection()
    pool = ConnectionPool()
    assert conn.host == "127.0.0.1"
    assert conn.port == 6379
    assert str(conn) == "Connection<host=127.0.0.1,port=6379,db=0>"
    async with pool:
        async with create_task_group() as tg:
            await tg.start(conn.run, pool)
            request = await conn.create_request(b"PING")
            res = await request
            assert res == b"PONG"
            assert conn._connection is not None
            tg.cancel_scope.cancel()


async def test_connect_cred_provider(redis_auth_cred_provider):
    conn = Connection(
        credential_provider=UserPassCredentialProvider(password="sekret"),
        host="localhost",
        port=6389,
    )
    pool = ConnectionPool()
    assert conn.host == "localhost"
    assert conn.port == 6389
    assert str(conn) == "Connection<host=localhost,port=6389,db=0>"
    async with pool:
        async with create_task_group() as tg:
            await tg.start(conn.run, pool)
            request = await conn.create_request(b"PING")
            res = await request
            assert res == b"PONG"
            assert conn._connection is not None
            tg.cancel_scope.cancel()


@pytest.mark.os("linux")
async def test_connect_tcp_keepalive_options(redis_basic):
    conn = Connection(
        socket_keepalive=True,
        socket_keepalive_options={socket.TCP_KEEPINTVL: 1, socket.TCP_KEEPCNT: 3},
    )
    await conn._connect()
    async with conn.connection:
        sock = conn.connection.extra(SocketAttribute.raw_socket)
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 1
        for k, v in ((socket.TCP_KEEPINTVL, 1), (socket.TCP_KEEPCNT, 3)):
            assert sock.getsockopt(socket.SOL_TCP, k) == v


@pytest.mark.parametrize("option", ["UNKNOWN", 999])
async def test_connect_tcp_wrong_socket_opt_raises(option, redis_basic):
    conn = Connection(socket_keepalive=True, socket_keepalive_options={option: 1})
    with pytest.raises((socket.error, TypeError)):
        await conn._connect()


# only test during dev
async def test_connect_unix_socket(redis_uds):
    path = "/tmp/coredis.redis.sock"
    conn = UnixDomainSocketConnection(path)
    pool = ConnectionPool()
    async with pool:
        async with create_task_group() as tg:
            await tg.start(conn.run, pool)
            assert conn.path == path
            assert str(conn) == f"UnixDomainSocketConnection<path={path},db=0>"
            req = await conn.create_request(b"PING")
            res = await req
            assert res == b"PONG"
            assert conn._connection is not None
            tg.cancel_scope.cancel()


async def test_stream_timeout(redis_basic):
    conn = Connection(stream_timeout=0.01)
    pool = ConnectionPool()
    async with pool:
        async with create_task_group() as tg:
            await tg.start(conn.run, pool)
            req = await conn.create_request(b"debug", "sleep", 0.05)
            with pytest.raises(TimeoutError):
                await req
            tg.cancel_scope.cancel()
