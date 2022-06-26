from __future__ import annotations

import socket

import pytest

from coredis import Connection, UnixDomainSocketConnection
from coredis.exceptions import TimeoutError

pytest_marks = pytest.mark.asyncio


async def test_connect_tcp(event_loop, redis_basic):
    conn = Connection(loop=event_loop)
    assert conn.host == "127.0.0.1"
    assert conn.port == 6379
    assert str(conn) == "Connection<host=127.0.0.1,port=6379,db=0>"
    await conn.send_command(b"PING")
    res = await conn.read_response()
    assert res == b"PONG"
    assert (conn._reader is not None) and (conn._writer is not None)
    conn.disconnect()
    assert (conn._reader is None) and (conn._writer is None)


@pytest.mark.os("linux")
async def test_connect_tcp_keepalive_options(event_loop, redis_basic):
    conn = Connection(
        loop=event_loop,
        socket_keepalive=True,
        socket_keepalive_options={
            socket.TCP_KEEPIDLE: 1,
            socket.TCP_KEEPINTVL: 1,
            socket.TCP_KEEPCNT: 3,
        },
    )
    await conn._connect()
    sock = conn._writer.transport.get_extra_info("socket")
    assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 1
    for k, v in (
        (socket.TCP_KEEPIDLE, 1),
        (socket.TCP_KEEPINTVL, 1),
        (socket.TCP_KEEPCNT, 3),
    ):
        assert sock.getsockopt(socket.SOL_TCP, k) == v
    conn.disconnect()


@pytest.mark.parametrize("option", ["UNKNOWN", 999])
async def test_connect_tcp_wrong_socket_opt_raises(event_loop, option, redis_basic):
    conn = Connection(
        loop=event_loop, socket_keepalive=True, socket_keepalive_options={option: 1}
    )
    with pytest.raises((socket.error, TypeError)):
        await conn._connect()
    # verify that the connection isn't left open
    assert conn._writer.transport.is_closing()


# only test during dev
async def test_connect_unix_socket(redis_uds):
    path = "/tmp/coredis.redis.sock"
    conn = UnixDomainSocketConnection(path)
    await conn.connect()
    assert conn.path == path
    assert str(conn) == f"UnixDomainSocketConnection<path={path},db=0>"
    await conn.send_command(b"PING")
    res = await conn.read_response()
    assert res == b"PONG"
    assert (conn._reader is not None) and (conn._writer is not None)
    conn.disconnect()
    assert (conn._reader is None) and (conn._writer is None)


async def test_stream_timeout(redis_basic):
    conn = Connection(stream_timeout=0)
    with pytest.raises(TimeoutError):
        await conn.connect()

    conn = Connection(stream_timeout=1)
    assert await conn.connect() is None

    conn = Connection(stream_timeout=0.01)
    await conn.connect() is None
    await conn.send_command(b"debug", "sleep", 0.05)
    with pytest.raises(TimeoutError):
        await conn.read_response()
