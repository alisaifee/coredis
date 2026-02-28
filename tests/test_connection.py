from __future__ import annotations

import math
import socket
import ssl

import pytest
from anyio import create_task_group, move_on_after, sleep
from anyio.abc import SocketAttribute
from anyio.streams.tls import TLSStream

from coredis import TCPConnection, UnixDomainSocketConnection
from coredis.credentials import UserPassCredentialProvider


async def check_request(connection, command, args, response):
    assert connection.stream is not None
    request = connection.create_request(command, *args)
    assert await request == response


async def test_connect_tcp(redis_basic):
    conn = TCPConnection(location=redis_basic.connection_pool.location)
    assert (
        str(conn)
        == f"Connection<host={redis_basic.connection_pool.location.host},port={redis_basic.connection_pool.location.port},db=0>"
    )
    async with create_task_group() as tg:
        await tg.start(conn.run)
        await check_request(conn, b"PING", (), b"PONG")
        tg.cancel_scope.cancel()


async def test_connection_statistics(redis_basic):
    conn = TCPConnection(location=redis_basic.connection_pool.location)
    assert math.isnan(conn.statistics.rtt)
    assert math.isnan(conn.statistics.ttfb)
    async with create_task_group() as tg:
        await tg.start(conn.run)
        assert conn.statistics.rtt > 0
        assert conn.statistics.ttfb > 0
        assert conn.statistics.requests_pending == 0
        requests = [conn.create_request(b"PING") for _ in range(10)]
        assert conn.statistics.requests_pending == 10
        [await r for r in requests]
        assert conn.statistics.requests_pending == 0
        tg.cancel_scope.cancel()


async def test_connect_tls(redis_ssl):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    ssl_context.load_cert_chain(certfile="./tests/tls/client.crt", keyfile="./tests/tls/client.key")
    conn = TCPConnection(location=redis_ssl.connection_pool.location, ssl_context=ssl_context)
    assert str(conn) == "Connection<host=localhost,port=8379,db=0>"
    async with create_task_group() as tg:
        await tg.start(conn.run)
        assert isinstance(conn.stream, TLSStream)
        await check_request(conn, b"PING", (), b"PONG")
        tg.cancel_scope.cancel()


async def test_connect_cred_provider(redis_auth):
    conn = TCPConnection(
        location=redis_auth.connection_pool.location,
        credential_provider=UserPassCredentialProvider(password="sekret"),
    )
    async with create_task_group() as tg:
        await tg.start(conn.run)
        await check_request(conn, b"PING", (), b"PONG")
        tg.cancel_scope.cancel()


@pytest.mark.os("linux")
async def test_connect_tcp_keepalive_options(redis_basic):
    conn = TCPConnection(
        location=redis_basic.connection_pool.location,
        socket_keepalive=True,
        socket_keepalive_options={socket.TCP_KEEPINTVL: 1, socket.TCP_KEEPCNT: 3},
    )
    async with create_task_group() as tg:
        await tg.start(conn.run)
        sock = conn.stream.extra(SocketAttribute.raw_socket)
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 1
        for k, v in ((socket.TCP_KEEPINTVL, 1), (socket.TCP_KEEPCNT, 3)):
            assert sock.getsockopt(socket.SOL_TCP, k) == v
        tg.cancel_scope.cancel()


@pytest.mark.os("darwin")
async def test_connect_tcp_keepalive_options_mac(redis_basic):
    conn = TCPConnection(
        location=redis_basic.connection_pool.location,
        socket_keepalive=True,
        socket_keepalive_options={socket.TCP_KEEPINTVL: 1, socket.TCP_KEEPCNT: 3},
    )
    async with create_task_group() as tg:
        await tg.start(conn.run)
        sock = conn.stream.extra(SocketAttribute.raw_socket)
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 8
        for k, v in ((socket.TCP_KEEPINTVL, 1), (socket.TCP_KEEPCNT, 3)):
            assert sock.getsockopt(socket.SOL_TCP, k) == v
        tg.cancel_scope.cancel()


@pytest.mark.parametrize("option", ["UNKNOWN", 999])
async def test_connect_tcp_wrong_socket_opt_raises(option, redis_basic):
    conn = TCPConnection(
        location=redis_basic.connection_pool.location,
        socket_keepalive=True,
        socket_keepalive_options={option: 1},
    )
    with pytest.raises((socket.error, TypeError)):
        await conn._connect()


# only test during dev
async def test_connect_unix_socket(redis_uds):
    conn = UnixDomainSocketConnection(location=redis_uds.connection_pool.location)
    async with create_task_group() as tg:
        await tg.start(conn.run)
        assert conn.location.path == redis_uds.connection_pool.location.path
        assert (
            str(conn)
            == f"UnixDomainSocketConnection<path={redis_uds.connection_pool.location.path},db=0>"
        )
        await check_request(conn, b"PING", (), b"PONG")
        tg.cancel_scope.cancel()


async def test_stream_timeout(redis_basic):
    conn = TCPConnection(redis_basic.connection_pool.location, stream_timeout=0.01)
    async with create_task_group() as tg:
        await tg.start(conn.run)
        req = conn.create_request(b"debug", "sleep", 0.05)
        with pytest.raises(TimeoutError):
            await req
        tg.cancel_scope.cancel()


async def test_request_cancellation(redis_basic):
    conn = TCPConnection(redis_basic.connection_pool.location)
    async with create_task_group() as tg:
        await tg.start(conn.run)
        request = conn.create_request(b"blpop", 1, "key", 1, disconnect_on_cancellation=True)
        with move_on_after(0.01):
            await request
        await sleep(0.01)
        assert not conn.usable


async def test_termination(redis_basic):
    conn = TCPConnection(redis_basic.connection_pool.location)
    async with create_task_group() as tg:
        await tg.start(conn.run)
        assert conn.usable
        conn.terminate()
        await sleep(0.01)
        assert not conn.usable


async def test_connection_rerun(redis_basic):
    conn = TCPConnection(redis_basic.connection_pool.location)
    async with create_task_group() as tg:
        await tg.start(conn.run)
        assert conn.usable
        with pytest.raises(RuntimeError, match="cannot be reused"):
            await tg.start(conn.run)
        tg.cancel_scope.cancel()
    async with create_task_group() as tg:
        with pytest.raises(RuntimeError, match="cannot be reused"):
            await tg.start(conn.run)
