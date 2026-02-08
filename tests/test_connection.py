from __future__ import annotations

import socket
import ssl

import pytest
from anyio import create_task_group, move_on_after, sleep
from anyio.abc import SocketAttribute
from anyio.streams.tls import TLSStream

from coredis import Connection, UnixDomainSocketConnection
from coredis.credentials import UserPassCredentialProvider


async def check_request(connection, command, args, response):
    assert connection.stream is not None
    request = connection.create_request(command, *args)
    assert await request == response


async def test_connect_tcp(redis_basic):
    conn = Connection()
    assert conn.host == "127.0.0.1"
    assert conn.port == 6379
    assert str(conn) == "Connection<host=127.0.0.1,port=6379,db=0>"
    async with create_task_group() as tg:
        await tg.start(conn.run)
        await check_request(conn, b"PING", (), b"PONG")
        tg.cancel_scope.cancel()


async def test_connect_tls(redis_ssl):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    ssl_context.load_cert_chain(certfile="./tests/tls/client.crt", keyfile="./tests/tls/client.key")
    conn = Connection(host="localhost", port=8379, ssl_context=ssl_context)
    assert conn.host == "localhost"
    assert conn.port == 8379
    assert str(conn) == "Connection<host=localhost,port=8379,db=0>"
    async with create_task_group() as tg:
        await tg.start(conn.run)
        assert isinstance(conn.stream, TLSStream)
        await check_request(conn, b"PING", (), b"PONG")
        tg.cancel_scope.cancel()


async def test_connect_cred_provider(redis_auth_server):
    conn = Connection(
        credential_provider=UserPassCredentialProvider(password="sekret"),
        host="localhost",
        port=6389,
    )
    async with create_task_group() as tg:
        await tg.start(conn.run)
        await check_request(conn, b"PING", (), b"PONG")
        tg.cancel_scope.cancel()


@pytest.mark.os("linux")
async def test_connect_tcp_keepalive_options(redis_basic):
    conn = Connection(
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
    conn = Connection(
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
        await check_request(conn, b"PING", (), b"PONG")
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
        assert not conn.usable


async def test_termination(redis_basic):
    conn = Connection()
    async with create_task_group() as tg:
        await tg.start(conn.run)
        assert conn.usable
        conn.terminate()
        await sleep(0.01)
        assert not conn.usable


async def test_connection_rerun(redis_basic):
    conn = Connection()
    async with create_task_group() as tg:
        await tg.start(conn.run)
        assert conn.usable
        with pytest.raises(RuntimeError, match="cannot be reused"):
            await tg.start(conn.run)
        tg.cancel_scope.cancel()
    async with create_task_group() as tg:
        with pytest.raises(RuntimeError, match="cannot be reused"):
            await tg.start(conn.run)
