from __future__ import annotations

import pytest

import coredis
from coredis.exceptions import AuthenticationError, ConnectionError, UnknownCommandError


@pytest.mark.min_server_version("6.0.0")
@pytest.mark.parametrize(
    "username, password",
    (
        ["", ""],
        ["fubar", ""],
        ["", "fubar"],
        ["fubar", "fubar"],
    ),
)
async def test_invalid_authentication(redis_auth, username, password):
    client = coredis.Redis("localhost", 6389, username=username, password=password)
    with pytest.raises(AuthenticationError):
        await client.ping()


@pytest.mark.min_server_version("6.0.0")
async def test_valid_authentication(redis_auth):
    client = coredis.Redis("localhost", 6389, password="sekret")
    assert await client.ping()


@pytest.mark.min_server_version("6.0.0")
async def test_valid_authentication_delayed(redis_auth):
    client = coredis.Redis("localhost", 6389)
    assert client.server_version is None
    with pytest.warns(UserWarning):
        await client.auth(password="sekret")
    assert await client.ping()
    assert client.server_version is not None


async def test_legacy_authentication(redis_auth, mocker):
    original_send_command = coredis.connection.BaseConnection.send_command

    async def fake_send_command(self, command, *args, **kwargs):
        if command == b"HELLO":
            raise UnknownCommandError("fubar")
        else:
            return await original_send_command(self, command, *args)

    mocker.patch.object(
        coredis.connection.BaseConnection, "send_command", fake_send_command
    )

    with pytest.warns(UserWarning, match="no support for the `HELLO` command"):
        with pytest.raises(ConnectionError):
            await coredis.Redis("localhost", 6389, password="sekret").ping()
        with pytest.raises(AuthenticationError):
            await coredis.Redis(
                "localhost",
                6389,
                username="bogus",
                password="sekret",
                protocol_version=2,
            ).ping()

        assert (
            b"PONG"
            == await coredis.Redis(
                "localhost", 6389, password="sekret", protocol_version=2
            ).ping()
        )
        assert (
            b"PONG"
            == await coredis.Redis(
                "localhost",
                6389,
                username="default",
                password="sekret",
                protocol_version=2,
            ).ping()
        )
