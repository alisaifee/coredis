from __future__ import annotations

import asyncio

import pytest

import coredis
from coredis.credentials import UserPassCredentialProvider
from coredis.exceptions import AuthenticationError, ConnectionError, UnknownCommandError


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


@pytest.mark.parametrize(
    "username, password",
    (
        ["", ""],
        ["fubar", ""],
        ["", "fubar"],
        ["fubar", "fubar"],
    ),
)
async def test_invalid_authentication_cred_provider(redis_auth_cred_provider, username, password):
    client = coredis.Redis(
        "localhost",
        6389,
        credential_provider=UserPassCredentialProvider(username=username, password=password),
    )
    with pytest.raises(AuthenticationError):
        await client.ping()


async def test_valid_authentication(redis_auth):
    client = coredis.Redis("localhost", 6389, password="sekret")
    assert await client.ping()


async def test_valid_authentication_cred_provider(redis_auth_cred_provider):
    client = coredis.Redis(
        "localhost",
        6389,
        credential_provider=UserPassCredentialProvider(password="sekret"),
    )
    assert await client.ping()


async def test_valid_authentication_delayed(redis_auth):
    client = coredis.Redis("localhost", 6389)
    assert client.server_version is None
    with pytest.warns(UserWarning):
        await client.auth(password="sekret")
    assert await client.ping()
    assert client.server_version is not None


async def test_legacy_authentication(redis_auth, mocker):
    original_request = coredis.connection.BaseConnection.create_request

    async def fake_request(self, command, *args, **kwargs):
        fut = asyncio.get_running_loop().create_future()
        if command == b"HELLO":
            fut.set_exception(UnknownCommandError("fubar"))
            return fut
        else:
            return await original_request(self, command, *args)

    mocker.patch.object(coredis.connection.BaseConnection, "create_request", fake_request)

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
            == await coredis.Redis("localhost", 6389, password="sekret", protocol_version=2).ping()
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


async def test_legacy_authentication_cred_provider(redis_auth_cred_provider, mocker):
    original_request = coredis.connection.BaseConnection.create_request

    async def fake_request(self, command, *args, **kwargs):
        fut = asyncio.get_running_loop().create_future()
        if command == b"HELLO":
            fut.set_exception(UnknownCommandError("fubar"))
            return fut
        else:
            return await original_request(self, command, *args)

    mocker.patch.object(coredis.connection.BaseConnection, "create_request", fake_request)

    with pytest.warns(UserWarning, match="no support for the `HELLO` command"):
        with pytest.raises(ConnectionError):
            await coredis.Redis(
                "localhost",
                6389,
                credential_provider=UserPassCredentialProvider(password="sekret"),
            ).ping()
        with pytest.raises(AuthenticationError):
            await coredis.Redis(
                "localhost",
                6389,
                credential_provider=UserPassCredentialProvider(username="bogus", password="sekret"),
                protocol_version=2,
            ).ping()

        assert (
            b"PONG"
            == await coredis.Redis(
                "localhost",
                6389,
                credential_provider=UserPassCredentialProvider(password="sekret"),
                protocol_version=2,
            ).ping()
        )
        assert (
            b"PONG"
            == await coredis.Redis(
                "localhost",
                6389,
                credential_provider=UserPassCredentialProvider(
                    username="default", password="sekret"
                ),
                protocol_version=2,
            ).ping()
        )
