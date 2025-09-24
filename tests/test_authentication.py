from __future__ import annotations

import pytest

import coredis
from coredis.credentials import UserPassCredentialProvider
from coredis.exceptions import AuthenticationError, ConnectionError

pytestmark = pytest.mark.anyio


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
    async with client:
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
    async with client:
        with pytest.raises(AuthenticationError):
            await client.ping()


async def test_valid_authentication(redis_auth):
    client = coredis.Redis("localhost", 6389, password="sekret")
    async with client:
        assert await client.ping()


async def test_valid_authentication_cred_provider(redis_auth_cred_provider):
    client = coredis.Redis(
        "localhost",
        6389,
        credential_provider=UserPassCredentialProvider(password="sekret"),
    )
    async with client:
        assert await client.ping()


async def test_valid_authentication_delayed(redis_auth):
    client = coredis.Redis("localhost", 6389)
    assert client.server_version is None
    async with client:
        with pytest.warns(UserWarning):
            await client.auth(password="sekret")
        assert await client.ping()
        assert client.server_version is not None


async def test_legacy_authentication(redis_auth):
    with pytest.warns(UserWarning, match="no support for the `HELLO` command"):
        with pytest.raises(ConnectionError):
            async with coredis.Redis("localhost", 6389, password="sekret") as client:
                await client.ping()
        with pytest.raises(AuthenticationError):
            async with coredis.Redis(
                "localhost",
                6389,
                username="bogus",
                password="sekret",
                protocol_version=2,
            ) as client:
                await client.ping()

        async with coredis.Redis(
            "localhost", 6389, password="sekret", protocol_version=2
        ) as client:
            assert await client.ping() == b"PONG"
        async with coredis.Redis(
            "localhost",
            6389,
            username="default",
            password="sekret",
            protocol_version=2,
        ) as client:
            assert await client.ping() == b"PONG"


async def test_legacy_authentication_cred_provider(redis_auth_cred_provider, mocker):
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
