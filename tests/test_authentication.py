from __future__ import annotations

import pytest

import coredis
from coredis.credentials import UserPassCredentialProvider
from coredis.exceptions import AuthenticationError


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
        await client.auth(password="sekret")
        assert await client.ping()
