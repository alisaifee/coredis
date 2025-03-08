from __future__ import annotations

import pytest

from coredis.credentials import UserPassCredentialProvider


class TestCredentialProviders:
    @pytest.mark.parametrize(
        "expected_creds",
        [
            ("user1", "12345abcedf"),
            ("", "mypassword"),
            ("someuser", ""),
            ("", ""),
        ],
    )
    async def test_credential_provider(self, expected_creds):
        provider = UserPassCredentialProvider(*expected_creds)

        actual_creds = await provider.get_credentials()

        assert actual_creds.username == expected_creds[0] or "default"
        assert actual_creds.password == expected_creds[1]
