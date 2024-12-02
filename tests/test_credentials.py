from __future__ import annotations

import pytest

from coredis.credentials import PassOnly, UserPassCredentialProvider


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
    def test_credential_provider(self, expected_creds):
        provider = UserPassCredentialProvider(*expected_creds)

        actual_creds = provider.get_credentials()

        if isinstance(actual_creds, PassOnly):
            assert actual_creds.password == expected_creds[1]
        else:
            assert actual_creds == expected_creds
