from __future__ import annotations

from moto import mock_aws

from coredis.recipes import ElastiCacheIAMProvider


class TestElastiCacheIAMProvider:
    async def test_get_credentials(self):
        with mock_aws():
            provider = ElastiCacheIAMProvider("test_user", "test_cluster")
            user_pass = await provider.get_credentials()
            assert user_pass.username == "test_user"
            assert "test_cluster/?Action=connect&User=test_user" in user_pass.password
