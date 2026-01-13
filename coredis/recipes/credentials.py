from __future__ import annotations

from urllib.parse import ParseResult, urlencode, urlunparse

# aiobotocore, botocore, asyncache & cachetools will need to be installed in addition
# to coredis dependencies. These can also be requested by installing coredis
# as coredis[recipes]
import aiobotocore.session
from aiobotocore.signers import AioRequestSigner
from asyncache import cached
from botocore.model import ServiceId
from cachetools import TTLCache

from coredis.credentials import AbstractCredentialProvider, UserPass


class ElastiCacheIAMProvider(AbstractCredentialProvider):
    """
    Credential provider that uses IAM authentication
    to connect to an Elasticache instance.
    """

    def __init__(self, user: str, cluster_name: str, region: str = "us-east-1") -> None:
        self.user: str = user
        self.cluster_name: str = cluster_name
        self.region: str = region

        self.session = aiobotocore.session.get_session()

    @cached(cache=TTLCache(maxsize=128, ttl=900))  # type: ignore[misc]
    async def get_credentials(self) -> UserPass:
        """
        Returns a short-lived token that can be used to connect to an
        IAM enabled Elasticache instance. The token will be cached for
        its lifetime (15 minutes) to avoid unnecessary requests.
        """
        request_signer = AioRequestSigner(
            ServiceId("elasticache"),
            self.region,
            "elasticache",
            "v4",
            await self.session.get_credentials(),
            self.session.get_component("event_emitter"),
        )
        query_params = {"Action": "connect", "User": self.user}
        url = urlunparse(
            ParseResult(
                scheme="https",
                netloc=self.cluster_name,
                path="/",
                query=urlencode(query_params),
                params="",
                fragment="",
            )
        )
        signed_url = await request_signer.generate_presigned_url(
            {"method": "GET", "url": url, "body": {}, "headers": {}, "context": {}},
            operation_name="connect",
            expires_in=900,
            region_name=self.region,
        )
        # Need to strip the protocol so that Elasticache accepts it
        return UserPass(self.user, signed_url.removeprefix("https://"))
