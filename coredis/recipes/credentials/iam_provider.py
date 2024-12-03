from __future__ import annotations

from urllib.parse import ParseResult, urlencode, urlunparse

# botocore and cachetools will need to be installed in addition
# to coredis' dependencies
import botocore.session
from botocore.model import ServiceId
from botocore.signers import RequestSigner
from cachetools import TTLCache, cached

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

        session = botocore.session.get_session()
        self.request_signer = RequestSigner(
            ServiceId("elasticache"),
            self.region,
            "elasticache",
            "v4",
            session.get_credentials(),
            session.get_component("event_emitter"),
        )

    @cached(cache=TTLCache(maxsize=128, ttl=900))
    async def get_credentials(self) -> UserPass:
        """
        Returns a short-lived token that can be used to connect to an
        IAM enabled Elasticache instance. The token will be cached for
        its lifetime (15 minutes) to avoid unnecessary requests.
        """
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
        signed_url = self.request_signer.generate_presigned_url(
            {"method": "GET", "url": url, "body": {}, "headers": {}, "context": {}},
            operation_name="connect",
            expires_in=900,
            region_name=self.region,
        )

        # Need to strip the protocol so that Elasticache accepts it
        return UserPass(self.user, signed_url.removeprefix("https://"))
