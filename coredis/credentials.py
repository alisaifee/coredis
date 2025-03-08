from __future__ import annotations

from abc import ABC, abstractmethod

from coredis.typing import NamedTuple


class UserPass(NamedTuple):
    """
    Username/password tuple.
    """

    #: Username
    username: str
    #: Password
    password: str


class AbstractCredentialProvider(ABC):
    """
    Abstract credential provider
    """

    @abstractmethod
    async def get_credentials(self) -> UserPass:
        """
        Returns an instance of :class:`coredis.credentials.UserPass` for
        establishing a connection to the redis server.
        """
        pass


class UserPassCredentialProvider(AbstractCredentialProvider):
    """
    Credential provider that just returns a
    :paramref:`UserPassCredentialProvider.password`
    and/or :paramref:`UserPassCredentialProvider.username`.
    """

    def __init__(self, username: str | None = None, password: str | None = None) -> None:
        self.username = username or ""
        self.password = password or ""

    async def get_credentials(self) -> UserPass:
        return UserPass(self.username or "default", self.password)
