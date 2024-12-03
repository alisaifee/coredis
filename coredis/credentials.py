from __future__ import annotations

from abc import ABC, abstractmethod

from coredis.typing import NamedTuple


class PassOnly(NamedTuple):
    password: str


class UserPass(NamedTuple):
    username: str
    password: str


class AbstractCredentialProvider(ABC):
    """
    Abstract credential provider
    """

    @abstractmethod
    def get_credentials(self) -> PassOnly | UserPass:
        pass


class UserPassCredentialProvider(AbstractCredentialProvider):
    """
    Credential provider that just returns a
    :paramref:`UserPassCredentialProvider.password`
    and/or :paramref:`UserPassCredentialProvider.username`.
    """

    def __init__(
        self, username: str | None = None, password: str | None = None
    ) -> None:
        self.username = username or ""
        self.password = password or ""

    def get_credentials(self) -> PassOnly | UserPass:
        return (
            PassOnly(self.password)
            if not self.username
            else UserPass(self.username, self.password)
        )
