from __future__ import annotations

import enum

from coredis.typing import StringT


@enum.unique
class CaseAndEncodingInsensitiveEnum(bytes, enum.Enum):
    __decoded: set[StringT]

    @property
    def variants(self) -> set[StringT]:
        if not hasattr(self, "__decoded"):
            decoded = str(self)
            self.__decoded = {
                self.value.lower(),  # type: ignore
                self.value,  # type: ignore
                decoded.lower(),
                decoded.upper(),
            }
        return self.__decoded

    def __eq__(self, other: object) -> bool:
        """
        Since redis tokens are case insensitive allow mixed case
        Additionally allow strings to be passed in instead of
        bytes.
        """

        if other:
            if isinstance(other, self.__class__):
                return bool(self.value == other.value)
            else:
                return other in self.variants
        return False

    def __str__(self) -> str:
        return self.decode("latin-1")

    def __hash__(self) -> int:
        return hash(self.value)
