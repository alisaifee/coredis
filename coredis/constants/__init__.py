from __future__ import annotations

from coredis._utils import b
from coredis.typing import Final

from .resp_types import RESPDataType

SYM_STAR: Final[bytes] = b("*")
SYM_DOLLAR: Final[bytes] = b("$")
SYM_CRLF: Final[bytes] = b("\r\n")
SYM_LF: Final[bytes] = b("\n")
SYM_EMPTY: Final[bytes] = b("")

__all__ = ["RESPDataType"]
