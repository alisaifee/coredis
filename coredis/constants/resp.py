"""
RESP protocol constants
"""

from __future__ import annotations

import enum
from typing import Final

from coredis._utils import b


class DataType(enum.IntEnum):
    """
    Markers used by redis server to signal
    the type of data being sent.

    See:

    - `RESP protocol spec <https://redis.io/docs/develop/reference/protocol-spec>`__
    - `RESP3 specification <https://github.com/antirez/RESP3/blob/master/spec.md>`__
    """

    NONE = ord(b"_")
    SIMPLE_STRING = ord(b"+")
    BULK_STRING = ord(b"$")
    VERBATIM = ord(b"=")
    BOOLEAN = ord(b"#")
    INT = ord(b":")
    DOUBLE = ord(b",")
    BIGNUMBER = ord(b"(")
    ARRAY = ord(b"*")
    PUSH = ord(b">")
    MAP = ord(b"%")
    SET = ord(b"~")
    ERROR = ord(b"-")
    ATTRIBUTE = ord(b"|")


SYM_STAR: Final[bytes] = b("*")
SYM_DOLLAR: Final[bytes] = b("$")
SYM_CRLF: Final[bytes] = b("\r\n")
SYM_LF: Final[bytes] = b("\n")
SYM_EMPTY: Final[bytes] = b("")
SYM_TRUE: Final[bytes] = b("t")
