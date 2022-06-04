from __future__ import annotations

from coredis._utils import b
from coredis.typing import Final

SYM_STAR: Final[bytes] = b("*")
SYM_DOLLAR: Final[bytes] = b("$")
SYM_CRLF: Final[bytes] = b("\r\n")
SYM_LF: Final[bytes] = b("\n")
SYM_EMPTY: Final[bytes] = b("")


class RESPDataType:
    """
    Markers used by redis server to signal
    the type of data being sent.

    See:

    - `RESP protocol spec <https://redis.io/docs/reference/protocol-spec/>`__
    - `RESP3 specification <https://github.com/antirez/RESP3/blob/master/spec.md>`__
    """

    NONE: Final[int] = ord(b"_")
    SIMPLE_STRING: Final[int] = ord(b"+")
    BULK_STRING: Final[int] = ord(b"$")
    VERBATIM: Final[int] = ord(b"=")
    BOOLEAN: Final[int] = ord(b"#")
    INT: Final[int] = ord(b":")
    DOUBLE: Final[int] = ord(b",")
    BIGNUMBER: Final[int] = ord(b"(")
    ARRAY: Final[int] = ord(b"*")
    PUSH: Final[int] = ord(b">")
    MAP: Final[int] = ord(b"%")
    SET: Final[int] = ord(b"~")
    ERROR: Final[int] = ord(b"-")
    ATTRIBUTE: Final[int] = ord(b"|")
