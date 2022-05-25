from __future__ import annotations

from coredis._utils import b
from coredis.typing import ClassVar

SYM_STAR = b("*")
SYM_DOLLAR = b("$")
SYM_CRLF = b("\r\n")
SYM_LF = b("\n")
SYM_EMPTY = b("")


class RESPDataType:
    """
    Markers used by redis server to signal
    the type of data being sent.

    See:

    - `RESP protocol spec <https://redis.io/docs/reference/protocol-spec/>`__
    - `RESP3 specification <https://github.com/antirez/RESP3/blob/master/spec.md>`__
    """

    NONE: ClassVar[int] = ord(b"_")
    SIMPLE_STRING: ClassVar[int] = ord(b"+")
    BULK_STRING: ClassVar[int] = ord(b"$")
    VERBATIM: ClassVar[int] = ord(b"=")
    BOOLEAN: ClassVar[int] = ord(b"#")
    INT: ClassVar[int] = ord(b":")
    DOUBLE: ClassVar[int] = ord(b",")
    BIGNUMBER: ClassVar[int] = ord(b"(")
    ARRAY: ClassVar[int] = ord(b"*")
    PUSH: ClassVar[int] = ord(b">")
    MAP: ClassVar[int] = ord(b"%")
    SET: ClassVar[int] = ord(b"~")
    ERROR: ClassVar[int] = ord(b"-")
    ATTRIBUTE: ClassVar[int] = ord(b"|")
