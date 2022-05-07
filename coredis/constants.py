from __future__ import annotations

import enum

from coredis._utils import b

SYM_STAR = b("*")
SYM_DOLLAR = b("$")
SYM_CRLF = b("\r\n")
SYM_LF = b("\n")
SYM_EMPTY = b("")


class RESPDataType(enum.IntEnum):
    """
    Markers used by redis server to signal
    the type of data being sent.

    See:

    - `RESP protocol spec <https://redis.io/docs/reference/protocol-spec/>`__
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
