from __future__ import annotations

from coredis.constants import SYM_CRLF, SYM_DOLLAR, SYM_EMPTY, SYM_STAR
from coredis.typing import List, Tuple, ValueT


class Packer:
    def __init__(self, encoding: str):
        self.encoding = encoding

    def encode(self, value: ValueT) -> bytes:
        """Returns a bytestring representation of the value"""
        if isinstance(value, str):
            return value.encode(self.encoding)
        elif isinstance(value, int):
            return b"%d" % value
        elif isinstance(value, float):
            return b"%.15g" % value
        return value

    def pack_command(self, command: bytes, *args: ValueT) -> List[bytes]:
        "Pack a series of arguments into the Redis protocol"
        output: List[bytes] = []
        # the client might have included 1 or more literal arguments in
        # the command name, e.g., 'CONFIG GET'. The Redis server expects these
        # arguments to be sent separately, so split the first argument
        # manually. All of these arguements get wrapped in the Token class
        # to prevent them from being encoded.
        if b" " in command:
            args = tuple(s for s in command.split()) + args
        else:
            args = (command,) + args

        buff = SYM_EMPTY.join((SYM_STAR, b"%d" % len(args), SYM_CRLF))

        for arg in args:
            if not isinstance(arg, bytes):
                arg = self.encode(arg)
            # to avoid large string mallocs, chunk the command into the
            # output list if we're sending large values

            if len(buff) > 6000 or len(arg) > 6000:
                buff = SYM_EMPTY.join((buff, SYM_DOLLAR, b"%d" % len(arg), SYM_CRLF))
                output.append(buff)
                output.append(arg)
                buff = SYM_CRLF
            else:
                buff = SYM_EMPTY.join(
                    (buff, SYM_DOLLAR, b"%d" % len(arg), SYM_CRLF, arg, SYM_CRLF)
                )
        output.append(buff)
        return output

    def pack_commands(self, commands: List[Tuple[ValueT, ...]]) -> List[bytes]:
        output: List[bytes] = []
        pieces: List[bytes] = []
        buffer_length = 0

        for cmd in commands:
            for chunk in self.pack_command(self.encode(cmd[0]), *cmd[1:]):
                pieces.append(chunk)
                buffer_length += len(chunk)

            if buffer_length > 6000:
                output.append(SYM_EMPTY.join(pieces))
                buffer_length = 0
                pieces = []

        if pieces:
            output.append(SYM_EMPTY.join(pieces))

        return output
