from __future__ import annotations

from coredis.commands._validators import mutually_exclusive_parameters
from coredis.tokens import PureToken
from coredis.typing import CommandArgList, StringT

__all__ = ["Predicate"]


class Predicate:
    """
    Field definition to be used in :meth:`~coredis.Redis.argrep`.

    EXACT string — Matches elements whose value is exactly equal to string.
    MATCH string — Matches elements whose value contains string as a substring.
    GLOB pattern — Matches elements whose value matches the glob-style pattern (with *,
     ?, and [...] wildcards), the same syntax used by KEYS and SCAN MATCH.
    RE pattern — Matches elements whose value matches the regular expression pattern.

    Usage::

        Predicate(glob="a*") | Predicate(match="b") & Predicate(exact="cob")
    """

    @mutually_exclusive_parameters("exact", "match", "glob", "re", required=True)
    def __init__(
        self,
        *,
        exact: StringT | None = None,
        match: StringT | None = None,
        glob: StringT | None = None,
        re: StringT | None = None,
    ) -> None:
        self._tokens: CommandArgList = []
        if exact:
            self._tokens.extend([PureToken.EXACT, exact])
        elif match:
            self._tokens.extend([PureToken.MATCH, match])
        elif glob:
            self._tokens.extend([PureToken.GLOB, glob])
        elif re:  # always reachable, but help type checker out
            self._tokens.extend([PureToken.RE, re])

    @classmethod
    def _combine(cls, left: Predicate, op: PureToken, right: Predicate) -> Predicate:
        combined = cls.__new__(cls)
        combined._tokens = [*left._tokens, op, *right._tokens]
        return combined

    def __or__(self, other: Predicate) -> Predicate:
        return self._combine(self, PureToken.OR, other)

    def __and__(self, other: Predicate) -> Predicate:
        return self._combine(self, PureToken.AND, other)

    @property
    def args(self) -> CommandArgList:
        return self._tokens
