from __future__ import annotations

from coredis.typing import Set

#: Populated by the @redis_command wrapper
READONLY_COMMANDS: Set[bytes] = set()
