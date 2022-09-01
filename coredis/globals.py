from __future__ import annotations

from collections import defaultdict

from coredis.commands.constants import CommandFlag
from coredis.typing import Dict, Set

#: Populated by the @redis_command wrapper
READONLY_COMMANDS: Set[bytes] = set()
#: Populated by the @redis_command wrapper
COMMAND_FLAGS: Dict[bytes, Set[CommandFlag]] = defaultdict(lambda: set())
