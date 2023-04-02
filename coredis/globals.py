from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING

from coredis.commands.constants import CommandFlag
from coredis.typing import Dict, Set

if TYPE_CHECKING:
    from coredis.modules.base import ModuleGroupRegistry
#: Populated by the @redis_command wrapper
READONLY_COMMANDS: Set[bytes] = set()
#: Populated by the @redis_command wrapper
COMMAND_FLAGS: Dict[bytes, Set[CommandFlag]] = defaultdict(lambda: set())

#: Populated by ModuleMeta
MODULE_GROUPS: Set[ModuleGroupRegistry] = set()
