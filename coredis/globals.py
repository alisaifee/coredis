from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from coredis.commands._routing import RoutingStrategy
    from coredis.commands.constants import CommandFlag
    from coredis.modules.base import ModuleGroupRegistry, ModuleRegistry

#: Populated by the @redis_command wrapper
READONLY_COMMANDS: set[bytes] = set()
#: Populated by the @redis_command wrapper
COMMAND_FLAGS: dict[bytes, set[CommandFlag]] = defaultdict(set)

#: Populated by the @redis_command wrapper
ROUTING_STRATEGIES: dict[bytes, RoutingStrategy[Any]] = {}

#: Populated by the @redis_command wrapper
CACHEABLE_COMMANDS: set[bytes] = set()

#: Populated by ModuleGroupRegistry
MODULE_GROUPS: set[ModuleGroupRegistry] = set()

#: Populated by ModuleRegistry
MODULES: dict[str, ModuleRegistry] = {}
