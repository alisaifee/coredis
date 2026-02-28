from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

from coredis.commands._routing import RoutingStrategy
from coredis.commands.constants import CommandFlag, NodeFlag

if TYPE_CHECKING:
    from coredis.modules.base import ModuleGroupRegistry, ModuleRegistry

#: Populated by the @redis_command wrapper
READONLY_COMMANDS: set[bytes] = set()
#: Populated by the @redis_command wrapper
COMMAND_FLAGS: dict[bytes, set[CommandFlag]] = defaultdict(set)

#: Populated by the @redis_command wrapper
ROUTE_FLAGS: dict[bytes, NodeFlag] = {}

#: Populated by the @redis_command wrapper
SPLIT_FLAGS: dict[bytes, NodeFlag] = {}
ROUTING_STRATEGIES: dict[bytes, RoutingStrategy[Any]] = {}

#: Populated by the @redis_command wrapper
CACHEABLE_COMMANDS: set[bytes] = set()

#: Populated by ModuleGroupRegistry
MODULE_GROUPS: set[ModuleGroupRegistry] = set()

#: Populated by ModuleRegistry
MODULES: dict[str, ModuleRegistry] = {}
