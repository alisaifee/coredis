from __future__ import annotations

import pytest

from coredis.cache import LRUCache
from coredis.commands.constants import CommandName


class TestLRUCache:
    def test_max_keys(self):
        cache = LRUCache(max_keys=1)
        cache.put(CommandName.GET, "a", value="1")
        cache.put(CommandName.GET, "b", value="1")
        with pytest.raises(KeyError):
            cache.get(CommandName.GET, "a")
