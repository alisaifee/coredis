from __future__ import annotations

import pytest

from coredis.cache import LRUCache


class TestLRUCache:
    def test_max_keys(self):
        cache = LRUCache(max_items=1)
        cache.insert("a", 1)
        cache.insert("b", 1)
        with pytest.raises(KeyError):
            cache.get("a")

    @pytest.mark.nopypy
    def test_max_bytes(self):
        cache = LRUCache(max_bytes=500)
        cache.insert("a", bytearray(400))
        cache.insert("b", bytearray(50))
        cache.shrink()
        cache.get("b")
        with pytest.raises(KeyError):
            cache.get("a")
