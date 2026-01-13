from __future__ import annotations

from deprecated.sphinx import versionadded

from coredis.commands import CommandMixin
from coredis.typing import AnyStr

from .autocomplete import Autocomplete
from .filters import (
    BloomFilter,
    CountMinSketch,
    CuckooFilter,
    RedisBloom,
    TDigest,
    TopK,
)
from .json import Json, RedisJSON
from .search import RediSearch, Search
from .timeseries import RedisTimeSeries, TimeSeries


class ModuleMixin(CommandMixin[AnyStr]):
    @property
    @versionadded(version="4.12.0")
    def json(self) -> Json[AnyStr]:
        """
        Property to access :class:`~coredis.modules.Json` commands.
        """
        return Json(self)

    @property
    @versionadded(version="4.12.0")
    def bf(self) -> BloomFilter[AnyStr]:
        """
        Property to access :class:`~coredis.modules.BloomFilter` commands.
        """
        return BloomFilter(self)

    @property
    @versionadded(version="4.12.0")
    def cf(self) -> CuckooFilter[AnyStr]:
        """
        Property to access :class:`~coredis.modules.CuckooFilter` commands.
        """
        return CuckooFilter(self)

    @property
    @versionadded(version="4.12.0")
    def cms(self) -> CountMinSketch[AnyStr]:
        """
        Property to access :class:`~coredis.modules.CountMinSketch` commands.
        """
        return CountMinSketch(self)

    @property
    @versionadded(version="4.12.0")
    def tdigest(self) -> TDigest[AnyStr]:
        """
        Property to access :class:`~coredis.modules.TDigest` commands.
        """
        return TDigest(self)

    @property
    @versionadded(version="4.12.0")
    def topk(self) -> TopK[AnyStr]:
        """
        Property to access :class:`~coredis.modules.TopK` commands.
        """
        return TopK(self)

    @property
    @versionadded(version="4.12.0")
    def timeseries(self) -> TimeSeries[AnyStr]:
        """
        Property to access :class:`~coredis.modules.TimeSeries` commands.
        """
        return TimeSeries(self)

    @property
    @versionadded(version="4.12.0")
    def search(self) -> Search[AnyStr]:
        """
        Property to access :class:`~coredis.modules.Search` commands.
        """
        return Search(self)

    @property
    @versionadded(version="4.12.0")
    def autocomplete(self) -> Autocomplete[AnyStr]:
        """
        Property to access :class:`~coredis.modules.Autocomplete` commands.
        """
        return Autocomplete(self)


__all__ = [
    "RediSearch",
    "RedisBloom",
    "RedisJSON",
    "RedisTimeSeries",
    "Json",
    "BloomFilter",
    "CuckooFilter",
    "CountMinSketch",
    "TopK",
    "TDigest",
    "TimeSeries",
]
