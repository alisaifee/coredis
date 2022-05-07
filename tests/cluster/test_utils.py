from __future__ import annotations

import pytest

from coredis._utils import clusterdown_wrapper, first_key
from coredis.exceptions import ClusterDownError, RedisClusterException


def test_first_key():
    assert first_key({"foo": 1}) == 1

    with pytest.raises(RedisClusterException) as ex:
        first_key({"foo": 1, "bar": 2})
    assert str(ex.value).startswith("More then 1 result from command")


@pytest.mark.asyncio()
async def test_clusterdown_wrapper():
    @clusterdown_wrapper
    def bad_func():
        raise ClusterDownError("CLUSTERDOWN")

    with pytest.raises(ClusterDownError) as cex:
        await bad_func()
    assert str(cex.value).startswith("CLUSTERDOWN error. Unable to rebuild the cluster")
