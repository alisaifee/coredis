from __future__ import annotations

import pytest

from coredis.exceptions import ClusterDownError, RedisClusterException
from coredis.utils import clusterdown_wrapper, first_key, merge_result


def test_merge_result():
    assert merge_result({"a": [1, 2, 3], "b": [4, 5, 6]}) == [1, 2, 3, 4, 5, 6]
    assert merge_result({"a": [1, 2, 3], "b": [1, 2, 3]}) == [1, 2, 3]


def test_merge_result_value_error():
    with pytest.raises(ValueError):
        merge_result([])


def test_first_key():
    assert first_key({"foo": 1}) == 1

    with pytest.raises(RedisClusterException) as ex:
        first_key({"foo": 1, "bar": 2})
    assert str(ex.value).startswith("More then 1 result from command")


def test_first_key_value_error():
    with pytest.raises(ValueError):
        first_key(None)


@pytest.mark.asyncio()
async def test_clusterdown_wrapper():
    @clusterdown_wrapper
    def bad_func():
        raise ClusterDownError("CLUSTERDOWN")

    with pytest.raises(ClusterDownError) as cex:
        await bad_func()
    assert str(cex.value).startswith("CLUSTERDOWN error. Unable to rebuild the cluster")
