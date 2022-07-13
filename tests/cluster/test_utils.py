from __future__ import annotations

import pytest

from coredis._utils import clusterdown_wrapper
from coredis.exceptions import ClusterDownError


@pytest.mark.asyncio()
async def test_clusterdown_wrapper():
    @clusterdown_wrapper
    def bad_func():
        raise ClusterDownError("CLUSTERDOWN")

    with pytest.raises(ClusterDownError) as cex:
        await bad_func()
    assert str(cex.value).startswith("CLUSTERDOWN error. Unable to rebuild the cluster")
