from __future__ import annotations

import sys
import unittest.mock

import pytest

from coredis.retry import (
    CompositeRetryPolicy,
    ConstantRetryPolicy,
    ExponentialBackoffRetryPolicy,
)


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.8 or higher")
class TestRetryPolicies:
    @pytest.mark.parametrize(
        "policy",
        [
            ConstantRetryPolicy((ZeroDivisionError,), 0, 1),
            ExponentialBackoffRetryPolicy((ZeroDivisionError,), 0, 1),
            CompositeRetryPolicy(
                ConstantRetryPolicy((ZeroDivisionError,), 0, 1),
                ExponentialBackoffRetryPolicy((AttributeError,), 0, 1),
            ),
        ],
    )
    async def test_no_exception(self, policy):
        call = unittest.mock.AsyncMock()
        failure = unittest.mock.AsyncMock()
        before = unittest.mock.AsyncMock()
        await policy.call_with_retries(call, before_hook=before, failure_hook=failure)
        call.assert_awaited_once()
        before.assert_awaited_once()
        failure.assert_not_awaited()

    @pytest.mark.parametrize(
        "policy",
        [
            ConstantRetryPolicy((ZeroDivisionError,), 1, 1),
            ExponentialBackoffRetryPolicy((ZeroDivisionError,), 1, 1),
            CompositeRetryPolicy(
                ConstantRetryPolicy((ZeroDivisionError,), 1, 1),
            ),
        ],
    )
    async def test_exception(self, policy):
        def raise_zerodiv():
            1 / 0

        call = unittest.mock.AsyncMock(side_effect=raise_zerodiv)
        failure = unittest.mock.AsyncMock()
        before = unittest.mock.AsyncMock()

        with pytest.raises(ZeroDivisionError):
            await policy.call_with_retries(call, before_hook=before, failure_hook=failure)

        assert before.await_count == 2
        assert call.await_count == 2
        assert failure.await_count == 2

    @pytest.mark.parametrize(
        "policy",
        [
            ConstantRetryPolicy((ZeroDivisionError,), 1, 1),
            ExponentialBackoffRetryPolicy((ZeroDivisionError,), 1, 1),
            CompositeRetryPolicy(
                ConstantRetryPolicy((ZeroDivisionError,), 1, 1),
            ),
        ],
    )
    async def test_exception_with_mapped_failure_hook(self, policy):
        def raise_zerodiv():
            1 / 0

        call = unittest.mock.AsyncMock(side_effect=raise_zerodiv)
        failure = {ArithmeticError: unittest.mock.AsyncMock()}

        with pytest.raises(ZeroDivisionError):
            await policy.call_with_retries(call, failure_hook=failure)

        assert call.await_count == 2
        assert failure[ArithmeticError].await_count == 2

    async def test_composite_retry(self):
        class Mock:
            def __init__(self):
                self.state = None

            async def call(self):
                if self.state is None:
                    self.state = 0
                elif self.state == 0:
                    self.state = str(self.state)
                elif self.state == "0":
                    self.state = "one"
                elif self.state == "one":
                    self.state = 1
                return 1 / int(self.state)

        mock = Mock()
        failure1 = unittest.mock.AsyncMock()
        failure2 = unittest.mock.AsyncMock()
        assert 1 == await CompositeRetryPolicy(
            ConstantRetryPolicy((ZeroDivisionError,), 2, 1),
            ConstantRetryPolicy((TypeError,), 2, 1),
            ConstantRetryPolicy((ValueError,), 1, 1),
        ).call_with_retries(
            mock.call,
            failure_hook={
                ArithmeticError: failure1,
                ValueError: failure2,
            },
        )

        assert failure1.await_count == 2
        assert failure2.await_count == 1
