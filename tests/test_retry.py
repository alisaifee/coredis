from __future__ import annotations

import sys
import unittest.mock

import pytest
from exceptiongroup import ExceptionGroup

import coredis.retry
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
            ConstantRetryPolicy((ZeroDivisionError,), retries=0, delay=1),
            ExponentialBackoffRetryPolicy((ZeroDivisionError,), retries=0, base_delay=1),
            CompositeRetryPolicy(
                ConstantRetryPolicy((ZeroDivisionError,), retries=0, delay=1),
                ExponentialBackoffRetryPolicy((AttributeError,), retries=0, base_delay=1),
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
            ConstantRetryPolicy((ZeroDivisionError,), retries=1, delay=1),
            ExponentialBackoffRetryPolicy((ZeroDivisionError,), retries=1, base_delay=1),
            CompositeRetryPolicy(
                ConstantRetryPolicy((ZeroDivisionError,), retries=1, delay=1),
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
            ConstantRetryPolicy((ZeroDivisionError,), retries=1, delay=1),
            ExponentialBackoffRetryPolicy((ZeroDivisionError,), retries=1, base_delay=1),
            CompositeRetryPolicy(
                ConstantRetryPolicy((ZeroDivisionError,), retries=1, delay=1),
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
            ConstantRetryPolicy((ZeroDivisionError,), retries=2, delay=1),
            ConstantRetryPolicy((TypeError,), retries=2, delay=1),
            ConstantRetryPolicy((ValueError,), retries=1, delay=1),
        ).call_with_retries(
            mock.call,
            failure_hook={
                ArithmeticError: failure1,
                ValueError: failure2,
            },
        )

        assert failure1.await_count == 2
        assert failure2.await_count == 1

    @pytest.mark.parametrize(
        "policy",
        [
            ConstantRetryPolicy((ZeroDivisionError, ValueError), retries=2, delay=0.1),
            ExponentialBackoffRetryPolicy(
                (ZeroDivisionError, ValueError), retries=2, base_delay=0.1
            ),
            CompositeRetryPolicy(
                ConstantRetryPolicy((ZeroDivisionError,), retries=2, delay=0.1),
            ),
        ],
    )
    async def test_exception_group(self, policy):
        call = unittest.mock.Mock()
        failure = {ArithmeticError: unittest.mock.AsyncMock()}

        async def group():
            call()
            raise ExceptionGroup("", (ValueError(), ZeroDivisionError()))

        nested_call = unittest.mock.Mock()

        async def nested_group():
            nested_call()
            raise ExceptionGroup(
                "outer", [ValueError(), ExceptionGroup("inner", (ZeroDivisionError(),))]
            )

        with pytest.raises(ExceptionGroup):
            await policy.call_with_retries(group, failure_hook=failure)
        assert call.call_count == 3
        assert failure[ArithmeticError].call_count == 3

        with pytest.raises(ExceptionGroup):
            await policy.call_with_retries(nested_group, failure_hook=failure)
        assert nested_call.call_count == 3
        assert failure[ArithmeticError].call_count == 6

    @pytest.mark.parametrize(
        "policy, expected_delay",
        [
            (ExponentialBackoffRetryPolicy((ZeroDivisionError,), retries=5, base_delay=1), 31),
            (
                ExponentialBackoffRetryPolicy(
                    (ZeroDivisionError,), retries=5, base_delay=1, max_delay=4
                ),
                15,
            ),
            (
                ExponentialBackoffRetryPolicy(
                    (ZeroDivisionError,), retries=5, base_delay=1, max_delay=4, jitter=True
                ),
                14,
            ),
        ],
    )
    async def test_exponential_backoff(self, policy, expected_delay, mocker):
        async def call():
            1 / 0

        sleep = mocker.patch("coredis.retry.sleep")

        with pytest.raises(ZeroDivisionError):
            await policy.call_with_retries(call)

        total_delay = sum([k[0][0] for k in sleep.call_args_list])
        assert 0 < total_delay <= expected_delay

    @pytest.mark.parametrize(
        "policy",
        [
            ConstantRetryPolicy(
                (ZeroDivisionError, ValueError), retries=None, deadline=0.5, delay=0.1
            ),
            ExponentialBackoffRetryPolicy(
                (ZeroDivisionError, ValueError), retries=None, deadline=0.5, base_delay=0.1
            ),
            CompositeRetryPolicy(
                ConstantRetryPolicy((ZeroDivisionError,), retries=None, deadline=0.5, delay=0.01),
                ExponentialBackoffRetryPolicy(
                    (ZeroDivisionError,), retries=None, deadline=0.5, base_delay=0.1
                ),
            ),
        ],
    )
    async def test_retry_with_deadline(self, policy, mocker):
        def raise_zerodiv():
            1 / 0

        call = unittest.mock.AsyncMock(side_effect=raise_zerodiv)
        sleep = mocker.spy(coredis.retry, "sleep")
        with pytest.raises(ZeroDivisionError):
            await policy.call_with_retries(call)
        total_delay = sum([k[0][0] for k in sleep.call_args_list])
        assert 0 < total_delay <= 0.5
