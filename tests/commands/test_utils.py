from __future__ import annotations

import datetime

from coredis.commands._utils import (
    normalized_time_milliseconds,
    normalized_time_seconds,
)


class TestNormalizedTime:
    def test_int_passthrough(self):
        assert normalized_time_seconds(42) == 42
        assert normalized_time_milliseconds(42) == 42

    def test_aware_utc(self):
        # 2012-12-12 12:00:00 UTC. The previous mktime(timetuple()) path dropped
        # tzinfo and treated the wall clock as local time.
        dt = datetime.datetime(2012, 12, 12, 12, 0, 0, tzinfo=datetime.timezone.utc)
        assert normalized_time_seconds(dt) == 1_355_313_600
        assert normalized_time_milliseconds(dt) == 1_355_313_600_000

    def test_aware_fixed_offset(self):
        # 2012-12-12 12:00:00.500 +05:30 == 06:30:00.500 UTC
        offset = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        dt = datetime.datetime(2012, 12, 12, 12, 0, 0, 500_000, tzinfo=offset)
        assert normalized_time_seconds(dt) == 1_355_293_800
        assert normalized_time_milliseconds(dt) == 1_355_293_800_500

    def test_aware_subsecond_milliseconds(self):
        dt = datetime.datetime(2012, 12, 12, 12, 0, 0, 123_456, tzinfo=datetime.timezone.utc)
        assert normalized_time_milliseconds(dt) == 1_355_313_600_123
