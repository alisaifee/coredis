from __future__ import annotations

import datetime
import time
from typing import Union


def normalized_seconds(value: Union[int, datetime.timedelta]) -> int:
    if isinstance(value, datetime.timedelta):
        value = value.seconds + value.days * 24 * 3600

    return value


def normalized_milliseconds(value: Union[int, datetime.timedelta]) -> int:
    if isinstance(value, datetime.timedelta):
        ms = int(value.microseconds / 1000)
        value = (value.seconds + value.days * 24 * 3600) * 1000 + ms

    return value


def normalized_time_seconds(value: Union[int, datetime.datetime]) -> int:
    if isinstance(value, datetime.datetime):
        s = int(value.microsecond / 1000000)
        value = int(time.mktime(value.timetuple())) + s

    return value


def normalized_time_milliseconds(value: Union[int, datetime.datetime]) -> int:
    if isinstance(value, datetime.datetime):
        ms = int(value.microsecond / 1000)
        value = int(time.mktime(value.timetuple())) * 1000 + ms

    return value
