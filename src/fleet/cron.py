"""Cron-expression scheduling: next fire times computed in the job's timezone,
returned as UTC epochs. cronsim does the parsing and the DST arithmetic
(nonexistent times fire once after the gap; ambiguous times fire once)."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from cronsim import CronSim, CronSimError


def validate(expr):
    try:
        CronSim(expr, datetime(2026, 1, 1, tzinfo=timezone.utc))
    except CronSimError as e:
        raise ValueError(f"invalid cron expression {expr!r}: {e}") from e


def validate_tz(tz_name):
    try:
        ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError, KeyError) as e:
        raise ValueError(f"unknown timezone {tz_name!r}") from e


def next_fire(expr, tz_name, after_epoch):
    """UTC epoch of the first fire strictly after after_epoch."""
    after = datetime.fromtimestamp(after_epoch, ZoneInfo(tz_name))
    return next(CronSim(expr, after)).timestamp()
