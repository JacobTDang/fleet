from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from fleet import cron

NY = "America/New_York"


def epoch(y, m, d, hh, mm, tz=NY):
    return datetime(y, m, d, hh, mm, tzinfo=ZoneInfo(tz)).timestamp()


def test_next_fire_basic_daily():
    assert cron.next_fire("0 9 * * *", NY, epoch(2026, 8, 20, 8, 0)) == epoch(2026, 8, 20, 9, 0)


def test_next_fire_is_strictly_after():
    t = cron.next_fire("0 9 * * *", NY, epoch(2026, 8, 20, 9, 0))
    assert t == epoch(2026, 8, 21, 9, 0)


def test_weekday_schedule_skips_weekend():
    # 2026-08-21 is a Friday; past 9am Friday -> Monday 9am
    t = cron.next_fire("0 9 * * 1-5", NY, epoch(2026, 8, 21, 10, 0))
    assert t == epoch(2026, 8, 24, 9, 0)


def test_timezone_matters():
    after = epoch(2026, 8, 20, 8, 0, tz="UTC")
    assert cron.next_fire("0 9 * * *", "UTC", after) == epoch(2026, 8, 20, 9, 0, tz="UTC")
    assert cron.next_fire("0 9 * * *", NY, after) == epoch(2026, 8, 20, 9, 0, tz=NY)


def test_spring_forward_nonexistent_time_fires_once_after_gap():
    # US spring-forward 2026-03-08: 02:00 -> 03:00, so 02:30 does not exist.
    t1 = cron.next_fire("30 2 * * *", NY, epoch(2026, 3, 8, 1, 0))
    d1 = datetime.fromtimestamp(t1, ZoneInfo(NY))
    assert (d1.month, d1.day) == (3, 8) and d1.hour == 3  # first valid instant after the gap
    t2 = cron.next_fire("30 2 * * *", NY, t1)
    d2 = datetime.fromtimestamp(t2, ZoneInfo(NY))
    assert (d2.month, d2.day) == (3, 9)  # exactly one fire on transition day


def test_fall_back_ambiguous_time_fires_once():
    # US fall-back 2026-11-01: 01:30 occurs twice; must fire exactly once.
    t1 = cron.next_fire("30 1 * * *", NY, epoch(2026, 11, 1, 0, 0))
    d1 = datetime.fromtimestamp(t1, ZoneInfo(NY))
    assert (d1.month, d1.day) == (11, 1)
    t2 = cron.next_fire("30 1 * * *", NY, t1)
    d2 = datetime.fromtimestamp(t2, ZoneInfo(NY))
    assert (d2.month, d2.day) == (11, 2)


def test_validate_rejects_garbage_and_accepts_good():
    with pytest.raises(ValueError):
        cron.validate("not a cron")
    with pytest.raises(ValueError):
        cron.validate("61 * * * *")
    cron.validate("*/5 * * * *")
    cron.validate("0 9 * * 1-5")


def test_validate_tz():
    with pytest.raises(ValueError):
        cron.validate_tz("Mars/Olympus")
    cron.validate_tz("America/New_York")
