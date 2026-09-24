from unittest import mock

import pytest

from osm_quality_pipeline.defs.utils import rate_limiter as rl


@pytest.fixture
def clock(monkeypatch):
    """Fake time: sleep() advances the clock instantly and records the duration."""
    state = {"now": 1_000_000.0, "sleeps": []}

    def sleep(seconds):
        state["sleeps"].append(seconds)
        state["now"] += seconds

    monkeypatch.setattr(rl.time, "time", lambda: state["now"])
    monkeypatch.setattr(rl.time, "sleep", sleep)
    return state


@pytest.fixture
def log(monkeypatch):
    logger = mock.Mock()
    monkeypatch.setattr(rl, "logger", logger)
    return logger


def make_limiter(tmp_path, **limits):
    return rl.ApiRateLimiter(db_path=str(tmp_path / "rl.sqlite"), api_name="test", **limits)


def test_per_minute_limit_logs_and_sleeps_seconds(tmp_path, clock, log):
    limiter = make_limiter(tmp_path, max_per_minute=2)
    limiter.acquire()
    limiter.acquire()
    limiter.acquire()

    assert clock["sleeps"] == [pytest.approx(60, abs=1)]
    message = log.info.call_args.args[0]
    assert "per-minute limit (2/minute) reached" in message


def test_daily_limit_sleeps_until_slot_frees_in_one_go(tmp_path, clock, log):
    limiter = make_limiter(tmp_path, max_per_minute=100, max_per_day=2)
    limiter.acquire()
    clock["now"] += 3600  # second request one hour later
    limiter.acquire()
    limiter.acquire()

    # the first request leaves the 24h window after 23h: one sleep, one warning
    assert clock["sleeps"] == [pytest.approx(23 * 3600, abs=1)]
    assert log.warning.call_count == 1
    message = log.warning.call_args.args[0]
    assert "daily limit (2/day) reached, sleeping 23h 0m until" in message


def test_no_limit_no_sleep(tmp_path, clock, log):
    limiter = make_limiter(tmp_path)
    for _ in range(5):
        limiter.acquire()
    assert clock["sleeps"] == []


@pytest.mark.parametrize("seconds, expected", [(12, "12s"), (125, "2m"), (3 * 3600 + 12 * 60, "3h 12m")])
def test_format_duration(seconds, expected):
    assert rl._format_duration(seconds) == expected
