"""Tests couverture OHLCV / fraîcheur (storage.coverage)."""

from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl

from myquantstore.instruments import RESOLUTION_1DAY, RESOLUTION_1MIN, Instrument, InstrumentType
from myquantstore.storage.aggregate_cache import write_aggregate
from myquantstore.storage.coverage import (
    HealthLevel,
    applicable_resolutions,
    assess_instrument_health,
    attach_coverage_fields,
    get_aggregate_date_range,
    get_resolution_coverage,
)


def _agg_df(days: list[date]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "window_start": [datetime(d.year, d.month, d.day, tzinfo=UTC) for d in days],
            "open": [1.0] * len(days),
            "high": [1.0] * len(days),
            "low": [1.0] * len(days),
            "close": [1.0] * len(days),
            "volume": [100] * len(days),
            "ticker": ["AAPL"] * len(days),
        }
    ).with_columns(pl.col("window_start").cast(pl.Datetime("ns")))


def test_coverage_absent(tmp_settings, aapl_instrument):
    cov = get_resolution_coverage(aapl_instrument, tmp_settings, RESOLUTION_1MIN)
    assert cov.present is False
    assert cov.lag_days is None


def test_coverage_fresh(tmp_settings, aapl_instrument):
    today = date(2026, 8, 1)
    write_aggregate(
        _agg_df([date(2026, 7, 30), date(2026, 7, 31)]),
        aapl_instrument,
        tmp_settings,
        resolution=RESOLUTION_1MIN,
    )
    cov = get_resolution_coverage(
        aapl_instrument, tmp_settings, RESOLUTION_1MIN, today=today
    )
    assert cov.present
    assert cov.max_date == date(2026, 7, 31)
    assert cov.lag_days == 1
    assert cov.is_stale(tmp_settings) is False


def test_coverage_stale(tmp_settings, aapl_instrument):
    today = date(2026, 8, 1)
    write_aggregate(
        _agg_df([date(2026, 7, 10)]),
        aapl_instrument,
        tmp_settings,
        resolution=RESOLUTION_1MIN,
    )
    cov = get_resolution_coverage(
        aapl_instrument, tmp_settings, RESOLUTION_1MIN, today=today
    )
    assert cov.lag_days == 22
    assert cov.is_stale(tmp_settings) is True

    health = assess_instrument_health(aapl_instrument, tmp_settings, today=today)
    assert health.worst_level == HealthLevel.STALE
    assert any(i.code == "stale" for i in health.issues)
    assert health.has_check_failures(strict_missing=False) is True


def test_check_failures_missing_vs_strict(tmp_settings, aapl_instrument):
    """Instrument neuf : missing = pas de fail check sauf --strict-missing."""
    health = assess_instrument_health(aapl_instrument, tmp_settings, today=date(2026, 8, 1))
    assert health.has_problems is True  # WARN missing
    assert health.has_check_failures(strict_missing=False) is False
    assert health.has_check_failures(strict_missing=True) is True


def test_cross_resolution_lag(tmp_settings, aapl_instrument):
    today = date(2026, 8, 1)
    write_aggregate(
        _agg_df([date(2026, 7, 10)]),
        aapl_instrument,
        tmp_settings,
        resolution=RESOLUTION_1MIN,
        source="massive",
    )
    write_aggregate(
        _agg_df([date(2026, 7, 31)]),
        aapl_instrument,
        tmp_settings,
        resolution=RESOLUTION_1DAY,
        source="yahoo",
    )
    health = assess_instrument_health(aapl_instrument, tmp_settings, today=today)
    assert any(i.code == "cross_resolution_lag" for i in health.issues)
    assert health.has_problems


def test_get_aggregate_date_range(tmp_settings, aapl_instrument):
    write_aggregate(
        _agg_df([date(2026, 1, 1), date(2026, 6, 15)]),
        aapl_instrument,
        tmp_settings,
        resolution=RESOLUTION_1MIN,
    )
    oldest, latest = get_aggregate_date_range(
        aapl_instrument, tmp_settings, RESOLUTION_1MIN
    )
    assert oldest == date(2026, 1, 1)
    assert latest == date(2026, 6, 15)


def test_attach_coverage_fields(tmp_settings, aapl_instrument):
    today = date(2026, 8, 1)
    write_aggregate(
        _agg_df([date(2026, 7, 10)]),
        aapl_instrument,
        tmp_settings,
        resolution=RESOLUTION_1MIN,
    )
    result: dict[str, object] = {"status": "skipped"}
    attach_coverage_fields(
        result, aapl_instrument, tmp_settings, RESOLUTION_1MIN, today=today
    )
    assert result["latest"] == "2026-07-10"
    assert result["lag_days"] == 22
    assert result["stale"] is True


def test_applicable_resolutions_futures(es_instrument):
    assert applicable_resolutions(es_instrument) == [RESOLUTION_1MIN, RESOLUTION_1DAY]


def test_applicable_resolutions_options():
    opt = Instrument(type=InstrumentType.OPTIONS, symbol="AAPL")
    assert applicable_resolutions(opt) == [RESOLUTION_1MIN]


def test_missing_aggregate_warn(tmp_settings, aapl_instrument):
    health = assess_instrument_health(
        aapl_instrument, tmp_settings, today=date(2026, 8, 1)
    )
    assert health.worst_level == HealthLevel.WARN
    assert any(i.code == "missing_aggregate" for i in health.issues)
