"""Tests resample extraday."""

from datetime import date, datetime, timedelta

import polars as pl

from myquantstore.query.resampler import resample_extraday


def _daily(n: int = 10) -> pl.DataFrame:
    rows = []
    for i in range(n):
        d = date(2024, 1, 1 + i)
        rows.append(
            {
                "window_start": datetime(2024, 1, 1 + i),
                "session_end_date": d,
                "ticker": "AAPL",
                "open": 100.0 + i,
                "high": 101.0 + i,
                "low": 99.0 + i,
                "close": 100.5 + i,
                "volume": 1000 + i,
            }
        )
    return pl.DataFrame(rows).with_columns(pl.col("window_start").cast(pl.Datetime("ns")))


def _weekdays(n_weeks: int = 4, extra_days: int = 0) -> pl.DataFrame:
    """n_weeks semaines Mon–Fri + optionnellement des jours de la semaine suivante."""
    rows = []
    # 2024-01-01 = lundi
    start = date(2024, 1, 1)
    day = start
    trading_days_needed = n_weeks * 5 + extra_days
    added = 0
    while added < trading_days_needed:
        if day.weekday() < 5:  # Mon=0 .. Fri=4
            rows.append(
                {
                    "window_start": datetime(day.year, day.month, day.day),
                    "session_end_date": day,
                    "ticker": "WCN",
                    "open": 100.0 + added,
                    "high": 101.0 + added,
                    "low": 99.0 + added,
                    "close": 100.5 + added,
                    "volume": 1000 + added,
                }
            )
            added += 1
        day += timedelta(days=1)
    return pl.DataFrame(rows).with_columns(pl.col("window_start").cast(pl.Datetime("ns")))


def test_k_days_1_noop():
    df = _daily(5)
    out = resample_extraday(df, 1)
    assert out.height == 5
    assert "candle_count" in out.columns


def test_k_days_2():
    df = _daily(10)
    out = resample_extraday(df, 2)
    # 10 days → 5 full buckets of 2
    assert out.height == 5
    assert out["candle_count"].to_list() == [2] * 5
    assert "window_start" in out.columns


def test_partial_dropped():
    df = _daily(5)
    out = resample_extraday(df, 2)
    # 2 full + 1 partial dropped → 2
    assert out.height == 2


def test_week_keeps_5_session_buckets():
    """Bugfix : candle_count>=7 droppait toutes les semaines (~5 séances)."""
    # 4 semaines complètes Mon–Fri, dernière date = vendredi semaine 4
    # cal_end semaine = dimanche → last Fri < Sun → drop dernière si incomplete
    # Avec 4 semaines full ending Friday: last_date=Fri of week4, cal_end=Sun week4 → drop 1
    df = _weekdays(n_weeks=4, extra_days=0)
    out = resample_extraday(df, 7, week_aligned=True)
    # 3 semaines historiques complètes (la 4e droppée car dimanche non atteint)
    assert out.height == 3
    assert all(c == 5 for c in out["candle_count"].to_list())


def test_week_complete_through_sunday_anchor():
    """Si la data atteint le dimanche calendaire (via last weekday of complete period logic).

    Avec last_date = dimanche théorique : on simule en allant jusqu'au vendredi
    d'une 5e semaine partielle → semaines 1-4 historiques gardées.
    """
    df = _weekdays(n_weeks=4, extra_days=2)  # +lun+mar semaine 5
    out = resample_extraday(df, 7, week_aligned=True)
    assert out.height == 4
    assert out["candle_count"].to_list() == [5, 5, 5, 5]


def test_old_bug_candle_count_ge_k_would_drop_all():
    """Régression : l'ancien filtre candle_count >= 7 vidait le résultat."""
    df = _weekdays(n_weeks=8, extra_days=0)
    out = resample_extraday(df, 7, week_aligned=True)
    assert out.height >= 6
    assert out.height > 0
