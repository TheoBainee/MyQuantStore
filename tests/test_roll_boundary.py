"""Frontière de roll futures : jour de roll = ancien contrat, séance suivante = nouveau.

Exemple de référence : ESU6 expire le vendredi 18/09/2026, ``days_before_expiry=7``
→ ``rollover_date`` = vendredi 11/09 (dernier jour d'ESU6), ESZ6 actif dès le
lundi 14/09. La séance CME du 14/09 ouvre le **dimanche 13/09 à 17:00 CT**
(22:00 UTC en heure d'été) : ces barres appartiennent déjà au nouveau contrat.

Les dumps simulent les bornes réelles du fetch (``_determine_segment_range``),
dates API interprétées comme jours UTC inclusifs.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from myquantstore.contracts.rollover import RolloverChain
from myquantstore.pipeline.aggregator import aggregate
from myquantstore.pipeline.fetchers.futures import _determine_segment_range
from myquantstore.query.reader import query
from myquantstore.storage.raw_dumps import save_raw_dump

_ROLL_DAY = date(2026, 9, 11)  # dernier jour de l'ancien contrat
_NEXT_SESSION = date(2026, 9, 14)  # premier jour du nouveau contrat
_TODAY = date(2026, 9, 16)


def _contracts() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ticker": ["ESU6", "ESZ6"],
            "first_trade_date": [date(2025, 6, 20), date(2025, 9, 19)],
            "last_trade_date": [date(2026, 9, 18), date(2026, 12, 18)],
            "settlement_date": [date(2026, 9, 18), date(2026, 12, 18)],
            "trade_tick_size": [0.25, 0.25],
            "name": ["E-mini S&P 500 Sep 2026", "E-mini S&P 500 Dec 2026"],
            "type": ["single", "single"],
            "product_code": ["ES", "ES"],
            "active": [True, True],
        }
    )


def _cme_session_bars(ticker: str, gte: date, lte: date, price: float) -> pl.DataFrame:
    """Barres 1min CME (heure d'été) sur les jours UTC [gte, lte].

    Séance D : D-1 22:00 UTC → D 21:00 UTC, ``session_end_date`` = D ; pause
    quotidienne 21:00-22:00 UTC ; fermé du vendredi 21:00 au dimanche 22:00 UTC.
    """
    rows: list[tuple[datetime, date]] = []
    current = datetime.combine(gte, datetime.min.time(), UTC)
    stop = datetime.combine(lte + timedelta(days=1), datetime.min.time(), UTC)
    while current < stop:
        session = (current + timedelta(hours=2)).date()
        weekend = (
            (current.weekday() == 4 and current.hour >= 21)
            or current.weekday() == 5
            or (current.weekday() == 6 and current.hour < 22)
        )
        if current.hour != 21 and not weekend and session.weekday() < 5:
            rows.append((current, session))
        current += timedelta(minutes=1)
    n = len(rows)
    prices = [price] * n
    return pl.DataFrame(
        {
            "window_start": [r[0] for r in rows],
            "ticker": [ticker] * n,
            "open": prices,
            "high": prices,
            "low": prices,
            "close": prices,
            "settlement_price": prices,
            "volume": [1] * n,
            "dollar_volume": [1.0] * n,
            "transactions": [1] * n,
            "session_end_date": [r[1] for r in rows],
        }
    )


@pytest.fixture
def chain() -> RolloverChain:
    return RolloverChain("ES", _contracts(), days_before_expiry=7)


@pytest.fixture
def roll_aggregate(tmp_settings, es_instrument, chain):
    """Dumps ESU6 / ESZ6 aux bornes exactes du fetch, puis agrégat."""
    for seg, price in zip(chain.segments, [6000.0, 6050.0], strict=True):
        gte, lte = _determine_segment_range(seg, date(2026, 9, 9), _TODAY, None, None, tmp_settings)
        assert gte is not None and lte is not None
        bars = _cme_session_bars(
            seg.ticker, date.fromisoformat(gte), date.fromisoformat(lte), price
        )
        save_raw_dump(bars, es_instrument, seg.ticker, "20260916T000000", tmp_settings)
    aggregate(es_instrument, tmp_settings)
    return tmp_settings, es_instrument


def _tickers_by_session(df: pl.DataFrame) -> dict[date, list[str]]:
    grouped = (
        df.with_columns(pl.col("ticker").cast(pl.Utf8))
        .group_by("session_end_date")
        .agg(pl.col("ticker").unique().sort())
    )
    return dict(zip(grouped["session_end_date"], grouped["ticker"].to_list(), strict=True))


class TestRollChain:
    def test_rollover_dates(self, chain):
        old, new = chain.segments
        assert old.rollover_date == _ROLL_DAY
        assert old.active_until == _NEXT_SESSION
        assert new.active_from == _NEXT_SESSION

    def test_new_contract_fetch_includes_sunday_evening_open(self, chain, tmp_settings):
        _, new = chain.segments
        gte, _ = _determine_segment_range(new, date(2026, 9, 9), _TODAY, None, None, tmp_settings)
        assert gte == "2026-09-13"


class TestRollQuery:
    def test_roll_day_is_old_contract_next_session_is_new(self, roll_aggregate, chain):
        settings, es = roll_aggregate
        by_session = _tickers_by_session(query(es, settings, chain=chain))
        assert by_session[_ROLL_DAY] == ["ESU6"]
        assert by_session[_NEXT_SESSION] == ["ESZ6"]
        assert all(t == ["ESU6"] for d, t in by_session.items() if d <= _ROLL_DAY)
        assert all(t == ["ESZ6"] for d, t in by_session.items() if d >= _NEXT_SESSION)

    def test_sunday_evening_open_is_new_contract(self, roll_aggregate, chain):
        settings, es = roll_aggregate
        df = query(es, settings, chain=chain).with_columns(pl.col("ticker").cast(pl.Utf8))
        sunday = df.filter(pl.col("window_start").dt.date() == date(2026, 9, 13))
        assert sunday.height == 120  # 17:00-19:00 CT = 22:00-24:00 UTC
        assert sunday["ticker"].unique().to_list() == ["ESZ6"]

    def test_old_contract_bar_alone_on_next_session_is_dropped(
        self, tmp_settings, es_instrument, chain
    ):
        """Barre ESU6 sans barre ESZ6 au même timestamp : écartée quand même."""
        lone = _cme_session_bars("ESU6", date(2026, 9, 13), date(2026, 9, 13), 6000.0)
        save_raw_dump(lone, es_instrument, "ESU6", "20260914T000000", tmp_settings)
        aggregate(es_instrument, tmp_settings)
        assert query(es_instrument, tmp_settings, chain=chain).is_empty()

    def test_no_dedup_keeps_raw_overlap(self, roll_aggregate, chain):
        """``dedup_timestamps=False`` : l'agrégat brut, recouvrement du fetch compris."""
        settings, es = roll_aggregate
        by_session = _tickers_by_session(query(es, settings, chain=chain, dedup_timestamps=False))
        assert by_session[_NEXT_SESSION] == ["ESU6", "ESZ6"]
