"""Stabilité : ré-agréger les mêmes dumps ne doit jamais changer la réponse de query().

``aggregate`` est rejoué à chaque ``fetch`` (et par ``schedule run``). À dumps
constants, l'agrégat et toute réponse ``query()`` / ``/v1/query`` doivent être
**strictement** identiques d'un appel à l'autre : mêmes valeurs, mêmes dtypes,
même ordre de lignes.

Le jeu de données vise les cas à risque :

- roll futures ESH5 → ESM5 (ESM5 actif le 2025-03-10) : les deux contrats ont une
  barre à chaque ``window_start`` de ce jour (recouvrement du fetch) ;
- refetch d'une même plage par un run ultérieur (dédup ``keep="last"``) ;
- volume suffisant (plusieurs milliers de barres) pour que Polars parallélise
  les tris et ``unique`` — une instabilité de tri n'apparaît qu'à ce prix.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from io import BytesIO

import polars as pl
import pytest
from fastapi.testclient import TestClient
from polars.testing import assert_frame_equal

from myquantstore.contracts.rollover import RolloverChain
from myquantstore.instruments import RESOLUTION_1DAY, Instrument, InstrumentType
from myquantstore.pipeline.aggregator import aggregate
from myquantstore.query.reader import query
from myquantstore.serve.server import create_serve_app
from myquantstore.storage.aggregate_cache import read_aggregate
from myquantstore.storage.parquet_io import write_parquet
from myquantstore.storage.raw_dumps import save_raw_dump

# Nombre de ré-agrégations par test : l'instabilité observée est quasi
# systématique à ce volume, 5 tours la rendent certaine.
_REPEATS = 5

# Premier jour d'ESM5 (active_from) ; jour de roll d'ESH5 = vendredi 07/03.
_NEW_CONTRACT_DAY = date(2025, 3, 10)


def _minute_bars(ticker: str, first: date, last: date, base: float) -> pl.DataFrame:
    """Barres 1min en semaine (pause 21h UTC), prix multiples de 0.25."""
    timestamps: list[datetime] = []
    current = datetime.combine(first, time(0, 0), UTC)
    stop = datetime.combine(last + timedelta(days=1), time(0, 0), UTC)
    while current < stop:
        if current.weekday() < 5 and current.hour != 21:
            timestamps.append(current)
        current += timedelta(minutes=1)
    n = len(timestamps)
    prices = [base + (i % 40) * 0.25 for i in range(n)]
    return pl.DataFrame(
        {
            "window_start": timestamps,
            "ticker": [ticker] * n,
            "open": prices,
            "high": [p + 1 for p in prices],
            "low": [p - 1 for p in prices],
            "close": [p + 0.5 for p in prices],
            "settlement_price": [p + 0.5 for p in prices],
            "volume": [100 + i % 7 for i in range(n)],
            "dollar_volume": [1000.0] * n,
            "transactions": [10] * n,
            "session_end_date": [ts.date() for ts in timestamps],
        }
    )


def _seed_es_roll(settings, instrument: Instrument) -> None:
    """ESH5 fetché jusqu'à active_until inclus (recouvrement), ESM5 dès ce jour, + un refetch ESM5."""
    save_raw_dump(
        _minute_bars("ESH5", date(2025, 3, 5), _NEW_CONTRACT_DAY, 5800.0),
        instrument,
        "ESH5",
        "20250310T220000",
        settings,
    )
    save_raw_dump(
        _minute_bars("ESM5", _NEW_CONTRACT_DAY, date(2025, 3, 12), 5850.0),
        instrument,
        "ESM5",
        "20250312T220000",
        settings,
    )
    # Refetch des deux derniers jours avec des prix révisés : keep="last" doit
    # toujours retenir ce run.
    save_raw_dump(
        _minute_bars("ESM5", date(2025, 3, 11), date(2025, 3, 12), 5851.0),
        instrument,
        "ESM5",
        "20250313T080000",
        settings,
    )


def _write_contracts_cache(settings, contracts_df: pl.DataFrame) -> None:
    write_parquet(
        contracts_df,
        settings.contracts_cache_path("ES"),
        product_code="ES",
        last_fetched_at=datetime.now(UTC).isoformat(),
    )


def _assert_identical(reference: pl.DataFrame, current: pl.DataFrame, context: str) -> None:
    """Égalité stricte : valeurs exactes, dtypes, ordre des lignes et des colonnes."""
    try:
        assert_frame_equal(reference, current, check_exact=True, categorical_as_str=True)
    except AssertionError as exc:
        raise AssertionError(f"Réponse instable ({context}) : {exc}") from exc


@pytest.fixture
def es_roll(tmp_settings, es_instrument):
    _seed_es_roll(tmp_settings, es_instrument)
    return tmp_settings, es_instrument


class TestAggregateStability:
    def test_repeated_aggregate_writes_identical_parquet(self, es_roll):
        settings, es = es_roll
        aggregate(es, settings)
        reference = read_aggregate(es, settings)
        for i in range(_REPEATS):
            aggregate(es, settings)
            _assert_identical(reference, read_aggregate(es, settings), f"agrégat, tour {i}")

    def test_aggregate_rows_ordered_by_window_start_then_ticker(self, es_roll):
        settings, es = es_roll
        aggregate(es, settings)
        df = read_aggregate(es, settings).with_columns(pl.col("ticker").cast(pl.Utf8))
        assert df.select(["window_start", "ticker"]).equals(
            df.select(["window_start", "ticker"]).sort(["window_start", "ticker"])
        )


_QUERY_CASES_NO_CHAIN = {
    "1min": {},
    "1min_no_dedup": {"dedup_timestamps": False},
    "5min": {"k_minutes": 5},
    "1hour": {"k_minutes": 60},
    "5min_forward_fill": {"k_minutes": 5, "forward_fill": True},
    "intraday_15min": {
        "k_minutes": 15,
        "intraday_begin": time(8, 30),
        "intraday_end": time(15, 0),
    },
}

_QUERY_CASES_CHAIN = {
    "chain_1min": {},
    "chain_1min_no_dedup": {"dedup_timestamps": False},
    "chain_5min": {"k_minutes": 5},
    "chain_adjust": {"adjust_rollover": True},
    "chain_normalize_tick_size": {"normalize_tick_size": True},
}


class TestQueryStability:
    @pytest.mark.parametrize(
        "kwargs", _QUERY_CASES_NO_CHAIN.values(), ids=_QUERY_CASES_NO_CHAIN.keys()
    )
    def test_query_without_chain_is_stable(self, es_roll, kwargs):
        settings, es = es_roll
        aggregate(es, settings)
        reference = query(es, settings, **kwargs)
        for i in range(_REPEATS):
            aggregate(es, settings)
            _assert_identical(reference, query(es, settings, **kwargs), f"tour {i}")

    @pytest.mark.parametrize("kwargs", _QUERY_CASES_CHAIN.values(), ids=_QUERY_CASES_CHAIN.keys())
    def test_query_with_chain_is_stable(self, es_roll, sample_chain, kwargs):
        settings, es = es_roll
        aggregate(es, settings)
        reference = query(es, settings, chain=sample_chain, **kwargs)
        for i in range(_REPEATS):
            aggregate(es, settings)
            current = query(es, settings, chain=sample_chain, **kwargs)
            _assert_identical(reference, current, f"tour {i}")

    def test_new_contract_day_keeps_new_contract_with_or_without_chain(self, es_roll, sample_chain):
        """Premier jour du nouveau contrat : sans chaîne comme avec, il gagne."""
        settings, es = es_roll
        aggregate(es, settings)
        for chain in (None, sample_chain):
            df = query(es, settings, chain=chain).with_columns(pl.col("ticker").cast(pl.Utf8))
            new_day = df.filter(pl.col("window_start").dt.date() == _NEW_CONTRACT_DAY)
            assert new_day.height > 0
            assert new_day["ticker"].unique().to_list() == ["ESM5"]

    def test_tickers_outside_chain_are_stable(self, tmp_settings, es_instrument):
        """Contrats absents de la chaîne (rang inconnu) : départage déterministe."""
        contracts = pl.DataFrame(
            {
                "ticker": ["ESZ4"],
                "first_trade_date": [date(2024, 9, 16)],
                "last_trade_date": [date(2024, 12, 20)],
                "settlement_date": [date(2024, 12, 20)],
                "trade_tick_size": [0.25],
                "name": ["E-mini S&P 500 Dec 2024"],
                "type": ["single"],
                "product_code": ["ES"],
                "active": [False],
            }
        )
        chain = RolloverChain("ES", contracts, days_before_expiry=7)
        _seed_es_roll(tmp_settings, es_instrument)
        aggregate(es_instrument, tmp_settings)
        reference = query(es_instrument, tmp_settings, chain=chain)
        for i in range(_REPEATS):
            aggregate(es_instrument, tmp_settings)
            current = query(es_instrument, tmp_settings, chain=chain)
            _assert_identical(reference, current, f"tour {i}")

    def test_extraday_query_is_stable(self, tmp_settings):
        """Track 1day (Yahoo, un seul ticker) : resample k jours / semaine ISO."""
        aapl = Instrument(type=InstrumentType.STOCKS, symbol="AAPL")
        days = [datetime(2024, 1, 1, tzinfo=UTC) + timedelta(days=i) for i in range(400)]
        days = [d for d in days if d.weekday() < 5]
        n = len(days)
        prices = [180.0 + (i % 50) * 0.5 for i in range(n)]
        daily = pl.DataFrame(
            {
                "window_start": days,
                "ticker": ["AAPL"] * n,
                "open": prices,
                "high": [p + 1 for p in prices],
                "low": [p - 1 for p in prices],
                "close": [p + 0.5 for p in prices],
                "volume": [1_000_000 + i for i in range(n)],
            }
        )
        save_raw_dump(
            daily, aapl, "AAPL", "20250101T000000", tmp_settings, resolution=RESOLUTION_1DAY
        )
        save_raw_dump(
            daily.tail(30).with_columns(pl.col("close") + 0.25),
            aapl,
            "AAPL",
            "20250102T000000",
            tmp_settings,
            resolution=RESOLUTION_1DAY,
        )
        cases = [{}, {"k_days": 5}, {"k_days": 7, "week_aligned": True}]
        aggregate(aapl, tmp_settings, resolution=RESOLUTION_1DAY)
        references = [
            query(aapl, tmp_settings, resolution=RESOLUTION_1DAY, no_split=True, **kw)
            for kw in cases
        ]
        for i in range(_REPEATS):
            aggregate(aapl, tmp_settings, resolution=RESOLUTION_1DAY)
            for kw, reference in zip(cases, references, strict=True):
                current = query(aapl, tmp_settings, resolution=RESOLUTION_1DAY, no_split=True, **kw)
                _assert_identical(reference, current, f"1day {kw}, tour {i}")


class TestServeStability:
    @pytest.mark.parametrize("with_contracts_cache", [False, True], ids=["no_chain", "chain"])
    @pytest.mark.parametrize(
        "params",
        [
            {"instrument": "ES"},
            {"instrument": "ES", "timescale_unit": "min", "timescale_nb": 5},
            {"instrument": "ES", "dedup_timestamps": False},
        ],
        ids=["1min", "5min", "no_dedup"],
    )
    def test_v1_query_is_stable(self, es_roll, sample_contracts_df, with_contracts_cache, params):
        settings, es = es_roll
        if with_contracts_cache:
            _write_contracts_cache(settings, sample_contracts_df)
        client = TestClient(create_serve_app(settings))

        def fetch() -> pl.DataFrame:
            resp = client.get("/v1/query", params=params)
            assert resp.status_code == 200, resp.text
            return pl.read_parquet(BytesIO(resp.content))

        aggregate(es, settings)
        reference = fetch()
        for i in range(_REPEATS):
            aggregate(es, settings)
            _assert_identical(reference, fetch(), f"/v1/query {params}, tour {i}")
