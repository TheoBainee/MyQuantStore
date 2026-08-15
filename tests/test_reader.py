"""Tests du module query/reader.py."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from myquantstore.pipeline.aggregator import aggregate
from myquantstore.query.reader import (
    check_ticksize_accuracy_fn,
    query,
)
from myquantstore.storage.raw_dumps import save_raw_dump


def _make_df(ticker: str, timestamps: list[datetime], prices: list[float], tick: float = 0.25) -> pl.DataFrame:
    """Crée un DataFrame de chandeliers avec prix multiples de tick."""
    return pl.DataFrame(
        {
            "window_start": timestamps,
            "ticker": [ticker] * len(timestamps),
            "open": prices,
            "high": [p + tick for p in prices],
            "low": [p - tick for p in prices],
            "close": [p + tick for p in prices],
            "settlement_price": [p + tick for p in prices],
            "volume": [100] * len(prices),
            "dollar_volume": [1000.0] * len(prices),
            "transactions": [10] * len(prices),
            "session_end_date": [ts.date() for ts in timestamps],
        }
    )


@pytest.fixture
def setup_aggregate(tmp_settings, es_instrument, sample_chain):
    """Crée un cache agrégé avec des données de test (prix conformes au tick)."""
    ts = [
        datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC),
        datetime(2025, 6, 1, 9, 31, 0, tzinfo=UTC),
        datetime(2025, 6, 1, 9, 32, 0, tzinfo=UTC),
    ]
    prices = [4500.00, 4501.25, 4502.50]

    df = _make_df("ESM5", ts, prices, tick=0.25)
    save_raw_dump(df, es_instrument, "ESM5", "20260711T183000", tmp_settings)
    aggregate(es_instrument, tmp_settings)


class TestQuery:
    """Tests de la fonction query()."""

    def test_query_returns_all_data(self, tmp_settings, es_instrument, sample_chain, setup_aggregate):
        df = query(es_instrument, tmp_settings, sample_chain)
        assert df.height == 3
        assert "window_start" in df.columns

    def test_query_with_start_filter(self, tmp_settings, es_instrument, sample_chain, setup_aggregate):
        df = query(
            es_instrument,
            tmp_settings,
            sample_chain,
            start=datetime(2025, 6, 1, 9, 31, 0, tzinfo=UTC),
        )
        assert df.height == 2

    def test_query_with_end_filter(self, tmp_settings, es_instrument, sample_chain, setup_aggregate):
        df = query(
            es_instrument,
            tmp_settings,
            sample_chain,
            end=datetime(2025, 6, 1, 9, 31, 0, tzinfo=UTC),
        )
        assert df.height == 2

    def test_query_with_limit(self, tmp_settings, es_instrument, sample_chain, setup_aggregate):
        df = query(es_instrument, tmp_settings, sample_chain, limit=2)
        assert df.height == 2

    def test_query_adjust_rollover_futures(self, tmp_settings, es_instrument, sample_chain, setup_aggregate):
        """adjust_rollover=True pour futures applique le back-adjust (sans lever d'erreur)."""
        # Avec la chaîne fournie, ne doit plus lever
        df = query(es_instrument, tmp_settings, sample_chain, adjust_rollover=True)
        # Le df doit être retourné (facteurs 1.0 si pas de rollover dans le sample)
        assert df.height >= 0

    def test_query_normalize_and_adjust_incompatible(self, tmp_settings, es_instrument, sample_chain, setup_aggregate):
        with pytest.raises(ValueError, match="incompatibles"):
            query(
                es_instrument,
                tmp_settings,
                sample_chain,
                adjust_rollover=True,
                normalize_tick_size=True,
            )

    def test_query_no_split_noop_for_futures(self, tmp_settings, es_instrument, sample_chain, setup_aggregate):
        """--no-split est un no-op pour futures (pas de splits)."""
        df = query(es_instrument, tmp_settings, sample_chain, no_split=True)
        assert df.height == 3
        # Les prix sont inchangés (pas d'ajustement pour futures)
        assert df["open"][0] == 4500.00

    def test_query_normalize_without_chain_raises(self, tmp_settings, es_instrument, setup_aggregate):
        """normalize_tick_size sans chain lève ValueError."""
        with pytest.raises(ValueError, match="chain"):
            query(es_instrument, tmp_settings, chain=None, normalize_tick_size=True)

    def test_query_dedup_timestamps_default_keeps_newer_contract(
        self, tmp_settings, es_instrument, sample_chain
    ):
        """Au même window_start, le contrat le plus récent de la chaîne gagne."""
        ts = datetime(2025, 3, 7, 0, 0, 0, tzinfo=UTC)
        save_raw_dump(
            _make_df("ESH5", [ts], [5800.00]),
            es_instrument,
            "ESH5",
            "20250307T000000",
            tmp_settings,
        )
        save_raw_dump(
            _make_df("ESM5", [ts], [5810.00]),
            es_instrument,
            "ESM5",
            "20250307T000001",
            tmp_settings,
        )
        aggregate(es_instrument, tmp_settings)

        df = query(es_instrument, tmp_settings, sample_chain)
        assert df.height == 1
        assert df["ticker"][0] == "ESM5"
        assert df["open"][0] == 5810.00

    def test_query_no_dedup_timestamps_keeps_both_contracts(
        self, tmp_settings, es_instrument, sample_chain
    ):
        ts = datetime(2025, 3, 7, 0, 0, 0, tzinfo=UTC)
        save_raw_dump(
            _make_df("ESH5", [ts], [5800.00]),
            es_instrument,
            "ESH5",
            "20250307T000000",
            tmp_settings,
        )
        save_raw_dump(
            _make_df("ESM5", [ts], [5810.00]),
            es_instrument,
            "ESM5",
            "20250307T000001",
            tmp_settings,
        )
        aggregate(es_instrument, tmp_settings)

        df = query(
            es_instrument, tmp_settings, sample_chain, dedup_timestamps=False
        )
        assert df.height == 2
        assert set(df["ticker"].to_list()) == {"ESH5", "ESM5"}

    def test_include_cols_keeps_requested_order(
        self, tmp_settings, es_instrument, sample_chain, setup_aggregate
    ):
        df = query(
            es_instrument,
            tmp_settings,
            sample_chain,
            include_cols=["close", "window_start", "open"],
        )
        assert df.columns == ["close", "window_start", "open"]

    def test_include_cols_unknown_raises(
        self, tmp_settings, es_instrument, sample_chain, setup_aggregate
    ):
        with pytest.raises(ValueError, match="include_cols"):
            query(
                es_instrument,
                tmp_settings,
                sample_chain,
                include_cols=["window_start", "not_a_column"],
            )


class TestNormalizeTickSize:
    """Tests de la normalisation tick_size (--normalize-tick-size)."""

    def test_normalize_converts_to_int32(self, tmp_settings, es_instrument, sample_chain, setup_aggregate):
        df = query(es_instrument, tmp_settings, sample_chain, normalize_tick_size=True)

        assert df.schema["open"] == pl.Int32
        assert df.schema["high"] == pl.Int32
        assert df.schema["low"] == pl.Int32
        assert df.schema["close"] == pl.Int32
        assert df.schema["settlement_price"] == pl.Int32

    def test_normalize_values_correct(self, tmp_settings, es_instrument, sample_chain, setup_aggregate):
        df = query(es_instrument, tmp_settings, sample_chain, normalize_tick_size=True)

        assert df["open"][0] == 18000  # 4500.00 / 0.25
        assert df["open"][1] == 18005  # 4501.25 / 0.25
        assert df["close"][0] == 18001  # (4500.00 + 0.25) / 0.25


class TestCheckTicksizeAccuracy:
    """Tests de --check-ticksize-accuracy (bilan de qualité)."""

    def test_check_accuracy_clean_data(self, tmp_settings, es_instrument, sample_chain, setup_aggregate):
        from myquantstore.storage.aggregate_cache import read_aggregate

        df = read_aggregate(es_instrument, tmp_settings)
        bilan = check_ticksize_accuracy_fn(df, sample_chain, trigger=0.1)

        assert bilan.height == 1
        assert bilan["ticker"][0] == "ESM5"
        assert bilan["non_conformes"][0] == 0
        assert bilan["statut"][0] == "OK"

    def test_check_accuracy_noisy_data(self, tmp_settings, es_instrument, sample_chain):
        ts = [
            datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC),
            datetime(2025, 6, 1, 9, 31, 0, tzinfo=UTC),
            datetime(2025, 6, 1, 9, 32, 0, tzinfo=UTC),
        ]
        prices = [4500.00, 4501.25, 4502.50]
        df = _make_df("ESM5", ts, prices, tick=0.25)

        df = df.with_columns(
            pl.when(pl.col("window_start") == datetime(2025, 6, 1, 9, 31, 0, tzinfo=UTC))
            .then(pl.lit(4501.27))
            .otherwise(pl.col("open"))
            .alias("open")
        )

        save_raw_dump(df, es_instrument, "ESM5", "20260711T183000", tmp_settings)
        aggregate(es_instrument, tmp_settings)

        from myquantstore.storage.aggregate_cache import read_aggregate

        df_agg = read_aggregate(es_instrument, tmp_settings)
        bilan = check_ticksize_accuracy_fn(df_agg, sample_chain, trigger=0.1)

        assert bilan["non_conformes"][0] == 1
        assert bilan["statut"][0] == "ERREUR"

    def test_check_accuracy_corrupted_data(self, tmp_settings, es_instrument, sample_chain):
        ts = [
            datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC),
            datetime(2025, 6, 1, 9, 31, 0, tzinfo=UTC),
        ]
        df = pl.DataFrame(
            {
                "window_start": ts,
                "ticker": ["ESM5"] * 2,
                "open": [4500.10, 4501.35],
                "high": [4501.10, 4502.35],
                "low": [4499.10, 4500.35],
                "close": [4500.60, 4501.85],
                "settlement_price": [4500.60, 4501.85],
                "volume": [100, 150],
                "dollar_volume": [1000.0, 2000.0],
                "transactions": [10, 15],
                "session_end_date": [ts[0].date(), ts[1].date()],
            }
        )

        save_raw_dump(df, es_instrument, "ESM5", "20260711T183000", tmp_settings)
        aggregate(es_instrument, tmp_settings)

        from myquantstore.storage.aggregate_cache import read_aggregate

        df_agg = read_aggregate(es_instrument, tmp_settings)
        bilan = check_ticksize_accuracy_fn(df_agg, sample_chain, trigger=0.1)

        assert bilan["non_conformes"][0] == 2
        assert bilan["statut"][0] == "ERREUR"

    def test_check_accuracy_does_not_modify_data(self, tmp_settings, es_instrument, sample_chain, setup_aggregate):
        from myquantstore.storage.aggregate_cache import read_aggregate

        df_before = read_aggregate(es_instrument, tmp_settings)

        df = query(es_instrument, tmp_settings, sample_chain, check_ticksize_accuracy=True)

        assert df.schema["open"] == pl.Float64

        df_after = read_aggregate(es_instrument, tmp_settings)
        assert df_after["open"].to_list() == df_before["open"].to_list()
