"""Tests du module storage/raw_dumps.py."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from myquantstore.config import generate_run_ts
from myquantstore.storage.raw_dumps import (
    get_latest_run_date,
    has_run_today,
    list_runs,
    list_tickers,
    raw_dumps_exist,
    read_all_runs,
    save_raw_dump,
)


@pytest.fixture
def sample_df():
    """DataFrame de chandeliers pour les tests de raw_dumps."""
    return pl.DataFrame(
        {
            "window_start": [
                datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC),
                datetime(2025, 6, 1, 9, 31, 0, tzinfo=UTC),
            ],
            "ticker": ["ESM5", "ESM5"],
            "open": [4500.0, 4501.0],
            "high": [4501.0, 4502.0],
            "low": [4499.0, 4500.0],
            "close": [4500.5, 4501.5],
            "volume": [100, 150],
        }
    )


class TestSaveRawDump:
    """Tests de save_raw_dump."""

    def test_save_creates_file(self, tmp_settings, es_instrument, sample_df):
        """save_raw_dump crée le fichier Parquet + sidecar (layout multi-type)."""
        run_ts = generate_run_ts()
        path = save_raw_dump(sample_df, es_instrument, "ESM5", run_ts, tmp_settings)

        assert path.exists()
        assert path.with_suffix(".meta.json").exists()
        # Le layout intègre le type
        assert "futures" in str(path)

    def test_save_multiple_runs(self, tmp_settings, es_instrument, sample_df):
        """Plusieurs runs créent plusieurs fichiers (jamais écrasés)."""
        save_raw_dump(sample_df, es_instrument, "ESM5", "20260704T180000", tmp_settings)
        save_raw_dump(sample_df, es_instrument, "ESM5", "20260711T183000", tmp_settings)

        runs = list_runs(es_instrument, "ESM5", tmp_settings)
        assert len(runs) == 2
        assert "20260704T180000" in runs
        assert "20260711T183000" in runs


class TestListFunctions:
    """Tests de list_runs, list_tickers, raw_dumps_exist."""

    def test_list_runs_empty(self, tmp_settings, es_instrument):
        assert list_runs(es_instrument, "ESM5", tmp_settings) == []

    def test_list_tickers_empty(self, tmp_settings, es_instrument):
        assert list_tickers(es_instrument, tmp_settings) == []

    def test_list_tickers_multiple(self, tmp_settings, es_instrument, sample_df):
        """list_tickers retourne tous les tickers d'un instrument futures."""
        save_raw_dump(sample_df, es_instrument, "ESM5", "20260711T183000", tmp_settings)
        save_raw_dump(sample_df, es_instrument, "ESU5", "20260711T183000", tmp_settings)

        tickers = list_tickers(es_instrument, tmp_settings)
        assert "ESM5" in tickers
        assert "ESU5" in tickers
        assert len(tickers) == 2

    def test_raw_dumps_exist_false(self, tmp_settings, es_instrument):
        assert not raw_dumps_exist(es_instrument, tmp_settings)

    def test_raw_dumps_exist_true(self, tmp_settings, es_instrument, sample_df):
        save_raw_dump(sample_df, es_instrument, "ESM5", "20260711T183000", tmp_settings)
        assert raw_dumps_exist(es_instrument, tmp_settings)


class TestReadAllRuns:
    """Tests de read_all_runs."""

    def test_read_all_runs_concatenates(self, tmp_settings, es_instrument, sample_df):
        """read_all_runs concatène tous les dumps avec run_id + symbol + instrument_type."""
        save_raw_dump(sample_df, es_instrument, "ESM5", "20260704T180000", tmp_settings)
        save_raw_dump(sample_df, es_instrument, "ESM5", "20260711T183000", tmp_settings)

        df = read_all_runs(es_instrument, tmp_settings)

        assert df.height == 4  # 2 lignes x 2 dumps
        assert "run_id" in df.columns
        assert "symbol" in df.columns
        assert "instrument_type" in df.columns

    def test_read_all_runs_empty(self, tmp_settings, es_instrument):
        df = read_all_runs(es_instrument, tmp_settings)
        assert df.is_empty()


class TestHasRunToday:
    """Tests de has_run_today."""

    def test_has_run_today_true(self, tmp_settings, es_instrument, sample_df):
        today_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        save_raw_dump(sample_df, es_instrument, "ESM5", today_ts, tmp_settings)

        done, run_ts = has_run_today(es_instrument, tmp_settings)
        assert done is True
        assert run_ts is not None
        assert run_ts.startswith(datetime.now(UTC).strftime("%Y%m%d"))

    def test_has_run_today_false(self, tmp_settings, es_instrument, sample_df):
        save_raw_dump(sample_df, es_instrument, "ESM5", "20200101T120000", tmp_settings)

        done, run_ts = has_run_today(es_instrument, tmp_settings)
        assert done is False
        assert run_ts is None

    def test_has_run_today_no_dumps(self, tmp_settings, es_instrument):
        done, run_ts = has_run_today(es_instrument, tmp_settings)
        assert done is False
        assert run_ts is None

    def test_has_run_today_per_ticker(self, tmp_settings, es_instrument, sample_df):
        """Skip par segment : dump ESM5 aujourd'hui n'implique pas ESU5."""
        today_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        save_raw_dump(sample_df, es_instrument, "ESM5", today_ts, tmp_settings)

        done_esm, _ = has_run_today(es_instrument, tmp_settings, ticker="ESM5")
        done_esu, _ = has_run_today(es_instrument, tmp_settings, ticker="ESU5")
        done_any, _ = has_run_today(es_instrument, tmp_settings)

        assert done_esm is True
        assert done_esu is False
        assert done_any is True


class TestGetLatestRunDate:
    """Tests de get_latest_run_date."""

    def test_latest_run_date(self, tmp_settings, es_instrument, sample_df):
        save_raw_dump(sample_df, es_instrument, "ESM5", "20260704T180000", tmp_settings)
        save_raw_dump(sample_df, es_instrument, "ESU5", "20260711T183000", tmp_settings)

        latest = get_latest_run_date(es_instrument, tmp_settings)
        assert latest == "20260711"

    def test_latest_run_date_none(self, tmp_settings, es_instrument):
        assert get_latest_run_date(es_instrument, tmp_settings) is None
