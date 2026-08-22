"""Tests du package pipeline/fetchers (dispatch multi-type + scaffold options)."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from myquantstore.instruments import Instrument, InstrumentType
from myquantstore.pipeline.fetchers import get_fetcher
from myquantstore.pipeline.fetchers.base import InstrumentFetcher
from myquantstore.pipeline.fetchers.futures import FuturesFetcher, _determine_segment_range
from myquantstore.pipeline.fetchers.options import OptionsFetcher
from myquantstore.pipeline.fetchers.stocks import StocksFetcher
from myquantstore.pipeline.fetchers.v2_single import V2SingleSymbolFetcher


class TestGetFetcher:
    def test_futures_returns_futures_fetcher(self):
        fetcher = get_fetcher(Instrument(InstrumentType.FUTURES, "ES"))
        assert isinstance(fetcher, FuturesFetcher)
        assert isinstance(fetcher, InstrumentFetcher)

    def test_stocks_returns_stocks_fetcher(self):
        fetcher = get_fetcher(Instrument(InstrumentType.STOCKS, "AAPL"))
        assert isinstance(fetcher, StocksFetcher)

    def test_options_returns_options_fetcher(self):
        fetcher = get_fetcher(Instrument(InstrumentType.OPTIONS, "AAPL"))
        assert isinstance(fetcher, OptionsFetcher)

    def test_forex_returns_v2_single_fetcher(self):
        fetcher = get_fetcher(Instrument(InstrumentType.FOREX, "EURUSD"))
        assert isinstance(fetcher, V2SingleSymbolFetcher)

    def test_indices_returns_v2_single_fetcher(self):
        fetcher = get_fetcher(Instrument(InstrumentType.INDICES, "NDX"))
        assert isinstance(fetcher, V2SingleSymbolFetcher)


class TestOptionsFetcherScaffold:
    def test_fetch_raises_not_implemented(self, tmp_settings):
        from myquantstore.api.client import MassiveClient

        fetcher = OptionsFetcher()
        client = MassiveClient(tmp_settings)
        try:
            with pytest.raises(NotImplementedError, match="options"):
                fetcher.fetch(
                    Instrument(InstrumentType.OPTIONS, "AAPL"),
                    tmp_settings,
                    client,
                )
        finally:
            client.close()


class TestDetermineSegmentRange:
    def test_inclusive_single_day_on_roll(self, tmp_settings):
        today = date(2026, 8, 22)
        seg = SimpleNamespace(active_from=today, active_until=today)
        gte, lte = _determine_segment_range(
            seg, date(2026, 1, 1), today, None, None, tmp_settings
        )
        assert gte == "2026-08-22"
        assert lte == "2026-08-22"

    def test_empty_when_start_after_end(self, tmp_settings):
        seg = SimpleNamespace(active_from=date(2026, 8, 23), active_until=date(2026, 8, 22))
        gte, lte = _determine_segment_range(
            seg, date(2026, 1, 1), date(2026, 8, 22), None, None, tmp_settings
        )
        assert gte is None
        assert lte is None
