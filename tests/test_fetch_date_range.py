"""Tests ``fetch --start-date / --end-date`` (plage explicite) et incrémental futures.

Couvre :

- validation de la plage (:func:`resolve_fetch_date_range`) et parsing CLI ;
- propagation CLI → ``run_fetch`` ;
- futures : segments / bornes sur la plage explicite, contournement du skip
  « dump du jour », incrémental réel (plus de re-fetch de tout ``history_months``) ;
- stocks end-to-end : un backfill explicite comble un trou dans l'agrégat et
  remplace les barres existantes de la plage (dédup ``keep="last"``).
"""

from __future__ import annotations

from argparse import Namespace
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import httpx
import polars as pl
import pytest
import respx

from myquantstore.api.client import MassiveClient
from myquantstore.cli import _build_parser, _cmd_fetch
from myquantstore.contracts.rollover import RolloverChain
from myquantstore.instruments import Instrument, InstrumentType
from myquantstore.pipeline.fetchers.futures import FuturesFetcher, _determine_segment_range
from myquantstore.pipeline.fetchers.stocks import StocksFetcher
from myquantstore.pipeline.historian import resolve_fetch_date_range

TODAY = date(2026, 9, 29)


class TestResolveFetchDateRange:
    def test_none_is_auto(self):
        assert resolve_fetch_date_range(None, None, TODAY) == (None, None)

    def test_start_only_ends_today(self):
        assert resolve_fetch_date_range(date(2026, 3, 9), None, TODAY) == (
            date(2026, 3, 9),
            TODAY,
        )

    def test_start_and_end(self):
        assert resolve_fetch_date_range(date(2026, 3, 9), date(2026, 3, 13), TODAY) == (
            date(2026, 3, 9),
            date(2026, 3, 13),
        )

    def test_end_only_rejected(self):
        with pytest.raises(ValueError, match="--end-date requiert --start-date"):
            resolve_fetch_date_range(None, date(2026, 3, 13), TODAY)

    def test_future_end_clamped_to_today(self):
        assert resolve_fetch_date_range(date(2026, 9, 1), date(2026, 12, 31), TODAY) == (
            date(2026, 9, 1),
            TODAY,
        )

    def test_start_after_end_rejected(self):
        with pytest.raises(ValueError, match="postérieure"):
            resolve_fetch_date_range(date(2026, 3, 14), date(2026, 3, 13), TODAY)


class TestFetchCliFlags:
    def test_parser_parses_dates(self):
        args = _build_parser().parse_args(
            ["fetch", "--start-date", "2026-03-09", "--end-date", "2026-03-13"]
        )
        assert args.start_date == date(2026, 3, 9)
        assert args.end_date == date(2026, 3, 13)

    def test_parser_defaults_none(self):
        args = _build_parser().parse_args(["fetch"])
        assert args.start_date is None
        assert args.end_date is None

    @pytest.mark.parametrize("bad", ["2026-3-9", "09/03/2026", "2026-03-09T10:00", "nope"])
    def test_parser_rejects_bad_format(self, bad):
        with pytest.raises(SystemExit):
            _build_parser().parse_args(["fetch", "--start-date", bad])

    @staticmethod
    def _patch_client(monkeypatch):
        class _Dummy:
            def __enter__(self):
                return self

            def __exit__(self, *args: object) -> None:
                return None

        monkeypatch.setattr("myquantstore.api.client.MassiveClient", lambda *a, **k: _Dummy())

    @staticmethod
    def _args(**kw):
        base: dict[str, object] = {
            "instrument": "ES",
            "type": "futures",
            "timeframe": "1day",
            "dry_run": False,
            "force": False,
            "no_cascade": True,
            "start_date": None,
            "end_date": None,
        }
        base.update(kw)
        return Namespace(**base)

    def test_cmd_fetch_forwards_range(self, tmp_settings, monkeypatch):
        self._patch_client(monkeypatch)
        seen: dict[str, object] = {}

        def fake_run_fetch(*a, **k):
            seen.update(k)
            return {"futures:ES[1day]": {"status": "ok", "candles": 1}}

        monkeypatch.setattr("myquantstore.pipeline.historian.run_fetch", fake_run_fetch)
        args = self._args(start_date=date(2026, 3, 9), end_date=date(2026, 3, 13))
        assert _cmd_fetch(tmp_settings, args) == 0
        assert seen["start_date"] == date(2026, 3, 9)
        assert seen["end_date"] == date(2026, 3, 13)

    def test_cmd_fetch_end_without_start_fails(self, tmp_settings, monkeypatch):
        self._patch_client(monkeypatch)
        called = []
        monkeypatch.setattr(
            "myquantstore.pipeline.historian.run_fetch", lambda *a, **k: called.append(1)
        )
        assert _cmd_fetch(tmp_settings, self._args(end_date=date(2026, 3, 13))) == 1
        assert called == []


class TestDetermineSegmentRangeExplicit:
    def _seg(self, start: date, until: date):
        return SimpleNamespace(active_from=start, active_until=until)

    def test_explicit_range_intersects_segment(self, tmp_settings):
        seg = self._seg(date(2026, 3, 1), date(2026, 6, 1))
        gte, lte = _determine_segment_range(
            seg,
            date(2024, 1, 1),
            TODAY,
            date(2024, 1, 1),
            TODAY,
            tmp_settings,
            start_date=date(2026, 2, 20),
            end_date=date(2026, 3, 13),
        )
        assert (gte, lte) == ("2026-03-01", "2026-03-13")

    def test_explicit_range_outside_segment_is_empty(self, tmp_settings):
        seg = self._seg(date(2026, 3, 1), date(2026, 6, 1))
        assert _determine_segment_range(
            seg,
            date(2024, 1, 1),
            TODAY,
            None,
            None,
            tmp_settings,
            start_date=date(2026, 7, 1),
            end_date=date(2026, 7, 10),
        ) == (None, None)

    def test_incremental_skips_old_segments(self, tmp_settings):
        """Régression : l'incrémental ne re-fetch plus tout history_months."""
        latest = date(2026, 9, 25)
        old = self._seg(date(2025, 12, 15), date(2026, 3, 16))
        gte, lte = _determine_segment_range(
            old, TODAY - timedelta(days=720), TODAY, date(2024, 10, 1), latest, tmp_settings
        )
        assert (gte, lte) == (None, None)

    def test_incremental_starts_at_latest_minus_buffer(self, tmp_settings):
        latest = date(2026, 9, 25)
        current = self._seg(date(2026, 9, 14), date(2026, 12, 14))
        gte, lte = _determine_segment_range(
            current, TODAY - timedelta(days=720), TODAY, date(2024, 10, 1), latest, tmp_settings
        )
        buffer = tmp_settings.overlap_buffer_days
        assert gte == (latest - timedelta(days=buffer)).isoformat()
        assert lte == TODAY.isoformat()


class TestFuturesFetcherExplicitRange:
    @pytest.fixture
    def patched(self, monkeypatch, sample_contracts_df):
        """ContractsCache / API / agrégation / coverage neutralisés ; enregistre les calls."""
        mod = "myquantstore.pipeline.fetchers.futures"
        calls: list[tuple[str, str | None, str | None]] = []

        class _Cache:
            def __init__(self, *a, **k):
                pass

            def get(self, client):
                return sample_contracts_df

        def fake_fetch(client, ticker, settings, window_start_gte=None, window_start_lte=None):
            calls.append((ticker, window_start_gte, window_start_lte))
            return pl.DataFrame()

        monkeypatch.setattr(f"{mod}.ContractsCache", _Cache)
        monkeypatch.setattr(f"{mod}.fetch_aggs_futures", fake_fetch)
        monkeypatch.setattr(f"{mod}.aggregate", lambda *a, **k: None)
        monkeypatch.setattr(f"{mod}.attach_coverage_fields", lambda *a, **k: None)
        return calls

    def test_explicit_range_fetches_active_contracts_and_ignores_day_skip(
        self, patched, monkeypatch, tmp_settings, sample_contracts_df
    ):
        mod = "myquantstore.pipeline.fetchers.futures"
        # Dump du jour présent pour tous les tickers : ne doit PAS bloquer le backfill.
        monkeypatch.setattr(f"{mod}.has_run_today", lambda *a, **k: (True, "20260929T070000"))
        monkeypatch.setattr(f"{mod}.raw_dumps_exist", lambda *a, **k: True)
        monkeypatch.setattr(
            f"{mod}.get_aggregate_date_range",
            lambda *a, **k: (date(2024, 12, 16), date(2025, 9, 1)),
        )

        chain = RolloverChain("ES", sample_contracts_df, tmp_settings.days_before_expiry)
        start, end = date(2025, 3, 3), date(2025, 3, 20)
        expected = []
        for seg in chain.continuous_segments(start, end):
            lo = max(start, seg.active_from)
            hi = min(end, seg.active_until)
            if lo <= hi:
                expected.append((seg.ticker, lo.isoformat(), hi.isoformat()))
        assert len(expected) >= 2  # la plage traverse un roll

        inst = Instrument(InstrumentType.FUTURES, "ES")
        result = FuturesFetcher().fetch(
            inst,
            tmp_settings,
            None,  # type: ignore[arg-type]
            start_date=start,
            end_date=end,
        )
        assert patched == expected
        assert result["skipped_segments"] == 0

    def test_without_range_day_skip_still_applies(self, patched, monkeypatch, tmp_settings):
        mod = "myquantstore.pipeline.fetchers.futures"
        monkeypatch.setattr(f"{mod}.has_run_today", lambda *a, **k: (True, "20260929T070000"))
        monkeypatch.setattr(f"{mod}.raw_dumps_exist", lambda *a, **k: False)

        inst = Instrument(InstrumentType.FUTURES, "ES")
        result = FuturesFetcher().fetch(inst, tmp_settings, None)  # type: ignore[arg-type]
        assert patched == []
        assert result["status"] == "skipped"


def _ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=UTC).timestamp() * 1000)


def _bar(dt: datetime, price: float) -> dict:
    return {
        "o": price,
        "h": price + 1,
        "l": price - 1,
        "c": price,
        "v": 100,
        "n": 10,
        "t": _ms(dt),
        "vw": price,
    }


class TestStocksBackfill:
    @pytest.fixture
    def stocks_settings(self, tmp_settings):
        return tmp_settings.model_copy(update={"futures": [], "stocks": ["AAPL"]})

    @pytest.fixture
    def client(self, stocks_settings):
        c = MassiveClient(stocks_settings)
        yield c
        c.close()

    @respx.mock
    def test_backfill_fills_gap_and_overrides_range(self, client, stocks_settings, monkeypatch):
        """Fetch initial avec un trou (mardi), puis backfill explicite du trou.

        Même jour que le fetch initial : le skip « dump du jour » est contourné.
        La barre du lundi re-servie par le backfill (corrigée) remplace l'ancienne.
        """
        # run_ts à la seconde : deux runs distincts (le test tourne en < 1 s).
        run_ids = iter(["20260929T070000", "20260929T080000"])
        monkeypatch.setattr(
            "myquantstore.pipeline.fetchers.stocks.generate_run_ts", lambda: next(run_ids)
        )
        respx.get("/stocks/v1/splits").mock(
            return_value=httpx.Response(200, json={"status": "OK", "results": []})
        )
        respx.get("/stocks/v1/dividends").mock(
            return_value=httpx.Response(200, json={"status": "OK", "results": []})
        )
        mon = datetime(2026, 3, 9, 14, 30)
        tue = datetime(2026, 3, 10, 14, 30)
        wed = datetime(2026, 3, 11, 14, 30)

        route = respx.get(url__regex=r"/v2/aggs/ticker/AAPL/range/1/minute/.*")
        route.side_effect = [
            httpx.Response(200, json={"status": "OK", "results": [_bar(mon, 100), _bar(wed, 102)]}),
            httpx.Response(200, json={"status": "OK", "results": [_bar(mon, 150), _bar(tue, 101)]}),
        ]

        inst = Instrument(InstrumentType.STOCKS, "AAPL")
        first = StocksFetcher().fetch(inst, stocks_settings, client)
        assert first["status"] == "ok"

        second = StocksFetcher().fetch(
            inst,
            stocks_settings,
            client,
            start_date=date(2026, 3, 9),
            end_date=date(2026, 3, 10),
        )
        assert second["status"] == "ok"
        assert route.calls[-1].request.url.path.endswith("/2026-03-09/2026-03-10")

        from myquantstore.storage.aggregate_cache import read_aggregate

        agg = read_aggregate(inst, stocks_settings).sort("window_start")
        closes = dict(
            zip(
                [ws.replace(tzinfo=None) for ws in agg["window_start"].to_list()],
                agg["close"].to_list(),
                strict=True,
            )
        )
        assert closes == {mon: 150.0, tue: 101.0, wed: 102.0}

    def test_without_range_same_day_is_skipped(self, client, stocks_settings, monkeypatch):
        monkeypatch.setattr(
            "myquantstore.pipeline.fetchers.stocks.has_run_today",
            lambda *a, **k: (True, "20260929T070000"),
        )
        inst = Instrument(InstrumentType.STOCKS, "AAPL")
        result = StocksFetcher().fetch(inst, stocks_settings, client)
        assert result["status"] == "skipped"
