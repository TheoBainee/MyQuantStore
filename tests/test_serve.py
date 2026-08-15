"""Tests de l'API ``myquantstore serve`` (FastAPI query réseau)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from io import BytesIO

import polars as pl
from fastapi.testclient import TestClient

from myquantstore.cli import _build_parser
from myquantstore.instruments import RESOLUTION_1DAY, Instrument
from myquantstore.pipeline.aggregator import aggregate
from myquantstore.serve.server import _ARROW_MEDIA, _PARQUET_MEDIA, create_serve_app
from myquantstore.storage.parquet_io import write_parquet
from myquantstore.storage.raw_dumps import save_raw_dump


def _make_ohlcv_df(ticker: str, timestamps: list[datetime], prices: list[float]) -> pl.DataFrame:
    n = len(timestamps)
    return pl.DataFrame(
        {
            "window_start": timestamps,
            "ticker": [ticker] * n,
            "open": prices,
            "high": [p + 1 for p in prices],
            "low": [p - 1 for p in prices],
            "close": [p + 0.5 for p in prices],
            "settlement_price": [p + 0.5 for p in prices],
            "volume": [100] * n,
            "dollar_volume": [1000.0] * n,
            "transactions": [10] * n,
            "session_end_date": [ts.date() for ts in timestamps],
        }
    )


def _seed_1min(
    instrument: Instrument,
    tmp_settings,
    *,
    ticker: str,
    timestamps: list[datetime],
    prices: list[float],
    run_ts: str = "20260711T183000",
) -> None:
    df = _make_ohlcv_df(ticker, timestamps, prices)
    save_raw_dump(df, instrument, ticker, run_ts, tmp_settings, resolution="1min")
    aggregate(instrument, tmp_settings, resolution="1min")


def _seed_1day(
    instrument: Instrument,
    tmp_settings,
    *,
    n: int = 5,
    base: float = 4500.0,
) -> None:
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    ts = [today - timedelta(days=n - i) for i in range(n)]
    prices = [base + i * 0.5 for i in range(n)]
    df = _make_ohlcv_df(instrument.symbol, ts, prices).with_columns(
        pl.lit(instrument.symbol).alias("symbol"),
        pl.lit(instrument.type.value).alias("instrument_type"),
        pl.lit(instrument.symbol).alias("product_code"),
        pl.lit("test_run").alias("run_id"),
    )
    save_raw_dump(
        df,
        instrument,
        instrument.symbol,
        "20260711T183000",
        tmp_settings,
        resolution=RESOLUTION_1DAY,
    )
    aggregate(instrument, tmp_settings, resolution=RESOLUTION_1DAY)


def _write_contracts_cache(
    tmp_settings, sample_contracts_df: pl.DataFrame, symbol: str = "ES"
) -> None:
    path = tmp_settings.contracts_cache_path(symbol)
    write_parquet(
        sample_contracts_df,
        path,
        product_code=symbol,
        last_fetched_at=datetime.now(UTC).isoformat(),
    )


def _fresh_minutes(n: int = 5) -> list[datetime]:
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    return [now - timedelta(minutes=n - i) for i in range(n)]


class TestServeHealth:
    def test_health_503_when_aggregate_missing(self, tmp_settings):
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get("/v1/health", params={"instrument": "ES"})
        assert resp.status_code == 503
        body = resp.json()
        assert body["has_problems"] is True
        assert body["ok"] is False
        assert body["instruments"][0]["instrument"] == "futures:ES"

    def test_health_200_when_fresh(self, tmp_settings, es_instrument, sample_contracts_df):
        _seed_1min(
            es_instrument,
            tmp_settings,
            ticker="ESM5",
            timestamps=_fresh_minutes(),
            prices=[4500.0, 4501.0, 4502.0, 4501.5, 4500.5],
        )
        _seed_1day(es_instrument, tmp_settings)
        _write_contracts_cache(tmp_settings, sample_contracts_df)
        settings = tmp_settings.model_copy(update={"futures": ["ES"]})
        app = create_serve_app(settings)
        client = TestClient(app)
        resp = client.get("/v1/health", params={"instrument": "futures:ES"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["has_problems"] is False
        assert body["instruments"][0]["coverages"]["1min"]["present"] is True

    def test_health_503_when_stale(self, tmp_settings, es_instrument):
        old = [
            datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC),
            datetime(2025, 6, 1, 9, 31, 0, tzinfo=UTC),
        ]
        _seed_1min(
            es_instrument, tmp_settings, ticker="ESM5", timestamps=old, prices=[4500.0, 4501.0]
        )
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get("/v1/health", params={"instrument": "ES"})
        assert resp.status_code == 503
        codes = {i["code"] for i in resp.json()["instruments"][0]["issues"]}
        assert "stale" in codes or "missing_aggregate" in codes

    def test_health_unknown_instrument_404(self, tmp_settings):
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get("/v1/health", params={"instrument": "ZZZZ"})
        assert resp.status_code == 404


class TestServeInstruments:
    def test_lists_config_and_resolutions(self, tmp_settings, es_instrument, nq_instrument):
        _seed_1min(
            es_instrument,
            tmp_settings,
            ticker="ESM5",
            timestamps=_fresh_minutes(3),
            prices=[4500.0, 4501.0, 4502.0],
        )
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get("/v1/instruments")
        assert resp.status_code == 200
        items = {row["key"]: row for row in resp.json()["instruments"]}
        assert "futures:ES" in items
        assert "futures:NQ" in items
        assert "1min" in items["futures:ES"]["resolutions"]
        assert items["futures:NQ"]["resolutions"] == []


class TestServeQuery:
    def test_query_parquet_roundtrip_es(self, tmp_settings, es_instrument, sample_contracts_df):
        ts = [
            datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC),
            datetime(2025, 6, 1, 9, 31, 0, tzinfo=UTC),
            datetime(2025, 6, 1, 9, 32, 0, tzinfo=UTC),
        ]
        _seed_1min(
            es_instrument,
            tmp_settings,
            ticker="ESM5",
            timestamps=ts,
            prices=[4500.0, 4501.25, 4502.5],
        )
        _write_contracts_cache(tmp_settings, sample_contracts_df)
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get("/v1/query", params={"instrument": "ES"})
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith(_PARQUET_MEDIA)
        df = pl.read_parquet(BytesIO(resp.content))
        assert df.height == 3
        assert "window_start" in df.columns
        assert df["ticker"][0] == "ESM5"

    def test_query_parquet_roundtrip_nq(self, tmp_settings, nq_instrument):
        ts = [
            datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC),
            datetime(2025, 6, 1, 9, 31, 0, tzinfo=UTC),
        ]
        _seed_1min(
            nq_instrument, tmp_settings, ticker="NQM5", timestamps=ts, prices=[21000.0, 21001.0]
        )
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get("/v1/query", params={"instrument": "NQ"})
        assert resp.status_code == 200
        df = pl.read_parquet(BytesIO(resp.content))
        assert df.height == 2
        assert df["ticker"][0] == "NQM5"

    def test_query_arrow_accept(self, tmp_settings, es_instrument, sample_contracts_df):
        ts = [datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC)]
        _seed_1min(es_instrument, tmp_settings, ticker="ESM5", timestamps=ts, prices=[4500.0])
        _write_contracts_cache(tmp_settings, sample_contracts_df)
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get(
            "/v1/query",
            params={"instrument": "ES"},
            headers={"Accept": _ARROW_MEDIA},
        )
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith(_ARROW_MEDIA)
        df = pl.read_ipc(BytesIO(resp.content))
        assert df.height == 1

    def test_query_404_missing_aggregate(self, tmp_settings):
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get("/v1/query", params={"instrument": "ES"})
        assert resp.status_code == 404

    def test_query_404_unknown_instrument(self, tmp_settings):
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get("/v1/query", params={"instrument": "ZZZZ"})
        assert resp.status_code == 404

    def test_query_400_bad_timescale(self, tmp_settings, es_instrument):
        ts = [datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC)]
        _seed_1min(es_instrument, tmp_settings, ticker="ESM5", timestamps=ts, prices=[4500.0])
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get(
            "/v1/query",
            params={"instrument": "ES", "timescale_unit": "month"},
        )
        assert resp.status_code == 400

    def test_query_400_adjust_and_normalize(self, tmp_settings, es_instrument, sample_contracts_df):
        ts = [datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC)]
        _seed_1min(es_instrument, tmp_settings, ticker="ESM5", timestamps=ts, prices=[4500.0])
        _write_contracts_cache(tmp_settings, sample_contracts_df)
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get(
            "/v1/query",
            params={"instrument": "ES", "adjust": True, "normalize_tick_size": True},
        )
        assert resp.status_code == 400

    def test_query_400_intraday_one_only(self, tmp_settings, es_instrument):
        ts = [datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC)]
        _seed_1min(es_instrument, tmp_settings, ticker="ESM5", timestamps=ts, prices=[4500.0])
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get(
            "/v1/query",
            params={"instrument": "ES", "intraday_begin": "09:30"},
        )
        assert resp.status_code == 400

    def test_dedup_default_keeps_newer_contract(
        self, tmp_settings, es_instrument, sample_contracts_df
    ):
        ts = datetime(2025, 3, 7, 0, 0, 0, tzinfo=UTC)
        save_raw_dump(
            _make_ohlcv_df("ESH5", [ts], [5800.00]),
            es_instrument,
            "ESH5",
            "20250307T000000",
            tmp_settings,
        )
        save_raw_dump(
            _make_ohlcv_df("ESM5", [ts], [5810.00]),
            es_instrument,
            "ESM5",
            "20250307T000001",
            tmp_settings,
        )
        aggregate(es_instrument, tmp_settings)
        _write_contracts_cache(tmp_settings, sample_contracts_df)

        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get("/v1/query", params={"instrument": "ES"})
        assert resp.status_code == 200
        df = pl.read_parquet(BytesIO(resp.content))
        assert df.height == 1
        assert df["ticker"][0] == "ESM5"

    def test_dedup_false_keeps_both_contracts(
        self, tmp_settings, es_instrument, sample_contracts_df
    ):
        ts = datetime(2025, 3, 7, 0, 0, 0, tzinfo=UTC)
        save_raw_dump(
            _make_ohlcv_df("ESH5", [ts], [5800.00]),
            es_instrument,
            "ESH5",
            "20250307T000000",
            tmp_settings,
        )
        save_raw_dump(
            _make_ohlcv_df("ESM5", [ts], [5810.00]),
            es_instrument,
            "ESM5",
            "20250307T000001",
            tmp_settings,
        )
        aggregate(es_instrument, tmp_settings)
        _write_contracts_cache(tmp_settings, sample_contracts_df)

        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get(
            "/v1/query",
            params={"instrument": "ES", "dedup_timestamps": False},
        )
        assert resp.status_code == 200
        df = pl.read_parquet(BytesIO(resp.content))
        assert df.height == 2
        assert set(df["ticker"].to_list()) == {"ESH5", "ESM5"}


class TestServeCli:
    def test_parser_exposes_serve(self):
        parser = _build_parser()
        args = parser.parse_args(["serve", "--host", "0.0.0.0", "--port", "9000"])
        assert args.command == "serve"
        assert args.host == "0.0.0.0"
        assert args.port == 9000

    def test_parser_serve_defaults_are_none(self):
        parser = _build_parser()
        args = parser.parse_args(["serve"])
        assert args.host is None
        assert args.port is None

    def test_cmd_serve_falls_back_to_settings(self, tmp_settings, monkeypatch):
        from myquantstore.cli import _cmd_serve
        import argparse

        captured: dict[str, object] = {}

        def fake_run(settings, host="127.0.0.1", port=8741):
            captured["host"] = host
            captured["port"] = port

        monkeypatch.setattr("myquantstore.serve.server.run_server", fake_run)
        settings = tmp_settings.model_copy(update={"serve_host": "0.0.0.0", "serve_port": 9001})
        args = argparse.Namespace(host=None, port=None)
        assert _cmd_serve(settings, args) == 0
        assert captured == {"host": "0.0.0.0", "port": 9001}

    def test_cmd_serve_cli_overrides_settings(self, tmp_settings, monkeypatch):
        from myquantstore.cli import _cmd_serve
        import argparse

        captured: dict[str, object] = {}

        def fake_run(settings, host="127.0.0.1", port=8741):
            captured["host"] = host
            captured["port"] = port

        monkeypatch.setattr("myquantstore.serve.server.run_server", fake_run)
        settings = tmp_settings.model_copy(update={"serve_host": "127.0.0.1", "serve_port": 8741})
        args = argparse.Namespace(host="10.0.0.2", port=9999)
        assert _cmd_serve(settings, args) == 0
        assert captured == {"host": "10.0.0.2", "port": 9999}

    def test_help_documents_query_params(self):
        parser = _build_parser()
        serve = None
        for action in parser._subparsers._group_actions:
            for name, sub in action.choices.items():
                if name == "serve":
                    serve = sub
        assert serve is not None
        text = serve.format_help()
        assert "normalize_tick_size=true" in text
        assert "include_cols=" in text
        assert "true/false" in text
        assert "/v1/query" in text
        assert "/v1/instruments" in text

class TestServeQueryExtras:
    def test_include_cols_filters(self, tmp_settings, es_instrument, sample_contracts_df):
        from datetime import UTC, datetime

        ts = [datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC)]
        _seed_1min(es_instrument, tmp_settings, ticker="ESM5", timestamps=ts, prices=[4500.0])
        _write_contracts_cache(tmp_settings, sample_contracts_df)
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get(
            "/v1/query",
            params={"instrument": "ES", "include_cols": "window_start,open,close"},
        )
        assert resp.status_code == 200
        df = pl.read_parquet(BytesIO(resp.content))
        assert df.columns == ["window_start", "open", "close"]

    def test_include_cols_unknown_400(self, tmp_settings, es_instrument, sample_contracts_df):
        from datetime import UTC, datetime

        ts = [datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC)]
        _seed_1min(es_instrument, tmp_settings, ticker="ESM5", timestamps=ts, prices=[4500.0])
        _write_contracts_cache(tmp_settings, sample_contracts_df)
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get(
            "/v1/query",
            params={"instrument": "ES", "include_cols": "window_start,not_a_column"},
        )
        assert resp.status_code == 400
        assert "include_cols" in resp.json()["detail"]

    def test_normalize_tick_size_true(self, tmp_settings, es_instrument, sample_contracts_df):
        from datetime import UTC, datetime

        ts = [datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC)]
        _seed_1min(es_instrument, tmp_settings, ticker="ESM5", timestamps=ts, prices=[4500.0])
        _write_contracts_cache(tmp_settings, sample_contracts_df)
        app = create_serve_app(tmp_settings)
        client = TestClient(app)
        resp = client.get(
            "/v1/query",
            params={"instrument": "ES", "normalize_tick_size": "true"},
        )
        assert resp.status_code == 200
        df = pl.read_parquet(BytesIO(resp.content))
        assert df.schema["open"] == pl.Int32
        assert df["open"][0] == 18000  # 4500 / 0.25


class TestServeInstrumentsFutures:
    def test_futures_extras_from_local_cache(
        self, tmp_settings, es_instrument, sample_contracts_df
    ):
        from datetime import UTC, datetime

        ts = [datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC)]
        _seed_1min(es_instrument, tmp_settings, ticker="ESM5", timestamps=ts, prices=[4500.0])
        _write_contracts_cache(tmp_settings, sample_contracts_df)
        settings = tmp_settings.model_copy(update={"futures": ["ES"]})
        app = create_serve_app(settings)
        client = TestClient(app)
        resp = client.get("/v1/instruments")
        assert resp.status_code == 200
        items = {row["key"]: row for row in resp.json()["instruments"]}
        es = items["futures:ES"]
        assert es["trade_tick_size"] == 0.25
        assert "ESM5" in es["tickers"]
        assert es["current_ticker"] is not None
        assert es["last_trade_date"] is not None
        assert isinstance(es["days_to_maturity"], int)

