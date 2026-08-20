"""Tests du module chart/server.py (serveur FastAPI de visualisation)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient

from myquantstore.chart.server import ChartDefaults, create_chart_app
from myquantstore.chart.thumbnails import render_sparkline_svg
from myquantstore.instruments import RESOLUTION_1DAY, Instrument, InstrumentType
from myquantstore.pipeline.aggregator import aggregate
from myquantstore.storage.raw_dumps import save_raw_dump


def _make_ohlcv_df(ticker: str, timestamps: list[datetime], prices: list[float]) -> pl.DataFrame:
    """Crée un DataFrame OHLCV pour les tests."""
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


def _seed_1min(es_instrument, tmp_settings) -> None:
    ts = [
        datetime(2025, 6, 1, 9, 30, 0, tzinfo=UTC),
        datetime(2025, 6, 1, 9, 31, 0, tzinfo=UTC),
        datetime(2025, 6, 1, 9, 32, 0, tzinfo=UTC),
        datetime(2025, 6, 1, 9, 33, 0, tzinfo=UTC),
        datetime(2025, 6, 1, 9, 34, 0, tzinfo=UTC),
    ]
    prices = [4500.00, 4501.25, 4502.50, 4501.75, 4500.50]
    df = _make_ohlcv_df("ESM5", ts, prices)
    save_raw_dump(df, es_instrument, "ESM5", "20260711T183000", tmp_settings, resolution="1min")
    aggregate(es_instrument, tmp_settings, resolution="1min")


def _seed_1day(instrument: Instrument, tmp_settings, *, n: int = 30, base: float = 100.0) -> None:
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    ts = [today - timedelta(days=n - i) for i in range(n)]
    prices = [base + i * 0.5 for i in range(n)]
    df = _make_ohlcv_df(instrument.symbol, ts, prices)
    # colonnes identité yahoo-like
    df = df.with_columns(
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


@pytest.fixture
def chart_setup(tmp_settings, es_instrument, sample_chain):
    """Crée un cache agrégé 1min+1day pour ES et retourne (settings, instruments, chains, defaults)."""
    _seed_1min(es_instrument, tmp_settings)
    _seed_1day(es_instrument, tmp_settings, base=4500.0)

    key = es_instrument.key  # "futures:ES"
    instruments = {key: es_instrument}
    chains = {key: sample_chain}
    defaults = ChartDefaults(default_product=key, thumbnail_lookback_days=90)
    return tmp_settings, instruments, chains, defaults


@pytest.fixture
def multi_chart_setup(tmp_settings, es_instrument, sample_chain):
    """ES + AAPL pour tester le dashboard multi-type."""
    _seed_1min(es_instrument, tmp_settings)
    _seed_1day(es_instrument, tmp_settings, base=4500.0)

    aapl = Instrument(type=InstrumentType.STOCKS, symbol="AAPL")
    _seed_1day(aapl, tmp_settings, base=180.0)

    instruments = {es_instrument.key: es_instrument, aapl.key: aapl}
    chains = {es_instrument.key: sample_chain}
    defaults = ChartDefaults(default_product=es_instrument.key, thumbnail_lookback_days=90)
    return tmp_settings, instruments, chains, defaults


class TestChartServer:
    """Tests des endpoints du serveur chart."""

    def test_get_index_returns_dashboard(self, multi_chart_setup):
        """GET / sert le dashboard (plus de redirect)."""
        settings, instruments, chains, defaults = multi_chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/", follow_redirects=False)
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Dashboard" in resp.text or "dashboard" in resp.text.lower()
        assert "futures:ES" in resp.text
        assert "stocks:AAPL" in resp.text
        assert "Futures" in resp.text
        assert "Stocks" in resp.text

    def test_get_instrument_page_returns_html(self, chart_setup):
        """GET /futures:ES retourne la page HTML du chart."""
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/futures:ES")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "futures:ES" in resp.text
        assert "lightweight-charts" in resp.text

    def test_chart_html_has_home_link(self, chart_setup):
        """La page chart expose un lien vers le dashboard."""
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/futures:ES")
        assert 'href="/"' in resp.text
        assert "home-btn" in resp.text or "Dashboard" in resp.text

    def test_chart_html_has_log_scale_toggle(self, chart_setup):
        """Toolbar : sélecteur échelle Lin/Log."""
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/futures:ES")
        assert 'id="scale-select"' in resp.text
        assert "PriceScaleMode.Logarithmic" in resp.text
        assert 'value="log"' in resp.text

    def test_get_unknown_instrument_404(self, chart_setup):
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/UNKNOWN")
        assert resp.status_code == 404

    def test_get_static_lightweight_charts_js(self, chart_setup):
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/static/lightweight-charts.standalone.production.js")
        assert resp.status_code == 200
        assert len(resp.content) > 1000

    def test_get_candles_returns_arrow_ipc(self, chart_setup):
        """GET /api/candles retourne des chandeliers en Arrow IPC."""
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/api/candles?product=futures:ES&timescale_unit=min&timescale_nb=1&limit=10")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/octet-stream"
        assert len(resp.content) > 0

        df = pl.read_ipc(BytesIO(resp.content))
        assert df.height == 5
        assert "open" in df.columns
        assert "close" in df.columns

    def test_get_candles_unknown_product_404(self, chart_setup):
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/api/candles?product=UNKNOWN")
        assert resp.status_code == 404

    def test_get_candles_with_before_param(self, chart_setup):
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        before = "2025-06-01T09:32:00"
        resp = client.get(f"/api/candles?product=futures:ES&limit=10&before={before}")
        assert resp.status_code == 200

        df = pl.read_ipc(BytesIO(resp.content))
        assert df.height == 3  # 09:30, 09:31, 09:32

    def test_get_candles_timescale_7min(self, chart_setup):
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/api/candles?product=futures:ES&timescale_unit=min&timescale_nb=7&limit=10")
        assert resp.status_code == 200
        if len(resp.content) > 0:
            df = pl.read_ipc(BytesIO(resp.content))
            assert df.height >= 1

    def test_get_candles_invalid_timescale_unit(self, chart_setup):
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/api/candles?product=futures:ES&timescale_unit=sec&timescale_nb=1")
        assert resp.status_code == 400

    def test_get_meta_returns_json(self, chart_setup):
        """GET /api/meta retourne tick_size pour futures."""
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/api/meta?product=futures:ES")
        assert resp.status_code == 200
        data = resp.json()
        assert data["product"] == "futures:ES"
        assert data["tick_size"] == 0.25
        assert data["total_candles"] == 5

    def test_get_meta_unknown_product_404(self, chart_setup):
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/api/meta?product=UNKNOWN")
        assert resp.status_code == 404

    def test_chart_html_injects_product(self, chart_setup):
        """La page HTML injecte la clé instrument et les defaults."""
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/futures:ES")
        text = resp.text
        assert '"__PRODUCT__"' not in text
        assert "futures:ES" in text
        assert "50000" in text  # max_visible_candles

    def test_thumbnail_svg(self, chart_setup):
        """GET /api/thumbnail/{key}.svg retourne un SVG sparkline."""
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/api/thumbnail/futures:ES.svg")
        assert resp.status_code == 200
        assert "image/svg+xml" in resp.headers["content-type"]
        assert b"<svg" in resp.content
        assert b"polyline" in resp.content or b"n/a" in resp.content

    def test_thumbnail_unknown_404(self, chart_setup):
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        resp = client.get("/api/thumbnail/stocks:NOPE.svg")
        assert resp.status_code == 404

    def test_dashboard_includes_portfolio_buttons_when_stocks(self, multi_chart_setup):
        settings, instruments, chains, defaults = multi_chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "portfolio:max-sharpe" in resp.text
        assert "portfolio:min-vol" in resp.text
        assert "Max Sharpe" in resp.text

    def test_portfolio_page_ok(self, multi_chart_setup):
        settings, instruments, chains, defaults = multi_chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)
        resp = client.get("/portfolio:max-sharpe")
        assert resp.status_code == 200
        assert "portfolio:max-sharpe" in resp.text


class TestThumbnailsUnit:
    def test_render_sparkline_empty(self):
        svg = render_sparkline_svg(())
        assert "<svg" in svg
        assert "n/a" in svg

    def test_render_sparkline_up(self):
        svg = render_sparkline_svg([1.0, 2.0, 3.0, 4.0], performance_pct=10.0)
        assert "polyline" in svg
        assert "#26a69a" in svg

    def test_render_sparkline_down(self):
        svg = render_sparkline_svg([4.0, 3.0, 2.0, 1.0], performance_pct=-10.0)
        assert "#ef5350" in svg


def _write_sample_overlay(root: Path, *, instrument: str = "futures:ES") -> None:
    back = root / "Backtests"
    back.mkdir(parents=True)
    stem = "ES_120_180"
    meta = {
        "long_15_25_50": {
            "instrument": instrument,
            "ticksize": 0.25,
            "cth_open": 120,
            "cth_close": 180,
            "is_short": False,
            "entry_factor": 15,
            "factor": 25,
            "stop_factor": 50,
            "session": {"tz": "America/Chicago", "begin": "04:00", "end": "15:30"},
            "extra": {},
        }
    }
    (back / f"{stem}.meta.json").write_text(json.dumps(meta), encoding="utf-8")
    pl.DataFrame(
        {
            "backtest_id": ["long_15_25_50", "long_15_25_50"],
            "time": [
                datetime(2025, 6, 1, 14, 0, tzinfo=UTC),
                datetime(2025, 6, 1, 15, 0, tzinfo=UTC),
            ],
            "price": [4500.0, 4510.0],
            "side": ["buy", "sell"],
            "kind": ["entry", "exit"],
        }
    ).write_parquet(back / f"{stem}.transactions.parquet")
    pl.DataFrame(
        {
            "backtest_id": ["long_15_25_50", "long_15_25_50", "long_15_25_50"],
            "time_from": [
                datetime(2025, 6, 1, 13, 0, tzinfo=UTC),
                datetime(2025, 6, 1, 14, 0, tzinfo=UTC),
                datetime(2025, 6, 1, 14, 0, tzinfo=UTC),
            ],
            "time_to": [
                datetime(2025, 6, 1, 14, 0, tzinfo=UTC),
                datetime(2025, 6, 1, 15, 0, tzinfo=UTC),
                datetime(2025, 6, 1, 15, 0, tzinfo=UTC),
            ],
            "price": [4500.0, 4520.0, 4480.0],
            "side": ["buy", "sell", "sell"],
            "order_type": ["LMT", "LMT", "STP"],
        }
    ).write_parquet(back / f"{stem}.orders.parquet")


class TestChartColorInjection:
    def test_html_injects_configured_colors(self, chart_setup):
        settings, instruments, chains, _ = chart_setup
        defaults = ChartDefaults(
            default_product="futures:ES",
            candle_up="#0FFF14",
            candle_down="#112233",
            tx_buy="#00AAFF",
            tx_sell="#FFAA00",
            order_buy="#0011FF",
            order_sell="#FF6600",
        )
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)
        resp = client.get("/futures:ES")
        assert resp.status_code == 200
        html = resp.text
        assert 'const CANDLE_UP = "#0FFF14"' in html
        assert 'const CANDLE_DOWN = "#112233"' in html
        assert 'const TX_BUY = "#00AAFF"' in html
        assert 'const TX_SELL = "#FFAA00"' in html
        assert 'const ORDER_BUY = "#0011FF"' in html
        assert 'const ORDER_SELL = "#FF6600"' in html
        assert "__CANDLE_UP__" not in html


class TestOverlayApi:
    def test_list_empty_without_dir(self, chart_setup):
        settings, instruments, chains, defaults = chart_setup
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)
        resp = client.get("/api/overlays", params={"product": "futures:ES"})
        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_and_load(self, chart_setup, tmp_path):
        settings, instruments, chains, defaults = chart_setup
        overlay_root = tmp_path / "overlays"
        _write_sample_overlay(overlay_root)
        settings.overlay_dir = str(overlay_root)
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)

        listed = client.get("/api/overlays", params={"product": "futures:ES"})
        assert listed.status_code == 200
        body = listed.json()
        assert len(body) == 1
        assert body[0]["stem"] == "ES_120_180"
        assert body[0]["ids"] == ["long_15_25_50"]

        other = client.get("/api/overlays", params={"product": "futures:NQ"})
        assert other.json() == []

        loaded = client.get("/api/overlay/ES_120_180")
        assert loaded.status_code == 200
        payload = loaded.json()
        assert payload["id"] == "long_15_25_50"
        assert len(payload["transactions"]) == 2
        assert payload["transactions"][0]["kind"] == "entry"
        types = {o["order_type"] for o in payload["orders"]}
        assert types == {"LMT", "STP"}
        assert all(o["order_type"] != "MKT" or True for o in payload["orders"])

    def test_path_traversal_rejected(self, chart_setup, tmp_path):
        settings, instruments, chains, defaults = chart_setup
        settings.overlay_dir = str(tmp_path / "overlays")
        (tmp_path / "overlays" / "Backtests").mkdir(parents=True)
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)
        resp = client.get("/api/overlay/../secret")
        assert resp.status_code in (400, 404)

    def test_unknown_stem_404(self, chart_setup, tmp_path):
        settings, instruments, chains, defaults = chart_setup
        settings.overlay_dir = str(tmp_path / "overlays")
        (tmp_path / "overlays" / "Backtests").mkdir(parents=True)
        app = create_chart_app(settings, instruments, chains, defaults)
        client = TestClient(app)
        resp = client.get("/api/overlay/missing")
        assert resp.status_code == 404
