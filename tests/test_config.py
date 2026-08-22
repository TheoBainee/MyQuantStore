"""Tests du module config.py."""

from __future__ import annotations

from pathlib import Path

import pytest

from myquantstore.config import Settings, generate_run_ts, load_settings, normalize_hex_color
from myquantstore.instruments import Instrument, InstrumentType


class TestSettings:
    """Tests de la classe Settings et de load_settings."""

    def test_load_settings_from_config_toml(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """load_settings charge config.toml correctement (structure multi-type)."""
        # Éviter le .env du dépôt (cwd) : isoler le cwd + écrire le .env local
        monkeypatch.chdir(tmp_path)
        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["NQ", "ES"]
forex = []
stocks = ["AAPL"]
indices = []
options = []

[futures]
days_before_expiry = 7
contracts_page_limit = 1000
contracts_snapshot_interval_months = 1

[stocks]
splits_page_limit = 5000
dividends_page_limit = 5000

[instrument_cache]
ttl_days = 30

[fetch]
timeframe = "1min"
overlap_buffer_days = 1
history_months = 24
        requests_per_minute = 6
        page_limit = 50000
        max_retries = 6

        [storage]
        data_dir = "./test_data"
        cache_dir = "./test_cache"
        log_dir = "./test_logs"

        [display]
        max_rows = 10
        max_columns = 20

        [chart]
        port = 8050
        host = "127.0.0.1"

[serve]
port = 9001
host = "0.0.0.0"

[tests]
data_quality_trigger = 0.1

[logging]
level = "INFO"
""",
            encoding="utf-8",
        )

        env_file = tmp_path / ".env"
        env_file.write_text("MASSIVE_API_KEY=test_key_12345\n", encoding="utf-8")

        settings = load_settings(config_path=config_toml)

        assert settings.api_key == "test_key_12345"
        assert settings.futures == ["NQ", "ES"]
        assert settings.stocks == ["AAPL"]
        assert settings.forex == []
        assert settings.timeframe == "1min"
        assert settings.overlap_buffer_days == 1
        # Compat int legacy: s'applique à tous les types
        assert settings.history_months_for("futures") == 24
        assert settings.history_months_for("indices") == 24
        assert settings.requests_per_minute == 6
        assert settings.contracts_page_limit == 1000
        assert settings.days_before_expiry == 7
        assert settings.instrument_cache_ttl_days == 30
        assert settings.splits_page_limit == 5000
        assert settings.data_dir == "./test_data"
        assert settings.cache_dir == "./test_cache"
        assert settings.log_dir == "./test_logs"
        assert settings.data_quality_trigger == 0.1
        assert settings.log_level == "INFO"
        assert settings.display_max_rows == 10
        assert settings.chart_port == 8050
        assert settings.chart_host == "127.0.0.1"
        assert settings.serve_port == 9001
        assert settings.serve_host == "0.0.0.0"

    def test_settings_defaults(self):
        """Les valeurs par défaut de Settings sont correctes (alignées config.toml.example)."""
        settings = Settings(api_key="test")
        assert settings.futures == ["NQ", "ES", "RTY", "YM"]
        assert settings.forex == []
        assert settings.stocks == []
        assert settings.overlap_buffer_days == 1
        assert settings.history_months == {
            "futures": 24,
            "forex": 24,
            "stocks": 24,
            "indices": 60,
            "options": 24,
        }
        assert settings.history_months_for(InstrumentType.INDICES) == 60
        assert settings.history_months_for(InstrumentType.FUTURES) == 24
        assert settings.requests_per_minute == 6
        assert settings.contracts_page_limit == 1000
        assert settings.contracts_snapshot_interval_months == 1
        assert settings.max_retries == 6
        assert settings.instrument_cache_ttl_days == 30
        assert settings.days_before_expiry == 7
        assert settings.data_quality_trigger == 0.1
        assert settings.log_level == "DEBUG"
        assert settings.display_max_rows == 10
        assert settings.display_max_columns == 20
        assert settings.default_timescale_nb == 5
        assert settings.default_nb_candle == 2000
        assert settings.max_visible_candles == 100000
        assert settings.chart_port == 8050
        assert settings.chart_host == "127.0.0.1"
        assert settings.chart_candle_up == "#26A69A"
        assert settings.chart_candle_down == "#EF5350"
        assert settings.chart_tx_buy == "#2196F3"
        assert settings.chart_tx_sell == "#FF9800"
        assert settings.chart_order_buy == "#2196F3"
        assert settings.chart_order_sell == "#FF9800"
        assert settings.serve_port == 8741
        assert settings.serve_host == "127.0.0.1"
        assert settings.data_dir == "~/.local/share/myquantstore/data"
        assert settings.cache_dir == "~/.local/share/myquantstore/cache"
        assert settings.log_dir == "~/.local/share/myquantstore/logs"

    def test_normalize_hex_color(self):
        assert normalize_hex_color("26a69a") == "#26A69A"
        assert normalize_hex_color("#ff9800") == "#FF9800"
        assert normalize_hex_color("f80") == "#FF8800"
        with pytest.raises(ValueError, match="hex"):
            normalize_hex_color("not-a-color")

    def test_chart_colors_and_overlay_nested(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.chdir(tmp_path)
        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["ES"]
forex = []
stocks = []
indices = []
options = []

[chart]
candle_up = "0FFF14"
candle_down = "#112233"

[chart.overlay]
overlay_dir = "~/overlays_nested"

[chart.overlay.backtest]
transaction_buy = "00ff00"
transaction_sell = "ff0000"
order_buy = "0000ff"
order_sell = "ffa500"
""",
            encoding="utf-8",
        )
        (tmp_path / ".env").write_text("MASSIVE_API_KEY=test_key\n", encoding="utf-8")
        settings = load_settings(config_path=config_toml)
        assert settings.chart_candle_up == "#0FFF14"
        assert settings.chart_candle_down == "#112233"
        assert settings.overlay_dir.endswith("overlays_nested")
        assert settings.chart_tx_buy == "#00FF00"
        assert settings.chart_tx_sell == "#FF0000"
        assert settings.chart_order_buy == "#0000FF"
        assert settings.chart_order_sell == "#FFA500"

    def test_overlay_dir_flat_backward_compat(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.chdir(tmp_path)
        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["ES"]
forex = []
stocks = []
indices = []
options = []

[chart]
overlay_dir = "/tmp/flat_overlays"
""",
            encoding="utf-8",
        )
        (tmp_path / ".env").write_text("MASSIVE_API_KEY=test_key\n", encoding="utf-8")
        settings = load_settings(config_path=config_toml)
        assert settings.overlay_dir == "/tmp/flat_overlays"

    def test_chart_intraday_from_toml(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        from datetime import time as time_cls

        monkeypatch.chdir(tmp_path)
        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["ES"]
forex = []
stocks = []
indices = []
options = []

[chart]
timezone = "America/Chicago"
intraday_begin = "09:30"
intraday_end = "16:00"
""",
            encoding="utf-8",
        )
        (tmp_path / ".env").write_text("MASSIVE_API_KEY=test_key\n", encoding="utf-8")
        settings = load_settings(config_path=config_toml)
        assert settings.chart_intraday_begin == time_cls(9, 30)
        assert settings.chart_intraday_end == time_cls(16, 0)
        assert settings.chart_timezone == "America/Chicago"

    def test_chart_timezone_invalid(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.chdir(tmp_path)
        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["ES"]
forex = []
stocks = []
indices = []
options = []

[chart]
timezone = "Not/AZone"
""",
            encoding="utf-8",
        )
        (tmp_path / ".env").write_text("MASSIVE_API_KEY=test_key\n", encoding="utf-8")
        with pytest.raises(Exception, match="timezone"):
            load_settings(config_path=config_toml)

    def test_chart_intraday_pair_required(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.chdir(tmp_path)
        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["ES"]
forex = []
stocks = []
indices = []
options = []

[chart]
intraday_begin = "09:30"
""",
            encoding="utf-8",
        )
        (tmp_path / ".env").write_text("MASSIVE_API_KEY=test_key\n", encoding="utf-8")
        with pytest.raises(Exception, match="intraday"):
            load_settings(config_path=config_toml)

    def test_validation_all_instruments_empty_raises(self):
        """Tous les types vides → erreur."""
        with pytest.raises(Exception, match="Aucun instrument"):
            Settings(api_key="test", futures=[], forex=[], stocks=[], indices=[], options=[])

    def test_validation_overlap_buffer_non_neg(self):
        with pytest.raises(Exception, match="overlap_buffer_days"):
            Settings(api_key="test", overlap_buffer_days=-1)
        with pytest.raises(Exception, match="overlap_buffer_days"):
            Settings(api_key="test", yahoo_overlap_buffer_days=-1)

    def test_validation_days_before_expiry_non_neg(self):
        with pytest.raises(Exception, match="days_before_expiry"):
            Settings(api_key="test", days_before_expiry=-1)

    def test_validation_history_months_ge_1(self):
        with pytest.raises(Exception, match="history_months"):
            Settings(api_key="test", history_months=0)
        with pytest.raises(Exception, match="history_months"):
            Settings(api_key="test", history_months={"indices": 0})

    def test_history_months_per_type_from_toml(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        """[fetch.history_months] charge les valeurs par type (indices=60 par défaut)."""
        monkeypatch.chdir(tmp_path)
        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["ES"]
forex = []
stocks = []
indices = ["SPX"]
options = []

[fetch.history_months]
futures = 36
indices = 60
""",
            encoding="utf-8",
        )
        settings = load_settings(config_path=config_toml)
        assert settings.history_months_for("futures") == 36
        assert settings.history_months_for("indices") == 60
        assert settings.history_months_for("stocks") == 24  # défaut non surchargé

    def test_validation_requests_per_minute_non_neg(self):
        with pytest.raises(Exception, match="requests_per_minute"):
            Settings(api_key="test", requests_per_minute=-1)

    def test_validation_max_retries_ge_1(self):
        with pytest.raises(Exception, match="max_retries"):
            Settings(api_key="test", max_retries=0)

    def test_validation_page_limit_range(self):
        with pytest.raises(Exception, match="page_limit"):
            Settings(api_key="test", page_limit=0)
        with pytest.raises(Exception, match="page_limit"):
            Settings(api_key="test", page_limit=50001)

    def test_validation_contracts_page_limit_range(self):
        with pytest.raises(Exception, match="contracts_page_limit"):
            Settings(api_key="test", contracts_page_limit=0)

    def test_validation_data_quality_trigger_positive(self):
        with pytest.raises(Exception, match="data_quality_trigger"):
            Settings(api_key="test", data_quality_trigger=0)

    def test_helpers_chemins(self, tmp_settings, es_instrument):
        """Les helpers de chemins retournent les bons paths (multi-type)."""
        assert tmp_settings.raw_dumps_dir().name == "raw"
        assert tmp_settings.aggregate_dir().name == "aggregate"
        # aggregate_path(instrument) -> aggregate/{type}/{symbol}/{resolution}.parquet
        agg_path = tmp_settings.aggregate_path(es_instrument)
        assert agg_path.name == "1min.parquet"
        assert agg_path.parent.name == "ES"
        assert agg_path.parent.parent.name == "futures"
        agg_day = tmp_settings.aggregate_path(es_instrument, resolution="1day")
        assert agg_day.name == "1day.parquet"
        # contracts_cache_path (futures, inchangé)
        assert tmp_settings.contracts_cache_path("ES").name == "ES.parquet"
        assert tmp_settings.contracts_meta_path("ES").name == "ES.meta.json"
        # raw_dump_path(instrument, ticker, run_ts) -> …/{ticker}/{resolution}/{run}.parquet
        dump_path = tmp_settings.raw_dump_path(es_instrument, "ESM5", "20260711T183000")
        assert "futures" in str(dump_path)
        assert "ES" in str(dump_path)
        assert "ESM5" in str(dump_path)
        assert "1min" in dump_path.parts
        assert "20260711T183000.parquet" in str(dump_path)
        # corporate_actions_path (stocks)
        ca_path = tmp_settings.corporate_actions_path("AAPL", "splits")
        assert "corporate_actions" in str(ca_path)
        assert ca_path.name == "splits.parquet"

    def test_expanduser_on_storage_paths(self, tmp_path: Path):
        """load_settings expand ~ dans data_dir/cache_dir/log_dir."""
        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["ES"]
forex = []
stocks = []
indices = []
options = []

[storage]
data_dir = "~/myquantstore_test_data"
cache_dir = "~/myquantstore_test_cache"
log_dir = "~/myquantstore_test_logs"
""",
            encoding="utf-8",
        )
        settings = load_settings(config_path=config_toml)
        assert not settings.data_dir.startswith("~")
        assert settings.data_dir.endswith("myquantstore_test_data")
        assert Path(settings.data_dir).is_absolute()
        assert settings.raw_dumps_dir().name == "raw"

    def test_resolve_instrument(self, tmp_settings):
        """resolve_instrument résout un symbole depuis la config."""
        inst = tmp_settings.resolve_instrument("ES")
        assert inst.type == InstrumentType.FUTURES
        assert inst.symbol == "ES"
        # Symbole non configuré
        with pytest.raises(ValueError, match="non trouvé"):
            tmp_settings.resolve_instrument("UNKNOWN")
        # all_instruments
        all_inst = tmp_settings.all_instruments()
        assert len(all_inst) == 2  # ES, NQ

    def test_instrument_key_and_api_ticker(self, es_instrument):
        """Instrument.key et api_ticker sont corrects."""
        assert es_instrument.key == "futures:ES"
        assert es_instrument.api_ticker == "ES"  # futures: pas de préfixe
        aapl = Instrument(type=InstrumentType.STOCKS, symbol="AAPL")
        assert aapl.api_ticker == "AAPL"
        forex = Instrument(type=InstrumentType.FOREX, symbol="EURUSD")
        assert forex.api_ticker == "C:EURUSD"
        idx = Instrument(type=InstrumentType.INDICES, symbol="NDX")
        assert idx.api_ticker == "I:NDX"


class TestGenerateRunTs:
    """Tests de generate_run_ts."""

    def test_format(self):
        run_ts = generate_run_ts()
        assert len(run_ts) == 15
        assert run_ts[8] == "T"
        assert run_ts[:8].isdigit()
        assert run_ts[9:].isdigit()

    def test_uniqueness(self):
        import time

        ts1 = generate_run_ts()
        time.sleep(1.1)
        ts2 = generate_run_ts()
        assert ts1 != ts2
