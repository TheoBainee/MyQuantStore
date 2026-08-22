"""Tests du module cli.py."""

from __future__ import annotations

from datetime import date

import polars as pl

from myquantstore.cli import _cmd_fetch, _render_df, _resolve_instruments, main
from myquantstore.instruments import InstrumentType


class TestResolveInstruments:
    """`--type` sans `--instrument` doit filtrer par type (pas tous les instruments)."""

    def test_no_arg_no_type_returns_all(self, tmp_settings):
        settings = tmp_settings.model_copy(
            update={"futures": ["ES"], "forex": ["EURUSD"], "stocks": ["AAPL"], "indices": ["NDX"]}
        )
        insts = _resolve_instruments(settings, None, None)
        keys = {i.key for i in insts}
        assert keys == {"futures:ES", "forex:EURUSD", "stocks:AAPL", "indices:NDX"}

    def test_type_only_filters_to_type(self, tmp_settings):
        settings = tmp_settings.model_copy(
            update={"futures": ["ES", "NQ"], "forex": ["EURUSD", "GBPUSD"], "stocks": ["AAPL"]}
        )
        insts = _resolve_instruments(settings, None, "forex")
        assert all(i.type == InstrumentType.FOREX for i in insts)
        assert {i.symbol for i in insts} == {"EURUSD", "GBPUSD"}

    def test_type_only_indices(self, tmp_settings):
        settings = tmp_settings.model_copy(
            update={"futures": ["ES"], "indices": ["SPX", "NDX"], "forex": ["EURUSD"]}
        )
        insts = _resolve_instruments(settings, None, "indices")
        assert [i.key for i in insts] == ["indices:SPX", "indices:NDX"]

    def test_instrument_with_type(self, tmp_settings):
        settings = tmp_settings.model_copy(update={"futures": ["ES"], "forex": ["EURUSD"]})
        insts = _resolve_instruments(settings, "EURUSD", "forex")
        assert len(insts) == 1
        assert insts[0].key == "forex:EURUSD"


class TestCliCommands:
    """Tests des commandes CLI."""

    def test_config_command(self, tmp_path, monkeypatch, capsys):
        """`myquantstore config` affiche la configuration."""
        env_file = tmp_path / ".env"
        env_file.write_text("MASSIVE_API_KEY=test_key_12345\n", encoding="utf-8")

        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["ES"]

[fetch]
timeframe = "1min"

[storage]
data_dir = "./data"
cache_dir = "./cache"
log_dir = "./logs"

[logging]
level = "DEBUG"
""",
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)

        result = main(["config"])

        assert result == 0
        captured = capsys.readouterr()
        assert "Configuration MyQuantStore" in captured.out
        assert "ES" in captured.out
        assert "Fichier :" in captured.out
        assert "config.toml" in captured.out

    def test_config_command_no_key(self, tmp_path, monkeypatch, capsys):
        """`myquantstore config` affiche NON CONFIGURÉE si pas de clé."""
        env_file = tmp_path / ".env"
        env_file.write_text("MASSIVE_API_KEY=\n", encoding="utf-8")

        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["ES"]

[logging]
level = "DEBUG"
""",
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)

        result = main(["config"])

        assert result == 0
        captured = capsys.readouterr()
        assert "NON CONFIGURÉE" in captured.out

    def test_no_command_prints_help(self, capsys):
        """`myquantstore` sans commande affiche l'aide."""
        result = main([])
        assert result == 0

    def test_status_command_empty(self, tmp_path, monkeypatch, capsys):
        """`myquantstore status` sur un environnement vide ne crash pas."""
        env_file = tmp_path / ".env"
        env_file.write_text("MASSIVE_API_KEY=test_key\n", encoding="utf-8")

        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["ES"]

[storage]
data_dir = "{}"
cache_dir = "{}"

[logging]
level = "INFO"
""".format(tmp_path / "data", tmp_path / "cache"),
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)

        result = main(["status", "--instrument", "ES"])

        assert result == 0
        captured = capsys.readouterr()
        assert "absent" in captured.out
        assert "Cache tickers" in captured.out

    def test_status_stale_and_check(self, tmp_path, monkeypatch, capsys):
        """status affiche STALE/lag et --check exit 1 si agrégé périmé."""
        from datetime import UTC, datetime

        from myquantstore.config import Settings
        from myquantstore.instruments import Instrument, InstrumentType
        from myquantstore.storage.aggregate_cache import write_aggregate

        data_dir = tmp_path / "data"
        cache_dir = tmp_path / "cache"
        (tmp_path / ".env").write_text("MASSIVE_API_KEY=test_key\n", encoding="utf-8")
        # seuil 0 → toute barre avec max < today est STALE (indépendant de la date du test)
        (tmp_path / "config.toml").write_text(
            f"""
[instruments]
stocks = ["AAPL"]

[storage]
data_dir = "{data_dir}"
cache_dir = "{cache_dir}"

[logging]
level = "INFO"

[health]
stale_lag_days_1min = 0
stale_lag_days_1day = 0
""",
            encoding="utf-8",
        )

        settings = Settings(
            api_key="test",
            stocks=["AAPL"],
            data_dir=str(data_dir),
            cache_dir=str(cache_dir),
            health_stale_lag_days_1min=0,
        )
        inst = Instrument(type=InstrumentType.STOCKS, symbol="AAPL")
        df = pl.DataFrame(
            {
                "window_start": [datetime(2026, 7, 10, tzinfo=UTC)],
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [100],
                "ticker": ["AAPL"],
            }
        ).with_columns(pl.col("window_start").cast(pl.Datetime("ns")))
        write_aggregate(df, inst, settings, resolution="1min")

        monkeypatch.chdir(tmp_path)
        result = main(["status", "--instrument", "AAPL", "--check"])
        assert result == 1
        out = capsys.readouterr().out
        assert "STALE" in out
        assert "lag=" in out

    def test_status_tickers_only(self, tmp_path, monkeypatch, capsys):
        """`myquantstore status --tickers` n'affiche que le cache tickers."""
        env_file = tmp_path / ".env"
        env_file.write_text("MASSIVE_API_KEY=test_key\n", encoding="utf-8")

        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["ES"]

[storage]
data_dir = "{}"
cache_dir = "{}"

[logging]
level = "INFO"
""".format(tmp_path / "data", tmp_path / "cache"),
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)

        result = main(["status", "--tickers"])
        assert result == 0
        out = capsys.readouterr().out
        assert "Cache tickers" in out
        assert "futures:ES" not in out

    def test_tickers_status_alias(self, tmp_path, monkeypatch, capsys):
        """`myquantstore tickers --status` alias de `status --tickers`."""
        env_file = tmp_path / ".env"
        env_file.write_text("MASSIVE_API_KEY=test_key\n", encoding="utf-8")

        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["ES"]

[storage]
data_dir = "{}"
cache_dir = "{}"

[logging]
level = "INFO"
""".format(tmp_path / "data", tmp_path / "cache"),
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)

        result = main(["tickers", "--status"])
        assert result == 0
        out = capsys.readouterr().out
        assert "Cache tickers" in out
        assert "futures:ES" not in out

    def test_setup_key_creates_env(self, tmp_path, monkeypatch):
        """`myquantstore setup-key` crée le fichier .env XDG."""
        from myquantstore.config import get_user_env_path

        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr("getpass.getpass", lambda prompt: "my_secret_key_123")

        result = main(["setup-key"])

        assert result == 0
        env_path = get_user_env_path()
        assert env_path.exists()
        content = env_path.read_text(encoding="utf-8")
        assert "MASSIVE_API_KEY=my_secret_key_123" in content
        assert "MASSIVE_BASE_URL=https://api.massive.com" in content

    def test_setup_key_empty_key_aborts(self, tmp_path, monkeypatch, capsys):
        """`myquantstore setup-key` avec clé vide → abandon."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr("getpass.getpass", lambda prompt: "")

        result = main(["setup-key"])

        assert result == 1

    def test_futures_contracts_no_key(self, tmp_path, monkeypatch, capsys):
        """`myquantstore futures contracts` sans clé → cache absent (lecture seule)."""
        env_file = tmp_path / ".env"
        env_file.write_text("MASSIVE_API_KEY=\n", encoding="utf-8")

        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["ES"]

[storage]
data_dir = "{}"
cache_dir = "{}"

[logging]
level = "INFO"
""".format(tmp_path / "data", tmp_path / "cache"),
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)

        result = main(["futures", "contracts", "--symbol", "ES"])
        assert result == 0

    def test_options_contracts_not_implemented(self, tmp_path, monkeypatch, capsys):
        """`myquantstore options contracts` → scaffold (exit 1, message non implémenté)."""
        env_file = tmp_path / ".env"
        env_file.write_text("MASSIVE_API_KEY=test\n", encoding="utf-8")

        config_toml = tmp_path / "config.toml"
        config_toml.write_text(
            """
[instruments]
futures = ["ES"]
options = []

[logging]
level = "INFO"
""",
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)

        result = main(["options", "contracts"])
        assert result == 1
        captured = capsys.readouterr()
        assert "Non implémenté" in captured.out


class TestRenderDf:
    """Tests du helper _render_df (tri + limites d'affichage)."""

    def test_render_df_sort_descending(self, tmp_settings, capsys):
        df = pl.DataFrame(
            {
                "rollover_date": [date(2024, 1, 1), date(2025, 6, 1), date(2024, 9, 1)],
                "ticker": ["A", "B", "C"],
            }
        )
        _render_df(df, tmp_settings, sort_col="rollover_date")
        out = capsys.readouterr().out
        idx_b = out.find("B")
        idx_a = out.find("A")
        assert idx_b < idx_a

    def test_render_df_limit_rows(self, tmp_settings, capsys):
        small_settings = tmp_settings.model_copy(update={"display_max_rows": 3})
        df = pl.DataFrame({"ticker": [f"T{i}" for i in range(10)]})
        _render_df(df, small_settings)
        out = capsys.readouterr().out
        assert "3 / 10 lignes" in out
        assert "…" in out  # ellipsis Polars ou message

    def test_render_df_limit_override(self, tmp_settings, capsys):
        """max_rows override display_max_rows."""
        small_settings = tmp_settings.model_copy(update={"display_max_rows": 2})
        df = pl.DataFrame({"ticker": [f"T{i}" for i in range(10)]})
        _render_df(df, small_settings, max_rows=5)
        out = capsys.readouterr().out
        assert "5 / 10 lignes" in out
        assert "2 / 10" not in out

    def test_render_df_limit_columns(self, tmp_settings, capsys):
        small_settings = tmp_settings.model_copy(update={"display_max_columns": 5})
        df = pl.DataFrame({f"col{i}": [1] for i in range(10)})
        _render_df(df, small_settings)
        out = capsys.readouterr().out
        assert "5 / 10 colonnes" in out

    def test_render_df_empty(self, tmp_settings, capsys):
        df = pl.DataFrame()
        _render_df(df, tmp_settings)
        out = capsys.readouterr().out
        assert "Aucune donnée" in out

    def test_render_df_missing_sort_col(self, tmp_settings, capsys):
        df = pl.DataFrame({"ticker": ["A", "B"]})
        _render_df(df, tmp_settings, sort_col="rollover_date")
        out = capsys.readouterr().out
        assert "A" in out and "B" in out


class TestFetchExitCode:
    def test_nonzero_on_error(self, tmp_settings, monkeypatch):
        from argparse import Namespace

        class _Dummy:
            def __enter__(self):
                return self

            def __exit__(self, *args: object) -> None:
                return None

        monkeypatch.setattr("myquantstore.api.client.MassiveClient", lambda *a, **k: _Dummy())
        monkeypatch.setattr(
            "myquantstore.pipeline.historian.run_fetch",
            lambda *a, **k: {"futures:ES[1day]": {"status": "error", "error": "boom"}},
        )
        args = Namespace(
            instrument="ES",
            type="futures",
            timeframe="1day",
            dry_run=False,
            force=False,
            no_cascade=True,
        )
        assert _cmd_fetch(tmp_settings, args) == 1

    def test_zero_on_ok(self, tmp_settings, monkeypatch):
        from argparse import Namespace

        class _Dummy:
            def __enter__(self):
                return self

            def __exit__(self, *args: object) -> None:
                return None

        monkeypatch.setattr("myquantstore.api.client.MassiveClient", lambda *a, **k: _Dummy())
        monkeypatch.setattr(
            "myquantstore.pipeline.historian.run_fetch",
            lambda *a, **k: {"futures:ES[1day]": {"status": "ok", "candles": 10}},
        )
        args = Namespace(
            instrument="ES",
            type="futures",
            timeframe="1day",
            dry_run=False,
            force=False,
            no_cascade=True,
        )
        assert _cmd_fetch(tmp_settings, args) == 0
