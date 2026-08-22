"""Tests CLI tickers / search / config add (shards)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from myquantstore.cli import main
from myquantstore.storage.parquet_io import write_parquet


def _seed_shard(cache_dir: Path, market: str = "stocks", active: bool = True) -> None:
    bucket = "active" if active else "inactive"
    path = cache_dir / "tickers" / market / f"{bucket}.parquet"
    df = pl.DataFrame(
        {
            "ticker": ["AAPL", "MSFT", "TSLA"],
            "name": ["Apple Inc", "Microsoft", "Tesla Inc"],
            "market": [market, market, market],
            "locale": ["us", "us", "us"],
            "type": ["CS", "CS", "CS"],
            "active": [active, active, active],
            "primary_exchange": ["XNAS", "XNAS", "XNAS"],
            "currency_name": ["usd", "usd", "usd"],
            "currency_symbol": [None, None, None],
            "base_currency_name": [None, None, None],
            "base_currency_symbol": [None, None, None],
            "cik": [None, None, None],
            "composite_figi": [None, None, None],
            "share_class_figi": [None, None, None],
            "last_updated_utc": [None, None, None],
            "delisted_utc": [None, None, None],
        }
    )
    write_parquet(
        df,
        path,
        last_fetched_at=datetime.now(UTC).isoformat(),
        source_url="/v3/reference/tickers",
    )


def _write_env_and_config(tmp_path: Path, stocks: list[str] | None = None) -> None:
    stocks = stocks or ["AAPL"]
    stocks_toml = ", ".join(f'"{s}"' for s in stocks)
    (tmp_path / ".env").write_text("MASSIVE_API_KEY=test_key\n", encoding="utf-8")
    (tmp_path / "config.toml").write_text(
        f"""
[instruments]
futures = ["ES"]
forex = []
stocks = [{stocks_toml}]
indices = []
options = []

[storage]
data_dir = "{tmp_path / "data"}"
cache_dir = "{tmp_path / "cache"}"
log_dir = "{tmp_path / "logs"}"

[display]
max_rows = 2
max_columns = 20

[logging]
level = "WARNING"
""",
        encoding="utf-8",
    )


def test_search_local(tmp_path, monkeypatch, capsys):
    _write_env_and_config(tmp_path)
    _seed_shard(tmp_path / "cache")
    monkeypatch.chdir(tmp_path)

    result = main(["search", "apple", "--no-cascade"])
    assert result == 0
    out = capsys.readouterr().out
    assert "AAPL" in out


def test_search_limit_overrides_display(tmp_path, monkeypatch, capsys):
    """--limit = plafond d'affichage (pas de coupe data) ; total + message de troncature."""
    _write_env_and_config(tmp_path)
    _seed_shard(tmp_path / "cache")
    monkeypatch.chdir(tmp_path)

    # 3 stocks dans le shard ; --limit 1 → total 3, affiche 1 + message
    result = main(["search", "--markets", "stocks", "--limit", "1", "--no-cascade"])
    assert result == 0
    out = capsys.readouterr().out
    assert "3 résultat" in out
    assert "affichage limité à 1 / 3" in out
    # ne doit pas se comporter comme display_max_rows=2
    assert "limité à 2" not in out


def test_search_add_single(tmp_path, monkeypatch, capsys):
    _write_env_and_config(tmp_path, stocks=["AAPL"])
    _seed_shard(tmp_path / "cache")
    monkeypatch.chdir(tmp_path)

    result = main(["search", "--ticker", "MSFT", "--add", "--no-cascade"])
    assert result == 0
    text = (tmp_path / "config.toml").read_text(encoding="utf-8")
    assert "MSFT" in text


def test_search_add_multi_requires_yes(tmp_path, monkeypatch, capsys):
    _write_env_and_config(tmp_path, stocks=[])
    _seed_shard(tmp_path / "cache")
    monkeypatch.chdir(tmp_path)

    result = main(["search", "--markets", "stocks", "--add", "--no-cascade"])
    assert result == 1
    out = capsys.readouterr().out
    assert "yes" in out.lower() or "Affinez" in out


def test_config_add(tmp_path, monkeypatch, capsys):
    _write_env_and_config(tmp_path, stocks=["AAPL"])
    _seed_shard(tmp_path / "cache")
    monkeypatch.chdir(tmp_path)

    result = main(["config", "add", "TSLA", "--no-cascade"])
    assert result == 0
    text = (tmp_path / "config.toml").read_text(encoding="utf-8")
    assert "TSLA" in text


def test_config_add_with_type(tmp_path, monkeypatch, capsys):
    _write_env_and_config(tmp_path, stocks=[])
    monkeypatch.chdir(tmp_path)

    result = main(["config", "add", "NVDA", "--type", "stocks", "--no-cascade"])
    assert result == 0
    text = (tmp_path / "config.toml").read_text(encoding="utf-8")
    assert "NVDA" in text


def test_search_markets_does_not_swallow_query(tmp_path, monkeypatch, capsys):
    """--markets prend un seul token CSV ; le positionnel reste la query."""
    _write_env_and_config(tmp_path)
    # ticker contenant USD dans le name pour matcher la query
    cache = tmp_path / "cache" / "tickers" / "fx" / "active.parquet"
    df = pl.DataFrame(
        {
            "ticker": ["C:EURUSD", "C:GBPUSD"],
            "name": ["Euro / US Dollar", "British Pound / US Dollar"],
            "market": ["fx", "fx"],
            "locale": ["global", "global"],
            "type": ["FX", "FX"],
            "active": [True, True],
            "primary_exchange": [None, None],
            "currency_name": ["usd", "usd"],
            "currency_symbol": [None, None],
            "base_currency_name": [None, None],
            "base_currency_symbol": [None, None],
            "cik": [None, None],
            "composite_figi": [None, None],
            "share_class_figi": [None, None],
            "last_updated_utc": [None, None],
            "delisted_utc": [None, None],
        }
    )
    write_parquet(df, cache, last_fetched_at=datetime.now(UTC).isoformat())
    monkeypatch.chdir(tmp_path)

    # Ancien piège nargs=+ : USD aurait été un market fantôme
    result = main(["search", "--markets", "fx", "USD", "--no-cascade"])
    assert result == 0
    out = capsys.readouterr().out
    assert "EURUSD" in out or "GBPUSD" in out
    assert "Cache tickers — ensure" not in out  # pas de cascade API


def test_search_unknown_market_errors(tmp_path, monkeypatch, capsys):
    _write_env_and_config(tmp_path)
    _seed_shard(tmp_path / "cache")
    monkeypatch.chdir(tmp_path)

    result = main(["search", "--markets", "usd", "--no-cascade"])
    assert result == 1
    out = capsys.readouterr().out
    assert "inconnu" in out.lower() or "Market" in out


def test_search_markets_csv(tmp_path, monkeypatch, capsys):
    _write_env_and_config(tmp_path)
    _seed_shard(tmp_path / "cache", market="stocks")
    monkeypatch.chdir(tmp_path)

    result = main(["search", "--markets", "stocks,fx", "--no-cascade"])
    assert result == 0
    out = capsys.readouterr().out
    assert "AAPL" in out


def test_config_add_csv_token(tmp_path, monkeypatch, capsys):
    _write_env_and_config(tmp_path, stocks=[])
    monkeypatch.chdir(tmp_path)

    result = main(["config", "add", "AAPL,MSFT", "--type", "stocks", "--no-cascade"])
    assert result == 0
    text = (tmp_path / "config.toml").read_text(encoding="utf-8")
    assert "AAPL" in text
    assert "MSFT" in text
