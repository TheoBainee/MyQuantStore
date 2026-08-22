"""Tests du cache tickers multi-shards."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import polars as pl
import pytest
import respx

from myquantstore.api.client import MassiveClient
from myquantstore.storage.parquet_io import write_parquet
from myquantstore.tickers.cache import (
    TickersCache,
    TickerTypesCache,
    parse_active_buckets,
    parse_csv_list,
    parse_markets_arg,
)


def _sample_df(tickers: list[str], market: str = "stocks") -> pl.DataFrame:
    n = len(tickers)
    return pl.DataFrame(
        {
            "ticker": tickers,
            "name": [f"Name {t}" for t in tickers],
            "market": [market] * n,
            "locale": ["us"] * n,
            "type": ["CS"] * n,
            "active": [True] * n,
            "primary_exchange": ["XNAS"] * n,
            "currency_name": ["usd"] * n,
            "currency_symbol": [None] * n,
            "base_currency_name": [None] * n,
            "base_currency_symbol": [None] * n,
            "cik": [None] * n,
            "composite_figi": [None] * n,
            "share_class_figi": [None] * n,
            "last_updated_utc": [None] * n,
            "delisted_utc": [None] * n,
        }
    )


def test_parse_csv_list():
    assert parse_csv_list(None) == []
    assert parse_csv_list("stocks,fx") == ["stocks", "fx"]
    assert parse_csv_list("stocks, fx ,indices") == ["stocks", "fx", "indices"]
    assert parse_csv_list(["AAPL", "MSFT,TSLA"]) == ["AAPL", "MSFT", "TSLA"]
    # espaces seuls ≠ séparateur
    assert parse_csv_list("stocks fx") == ["stocks fx"]
    assert parse_csv_list("A,B,A", lower=True) == ["a", "b"]


def test_parse_markets_arg():
    assert parse_markets_arg(None) == ["stocks"]
    assert parse_markets_arg("stocks,fx") == ["stocks", "fx"]
    assert parse_markets_arg(["stocks", "fx"]) == ["stocks", "fx"]
    assert parse_markets_arg("all") == ["stocks", "fx", "indices", "otc", "crypto"]
    with pytest.raises(ValueError, match="inconnu"):
        parse_markets_arg("usd")
    with pytest.raises(ValueError, match="inconnu"):
        parse_markets_arg("fx,usd")


def test_parse_active_buckets():
    assert parse_active_buckets("true") == [True]
    assert parse_active_buckets("false") == [False]
    assert parse_active_buckets("all") == [True, False]


def test_shard_paths(tmp_settings):
    cache = TickersCache(tmp_settings)
    p = cache.shard_path("stocks", True)
    assert p.name == "active.parquet"
    assert p.parent.name == "stocks"


def test_write_and_read_concat_shards(tmp_settings):
    cache = TickersCache(tmp_settings)
    now = datetime.now(UTC).isoformat()
    write_parquet(
        _sample_df(["AAPL", "MSFT"], "stocks"),
        cache.shard_path("stocks", True),
        last_fetched_at=now,
        row_count=2,
    )
    write_parquet(
        _sample_df(["EURUSD"], "fx"),
        cache.shard_path("fx", True),
        last_fetched_at=now,
        row_count=1,
    )
    df = cache.read_concat(markets=["stocks", "fx"], active=True)
    assert df.height == 3
    assert set(df["ticker"].to_list()) == {"AAPL", "MSFT", "EURUSD"}


def test_inventory_lists_present_shards(tmp_settings):
    cache = TickersCache(tmp_settings)
    now = datetime.now(UTC).isoformat()
    write_parquet(
        _sample_df(["AAPL"], "stocks"),
        cache.shard_path("stocks", True),
        last_fetched_at=now,
        row_count=1,
    )
    write_parquet(
        _sample_df(["EURUSD"], "fx"),
        cache.shard_path("fx", True),
        last_fetched_at=now,
        row_count=1,
    )

    inv = cache.inventory()
    assert {(s.market, s.bucket) for s in inv} == {("stocks", "active"), ("fx", "active")}
    assert all(s.exists and s.fresh for s in inv)

    inv_missing = cache.inventory(include_missing=True)
    markets = {s.market for s in inv_missing}
    assert "stocks" in markets and "crypto" in markets
    absent = [s for s in inv_missing if not s.exists]
    assert any(s.market == "crypto" for s in absent)


def test_shard_fresh_skip(tmp_settings):
    cache = TickersCache(tmp_settings)
    write_parquet(
        _sample_df(["AAPL"]),
        cache.shard_path("stocks", True),
        last_fetched_at=datetime.now(UTC).isoformat(),
    )
    assert cache.is_shard_fresh("stocks", True)
    df = cache.refresh_shard(
        client=None,  # type: ignore[arg-type]
        market="stocks",
        active=True,
        force=False,
    )
    assert df.height == 1


@respx.mock
def test_refresh_shard_fetches(tmp_settings):
    respx.get(url__regex=r".*/v3/reference/tickers.*").mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [
                    {
                        "ticker": "AAPL",
                        "name": "Apple",
                        "market": "stocks",
                        "type": "CS",
                        "active": True,
                    }
                ],
                "status": "OK",
            },
        )
    )
    cache = TickersCache(tmp_settings)
    with MassiveClient(tmp_settings) as client:
        df = cache.refresh_shard(client, "stocks", True, force=True)
    assert df.height == 1
    assert cache.shard_path("stocks", True).exists()


def test_stale_shard(tmp_settings):
    cache = TickersCache(tmp_settings)
    old = (datetime.now(UTC) - timedelta(days=60)).isoformat()
    write_parquet(_sample_df(["AAPL"]), cache.shard_path("stocks", True), last_fetched_at=old)
    assert not cache.is_shard_fresh("stocks", True)


@respx.mock
def test_ticker_types_cache(tmp_settings):
    respx.get("https://api.test.massive.com/v3/reference/tickers/types").mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [
                    {
                        "code": "CS",
                        "description": "Common Stock",
                        "asset_class": "stocks",
                        "locale": "us",
                    }
                ],
                "status": "OK",
            },
        )
    )
    cache = TickerTypesCache(tmp_settings)
    with MassiveClient(tmp_settings) as client:
        df = cache.get(client, force_refresh=True)
    assert df.height == 1
    assert cache.exists
