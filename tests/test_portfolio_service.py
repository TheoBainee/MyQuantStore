"""Tests PortfolioService — cache mémoire TTL [chart] pf_optim_cache_ttl_days."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from myquantstore.analytics.optimize import PortfolioResult
from myquantstore.analytics.portfolio_service import PortfolioService, _CacheEntry
from myquantstore.instruments import Instrument, InstrumentType


def _fake_result(obj: str = "max-sharpe") -> PortfolioResult:
    return PortfolioResult(
        weights={"AAA": 0.6, "BBB": 0.4},
        mean_ann=0.12,
        vol_ann=0.18,
        sharpe=0.55,
        objective=obj,
        symbols=["AAA", "BBB"],
    )


@pytest.fixture
def stock_insts() -> list[Instrument]:
    return [
        Instrument(type=InstrumentType.STOCKS, symbol="AAA"),
        Instrument(type=InstrumentType.STOCKS, symbol="BBB"),
    ]


def _svc(tmp_settings, stock_insts, ttl_days: int) -> PortfolioService:
    tmp_settings.chart_pf_optim_cache_ttl_days = ttl_days
    tmp_settings.portfolio_rf_source = "static"
    tmp_settings.portfolio_risk_free_rate = 0.04
    return PortfolioService(tmp_settings, stock_insts)


@patch("myquantstore.analytics.portfolio_service.optimize")
@patch("myquantstore.analytics.portfolio_service.resolve_risk_free_rate")
@patch("myquantstore.analytics.portfolio_service.compute_returns")
@patch("myquantstore.analytics.portfolio_service.build_price_panel")
def test_cache_hit_within_ttl(
    mock_panel,
    mock_rets,
    mock_rf,
    mock_opt,
    tmp_settings,
    stock_insts,
):
    mock_rf.return_value = MagicMock(rate=0.04)
    mock_opt.return_value = _fake_result()
    svc = _svc(tmp_settings, stock_insts, ttl_days=1)

    r1 = svc.get_result("portfolio:max-sharpe")
    r2 = svc.get_result("portfolio:max-sharpe")

    assert r1 is r2
    assert mock_opt.call_count == 1
    assert mock_panel.call_count == 1


@patch("myquantstore.analytics.portfolio_service.optimize")
@patch("myquantstore.analytics.portfolio_service.resolve_risk_free_rate")
@patch("myquantstore.analytics.portfolio_service.compute_returns")
@patch("myquantstore.analytics.portfolio_service.build_price_panel")
def test_cache_miss_after_ttl_expired(
    mock_panel,
    mock_rets,
    mock_rf,
    mock_opt,
    tmp_settings,
    stock_insts,
):
    mock_rf.return_value = MagicMock(rate=0.04)
    mock_opt.side_effect = [
        _fake_result(),
        PortfolioResult(
            weights={"AAA": 1.0},
            mean_ann=0.1,
            vol_ann=0.2,
            sharpe=0.3,
            objective="max-sharpe",
            symbols=["AAA", "BBB"],
        ),
    ]
    svc = _svc(tmp_settings, stock_insts, ttl_days=1)

    r1 = svc.get_result("portfolio:max-sharpe")
    # Force expiration
    entry = svc._cache["portfolio:max-sharpe"]
    svc._cache["portfolio:max-sharpe"] = _CacheEntry(
        result=entry.result,
        computed_at=datetime.now(UTC) - timedelta(days=2),
    )
    r2 = svc.get_result("portfolio:max-sharpe")

    assert mock_opt.call_count == 2
    assert r2.weights["AAA"] == 1.0
    assert r1 is not r2


@patch("myquantstore.analytics.portfolio_service.optimize")
@patch("myquantstore.analytics.portfolio_service.resolve_risk_free_rate")
@patch("myquantstore.analytics.portfolio_service.compute_returns")
@patch("myquantstore.analytics.portfolio_service.build_price_panel")
def test_ttl_zero_disables_cache(
    mock_panel,
    mock_rets,
    mock_rf,
    mock_opt,
    tmp_settings,
    stock_insts,
):
    mock_rf.return_value = MagicMock(rate=0.04)
    mock_opt.side_effect = [_fake_result(), _fake_result()]
    svc = _svc(tmp_settings, stock_insts, ttl_days=0)

    svc.get_result("portfolio:max-sharpe")
    svc.get_result("portfolio:max-sharpe")

    assert mock_opt.call_count == 2


@patch("myquantstore.analytics.portfolio_service.optimize")
@patch("myquantstore.analytics.portfolio_service.resolve_risk_free_rate")
@patch("myquantstore.analytics.portfolio_service.compute_returns")
@patch("myquantstore.analytics.portfolio_service.build_price_panel")
def test_products_cached_independently(
    mock_panel,
    mock_rets,
    mock_rf,
    mock_opt,
    tmp_settings,
    stock_insts,
):
    mock_rf.return_value = MagicMock(rate=0.04)
    mock_opt.side_effect = [
        _fake_result("max-sharpe"),
        _fake_result("min-vol"),
    ]
    svc = _svc(tmp_settings, stock_insts, ttl_days=1)

    svc.get_result("portfolio:max-sharpe")
    svc.get_result("portfolio:min-vol")
    svc.get_result("portfolio:max-sharpe")  # hit

    assert mock_opt.call_count == 2
    assert "portfolio:max-sharpe" in svc._cache
    assert "portfolio:min-vol" in svc._cache


def test_settings_default_and_toml_mapping(tmp_path, monkeypatch):
    from myquantstore.config import Settings, load_settings

    s = Settings(
        stocks=["AAPL"],
        data_dir=str(tmp_path / "data"),
        cache_dir=str(tmp_path / "cache"),
        log_dir=str(tmp_path / "logs"),
    )
    assert s.chart_pf_optim_cache_ttl_days == 1

    cfg = tmp_path / "config.toml"
    cfg.write_text(
        """
[instruments]
stocks = ["AAPL"]

[chart]
pf_optim_cache_ttl_days = 3

[storage]
data_dir = "{data}"
cache_dir = "{cache}"
log_dir = "{logs}"
""".format(
            data=tmp_path / "data",
            cache=tmp_path / "cache",
            logs=tmp_path / "logs",
        )
    )
    loaded = load_settings(cfg)
    assert loaded.chart_pf_optim_cache_ttl_days == 3
