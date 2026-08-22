"""Service lazy : optim + poids pour products chart ``portfolio:*``."""

from __future__ import annotations

from dataclasses import dataclass, field

from myquantstore.analytics.optimize import PortfolioResult, optimize
from myquantstore.analytics.panel import build_price_panel
from myquantstore.analytics.returns import compute_returns
from myquantstore.analytics.risk_free import resolve_risk_free_rate
from myquantstore.config import Settings
from myquantstore.instruments import Instrument, InstrumentType
from myquantstore.logging_setup import get_logger

logger = get_logger("analytics.portfolio_service")

PORTFOLIO_PRODUCTS: dict[str, str] = {
    "portfolio:max-sharpe": "max-sharpe",
    "portfolio:min-vol": "min-vol",
}


def is_portfolio_product(key: str) -> bool:
    return key in PORTFOLIO_PRODUCTS


def portfolio_objective(key: str) -> str:
    if key not in PORTFOLIO_PRODUCTS:
        raise KeyError(key)
    return PORTFOLIO_PRODUCTS[key]


@dataclass
class PortfolioService:
    """Cache mémoire process (lazy, pas de TTL) des résultats d'optim."""

    settings: Settings
    instruments: list[Instrument] = field(default_factory=list)
    _cache: dict[str, PortfolioResult] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.instruments:
            self.instruments = list(self.settings.instruments_of_type(InstrumentType.STOCKS))

    def get_result(self, product_key: str) -> PortfolioResult:
        obj = portfolio_objective(product_key)
        if product_key in self._cache:
            return self._cache[product_key]
        logger.info(f"Lazy optim portfolio {product_key} ({len(self.instruments)} stocks)…")
        panel = build_price_panel(
            self.instruments,
            self.settings,
            timescale="day",
            adjust_dividends=True,
        )
        rets = compute_returns(panel, self.settings, kind="simple")
        rf_quote = resolve_risk_free_rate(self.settings)
        result = optimize(
            rets,
            obj,
            risk_free_rate=rf_quote.rate,
            n_samples=self.settings.portfolio_frontier_samples,
            seed=self.settings.portfolio_optim_seed,
        )
        self._cache[product_key] = result
        logger.info(
            f"Optim {product_key}: Sharpe={result.sharpe:.2f} "
            f"vol={result.vol_ann:.2%} legs="
            f"{sum(1 for w in result.weights.values() if w > 1e-4)}"
        )
        return result

    def weights_for(self, product_key: str) -> dict[str, float]:
        return dict(self.get_result(product_key).weights)
