"""Service lazy : optim + poids pour products chart ``portfolio:*``."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

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


@dataclass(frozen=True, slots=True)
class _CacheEntry:
    """Entrée cache mémoire : résultat d'optim + horodatage UTC du calcul."""

    result: PortfolioResult
    computed_at: datetime


@dataclass
class PortfolioService:
    """Cache mémoire process (lazy, TTL ``[chart] pf_optim_cache_ttl_days``).

    Les produits chart ``portfolio:max-sharpe`` / ``portfolio:min-vol``
    déclenchent l'optim au premier accès. Entrée valide tant que
    ``now - computed_at < TTL`` (TTL 0 = pas de cache, recalcul systématique).
    """

    settings: Settings
    instruments: list[Instrument] = field(default_factory=list)
    _cache: dict[str, _CacheEntry] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.instruments:
            self.instruments = list(self.settings.instruments_of_type(InstrumentType.STOCKS))

    def _cache_fresh(self, entry: _CacheEntry) -> bool:
        ttl_days = self.settings.chart_pf_optim_cache_ttl_days
        if ttl_days <= 0:
            return False
        age = datetime.now(UTC) - entry.computed_at
        return age < timedelta(days=ttl_days)

    def get_result(self, product_key: str) -> PortfolioResult:
        obj = portfolio_objective(product_key)
        cached = self._cache.get(product_key)
        if cached is not None and self._cache_fresh(cached):
            logger.debug(
                f"Portfolio optim cache hit {product_key} "
                f"(age={(datetime.now(UTC) - cached.computed_at).total_seconds():.0f}s, "
                f"ttl_days={self.settings.chart_pf_optim_cache_ttl_days})"
            )
            return cached.result

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
        self._cache[product_key] = _CacheEntry(
            result=result,
            computed_at=datetime.now(UTC),
        )
        logger.info(
            f"Optim {product_key}: Sharpe={result.sharpe:.2f} "
            f"vol={result.vol_ann:.2%} legs="
            f"{sum(1 for w in result.weights.values() if w > 1e-4)}"
        )
        return result

    def weights_for(self, product_key: str) -> dict[str, float]:
        return dict(self.get_result(product_key).weights)
