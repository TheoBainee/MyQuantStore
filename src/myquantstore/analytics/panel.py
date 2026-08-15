"""Panel multi-instruments de closes ajustés (track 1day / resample weekly)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import polars as pl

from myquantstore.config import Settings
from myquantstore.instruments import RESOLUTION_1DAY, Instrument, InstrumentType
from myquantstore.logging_setup import get_logger
from myquantstore.query.reader import query
from myquantstore.storage.aggregate_cache import aggregate_exists

logger = get_logger("analytics.panel")


@dataclass(slots=True)
class PricePanel:
    """Closes alignés en wide format ``[date, SYM1, SYM2, …]``."""

    prices: pl.DataFrame
    symbols: list[str]
    dropped: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    start: datetime | None = None
    end: datetime | None = None
    timescale: str = "day"

    @property
    def n_assets(self) -> int:
        return len(self.symbols)

    @property
    def n_obs(self) -> int:
        return self.prices.height


def _default_start(settings: Settings) -> datetime:
    years = settings.portfolio_default_lookback_years
    return datetime.now(UTC).replace(tzinfo=None) - timedelta(days=365 * years)


def _parse_dt(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = value.strip()
        if len(s) == 10:
            dt = datetime.fromisoformat(s)
        else:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC).replace(tzinfo=None)
    return dt


def build_price_panel(
    instruments: list[Instrument],
    settings: Settings,
    *,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
    timescale: str = "day",
    adjust_dividends: bool = True,
    min_coverage: float | None = None,
) -> PricePanel:
    """Charge les closes ajustés et construit un panel wide aligné.

    :param instruments: Univers (typiquement stocks avec agrégé 1day).
    :param settings: Config.
    :param start/end: Fenêtre (défaut lookback_years → aujourd'hui).
    :param timescale: ``day`` ou ``week``.
    :param adjust_dividends: Total return si True (``adjust_rollover`` query).
    :param min_coverage: Seuil de couverture date (défaut config).
    """
    if timescale not in ("day", "week"):
        raise ValueError("timescale doit être 'day' ou 'week'")

    start_dt = _parse_dt(start) or _default_start(settings)
    end_dt = _parse_dt(end)
    cov_thr = min_coverage if min_coverage is not None else settings.portfolio_min_coverage
    week_aligned = timescale == "week"
    k_days = 7 if week_aligned else 1

    series: list[pl.DataFrame] = []
    skipped: list[str] = []
    warnings: list[str] = []

    for inst in instruments:
        if inst.type != InstrumentType.STOCKS:
            warnings.append(f"{inst.key}: type {inst.type.value} ignoré (portfolio v1 = stocks)")
            skipped.append(inst.symbol)
            continue
        if not aggregate_exists(inst, settings, resolution=RESOLUTION_1DAY):
            warnings.append(f"{inst.key}: pas d'agrégé 1day — ignoré")
            skipped.append(inst.symbol)
            continue
        try:
            df = query(
                inst,
                settings,
                chain=None,
                start=start_dt,
                end=end_dt,
                resolution=RESOLUTION_1DAY,
                k_days=k_days,
                week_aligned=week_aligned,
                no_split=False,
                adjust_rollover=adjust_dividends,
                limit=None,
            )
        except Exception as exc:
            warnings.append(f"{inst.key}: query échouée ({exc})")
            skipped.append(inst.symbol)
            continue

        if df.is_empty() or "close" not in df.columns:
            warnings.append(f"{inst.key}: aucune donnée close")
            skipped.append(inst.symbol)
            continue

        one = (
            df.select(
                pl.col("window_start").dt.date().alias("date"),
                pl.col("close").cast(pl.Float64).alias(inst.symbol),
            )
            .group_by("date")
            .agg(pl.col(inst.symbol).last())
            .sort("date")
        )
        series.append(one)

    if not series:
        raise ValueError(
            "Aucun titre exploitable pour le panel. "
            "Vérifiez fetch --timeframe 1day et la liste stocks."
        )

    wide = series[0]
    for s in series[1:]:
        wide = wide.join(s, on="date", how="full", coalesce=True)
    wide = wide.sort("date")

    symbol_cols = [c for c in wide.columns if c != "date"]
    n_dates = wide.height
    if n_dates == 0:
        raise ValueError("Panel vide après alignement des dates.")

    kept: list[str] = []
    dropped: list[str] = list(skipped)
    for col in symbol_cols:
        non_null = wide[col].drop_nulls().len()
        coverage = non_null / n_dates if n_dates else 0.0
        if coverage < cov_thr:
            dropped.append(col)
            warnings.append(
                f"{col}: coverage={coverage:.1%} < {cov_thr:.0%} — exclu du panel"
            )
        else:
            kept.append(col)

    if len(kept) < 2:
        raise ValueError(
            f"Moins de 2 titres après filtre coverage (gardés={kept}, droppés={dropped}). "
            "Assouplissez min_coverage ou élargissez la fenêtre."
        )

    # Inner: ne garder que les dates où tous les titres retenus sont présents
    prices = wide.select(["date", *kept]).drop_nulls()
    if prices.height < 5:
        raise ValueError(
            f"Trop peu d'observations communes ({prices.height}). "
            "Élargissez la fenêtre ou réduisez l'univers."
        )

    logger.info(
        f"Panel: {len(kept)} titres × {prices.height} dates "
        f"[{prices['date'][0]} → {prices['date'][-1]}] timescale={timescale} "
        f"(droppés={len(dropped)})"
    )

    return PricePanel(
        prices=prices,
        symbols=kept,
        dropped=dropped,
        warnings=warnings,
        start=start_dt,
        end=end_dt,
        timescale=timescale,
    )
