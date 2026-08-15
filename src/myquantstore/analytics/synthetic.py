"""Série OHLCV synthétique d'un portefeuille (combinaison linéaire + rebase 100).

Invariant : la combo se fait sur la **résolution de stockage** (1min ou 1day),
puis le resample éventuel. Jamais l'inverse.
"""

from __future__ import annotations

from datetime import datetime

import polars as pl

from myquantstore.config import Settings
from myquantstore.instruments import (
    RESOLUTION_1DAY,
    RESOLUTION_1MIN,
    Instrument,
    InstrumentType,
)
from myquantstore.logging_setup import get_logger
from myquantstore.query.reader import query
from myquantstore.query.resampler import resample_extraday, resample_ohlcv
from myquantstore.storage.aggregate_cache import aggregate_exists

logger = get_logger("analytics.synthetic")

# Poids en-dessous de ce seuil ignorés pour la série
_MIN_W = 1e-6


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    cleaned = {s: float(w) for s, w in weights.items() if float(w) >= _MIN_W}
    total = sum(cleaned.values())
    if total <= 0:
        raise ValueError("Somme des poids nulle — impossible de construire le panier")
    return {s: w / total for s, w in cleaned.items()}


def _load_leg_ohlcv(
    symbol: str,
    settings: Settings,
    *,
    resolution: str,
    start: datetime | None,
    end: datetime | None,
    adjust_dividends: bool,
) -> pl.DataFrame:
    inst = Instrument(type=InstrumentType.STOCKS, symbol=symbol)
    if not aggregate_exists(inst, settings, resolution=resolution):
        raise FileNotFoundError(f"{inst.key}: pas d'agrégé {resolution}")
    df = query(
        inst,
        settings,
        chain=None,
        start=start,
        end=end,
        resolution=resolution,
        k_minutes=1,
        k_days=1,
        no_split=False,
        adjust_rollover=adjust_dividends,
        limit=None,
    )
    if df.is_empty():
        raise ValueError(f"{inst.key}: aucune barre {resolution}")
    cols = ["window_start", "open", "high", "low", "close"]
    has_vol = "volume" in df.columns
    if has_vol:
        cols.append("volume")
    out = df.select(cols)
    # naive timestamps pour join
    out = out.with_columns(pl.col("window_start").dt.replace_time_zone(None))
    return out.sort("window_start")


def build_portfolio_ohlcv(
    weights: dict[str, float],
    settings: Settings,
    *,
    resolution: str = RESOLUTION_1DAY,
    start: datetime | None = None,
    end: datetime | None = None,
    k_minutes: int = 1,
    k_days: int = 1,
    week_aligned: bool = False,
    adjust_dividends: bool = True,
    rebase: float = 100.0,
) -> pl.DataFrame:
    """Construit OHLCV panier : combo sur ``resolution`` puis resample.

    :param weights: Poids longs-only (renormalisés).
    :param resolution: Barre de base ``1min`` ou ``1day``.
    :param k_minutes: Resample intraday après combo (si resolution=1min).
    :param k_days: Resample extraday après combo (si resolution=1day).
    :param rebase: Niveau de l'index à t0 (défaut 100).
    :return: DataFrame window_start, open, high, low, close, volume, ticker.
    """
    if resolution not in (RESOLUTION_1MIN, RESOLUTION_1DAY):
        raise ValueError(f"resolution invalide: {resolution}")

    w = _normalize_weights(weights)
    legs: dict[str, pl.DataFrame] = {}
    errors: list[str] = []

    for sym in w:
        try:
            legs[sym] = _load_leg_ohlcv(
                sym,
                settings,
                resolution=resolution,
                start=start,
                end=end,
                adjust_dividends=adjust_dividends,
            )
        except Exception as exc:
            errors.append(f"{sym}: {exc}")

    if len(legs) < 1:
        raise ValueError(
            "Aucun leg chargeable pour le panier. " + "; ".join(errors[:5])
        )
    if errors:
        logger.warning(f"Legs exclus du panier: {errors}")
        # renormalise sur legs OK
        w = _normalize_weights({s: w[s] for s in legs})

    # Alignement inner sur window_start
    symbols = list(legs.keys())
    base = legs[symbols[0]].select(
        pl.col("window_start"),
        pl.col("open").alias(f"{symbols[0]}__o"),
        pl.col("high").alias(f"{symbols[0]}__h"),
        pl.col("low").alias(f"{symbols[0]}__l"),
        pl.col("close").alias(f"{symbols[0]}__c"),
        (
            pl.col("volume").alias(f"{symbols[0]}__v")
            if "volume" in legs[symbols[0]].columns
            else pl.lit(None).cast(pl.Float64).alias(f"{symbols[0]}__v")
        ),
    )
    for sym in symbols[1:]:
        leg = legs[sym].select(
            pl.col("window_start"),
            pl.col("open").alias(f"{sym}__o"),
            pl.col("high").alias(f"{sym}__h"),
            pl.col("low").alias(f"{sym}__l"),
            pl.col("close").alias(f"{sym}__c"),
            (
                pl.col("volume").alias(f"{sym}__v")
                if "volume" in legs[sym].columns
                else pl.lit(None).cast(pl.Float64).alias(f"{sym}__v")
            ),
        )
        base = base.join(leg, on="window_start", how="inner")

    base = base.sort("window_start")
    if base.height < 2:
        raise ValueError("Moins de 2 barres communes après alignement des legs")

    # Prix panier non rebasé : sum w_i * P_i
    o_expr = pl.lit(0.0)
    h_expr = pl.lit(0.0)
    l_expr = pl.lit(0.0)
    c_expr = pl.lit(0.0)
    v_expr = pl.lit(0.0)
    for sym, wi in w.items():
        o_expr = o_expr + pl.col(f"{sym}__o") * wi
        h_expr = h_expr + pl.col(f"{sym}__h") * wi
        l_expr = l_expr + pl.col(f"{sym}__l") * wi
        c_expr = c_expr + pl.col(f"{sym}__c") * wi
        v_expr = v_expr + pl.col(f"{sym}__v").fill_null(0.0) * wi

    basket = base.select(
        pl.col("window_start"),
        o_expr.alias("open"),
        h_expr.alias("high"),
        l_expr.alias("low"),
        c_expr.alias("close"),
        v_expr.alias("volume"),
    )

    # Rebase 100 sur le close t0 (même facteur sur OHLC)
    c0 = float(basket["close"][0])
    if c0 == 0 or c0 != c0:  # noqa: PLR0124 — NaN check
        raise ValueError("close t0 invalide pour rebase")
    factor = rebase / c0
    basket = basket.with_columns(
        (pl.col("open") * factor).alias("open"),
        (pl.col("high") * factor).alias("high"),
        (pl.col("low") * factor).alias("low"),
        (pl.col("close") * factor).alias("close"),
    )

    # Identité + colonnes attendues par le resampler
    basket = basket.with_columns(
        pl.lit("PORTFOLIO").alias("ticker"),
        pl.lit("portfolio").alias("product_code"),
        pl.col("window_start").dt.date().alias("session_end_date"),
        pl.col("volume").fill_null(0.0).cast(pl.Float64),
        pl.lit(0).cast(pl.Int64).alias("transactions"),
        (pl.col("close") * pl.col("volume").fill_null(0.0)).alias("dollar_volume"),
    )

    # Resample après combo (jamais avant)
    if resolution == RESOLUTION_1MIN and k_minutes > 1:
        basket = resample_ohlcv(basket, k_minutes)
    elif resolution == RESOLUTION_1DAY and k_days > 1:
        basket = resample_extraday(basket, k_days, week_aligned=week_aligned)

    logger.info(
        f"Panier synthétique: {len(w)} legs × {basket.height} barres "
        f"res={resolution} k_min={k_minutes} k_days={k_days} rebase={rebase}"
    )
    return basket
