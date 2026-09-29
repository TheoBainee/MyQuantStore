"""Ajustements des prix à la query (splits, dividends).

MyQuantStore stocke les prix **bruts** (``adjusted=false`` au fetch pour stocks)
et applique les ajustements à la lecture (query). Cela permet les toggles
runtime ``--no-split`` (splits ON par défaut) et ``--adjust`` (dividends + rollover back-adjusted).

Mécanisme d'ajustement (cf. doc API ``/stocks/v1/splits`` et ``/dividends``) :

    pour un prix à la date D, trouver le premier split dont
    execution_date > D et multiplier le prix brut par son
    historical_adjustment_factor (facteur cumulatif).

État d'implémentation :
- :func:`apply_split_adjustment` : **implémenté** (toggle ``--no-split``).
  - :func:`apply_dividend_adjustment` : **implémenté** (via ``--adjust`` pour stocks).
- :func:`apply_rollover_adjustment` : **implémenté** (back-adjusted via ``--adjust`` pour futures).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from myquantstore.logging_setup import get_logger

if TYPE_CHECKING:
    from myquantstore.contracts.rollover import RolloverChain

logger = get_logger("adjust")

# Colonnes de prix à ajuster (communes à tous les types ; settlement_price est
# futures-only mais sera absente des DataFrames stocks — géré par ``if col in``).
_PRICE_COLS = ["open", "high", "low", "close"]


def _left_join_keep_order(df: pl.DataFrame, right: pl.DataFrame, on: str) -> pl.DataFrame:
    """Jointure gauche qui conserve l'ordre des lignes de ``df``.

    L'ordre de sortie d'un ``join`` Polars n'est pas garanti (il peut varier d'un
    appel à l'autre) ; on le restaure via un index de ligne.
    """
    return (
        df.with_row_index("_join_row")
        .join(right, on=on, how="left")
        .sort("_join_row")
        .drop("_join_row")
    )


def _split_factor_by_candle_date(
    candle_dates: pl.Series,
    splits: pl.DataFrame,
) -> pl.DataFrame:
    """Mappe chaque date de chandelier → ``historical_adjustment_factor``.

    Pour la date D : facteur du premier split avec ``execution_date > D``,
    sinon 1.0. Implémenté via ``join_asof`` forward sur ``D + 1 jour``.
    """
    mapping = pl.DataFrame({"_candle_date": candle_dates}).with_columns(
        (pl.col("_candle_date") + pl.duration(days=1)).alias("_search_date")
    )
    splits_for_join = (
        splits.select(["execution_date", "historical_adjustment_factor"])
        .rename(
            {
                "execution_date": "_search_date",
                "historical_adjustment_factor": "_split_factor",
            }
        )
        .sort("_search_date")
    )
    mapping = mapping.sort("_search_date").join_asof(
        splits_for_join,
        on="_search_date",
        strategy="forward",
    )
    return mapping.with_columns(
        pl.col("_split_factor").fill_null(1.0).alias("_split_factor")
    )


def apply_split_adjustment(
    df: pl.DataFrame,
    splits: pl.DataFrame,
) -> pl.DataFrame:
    """Applique l'ajustement split aux prix bruts d'un stock.

    Pour chaque chandelier à la date D, on multiplie les prix par le facteur
    cumulatif du premier split **postérieur** à D (``execution_date > D``).
    Si aucun split postérieur n'existe, le facteur est 1.0 (pas d'ajustement).

    :param df: DataFrame de chandeliers bruts avec colonne ``window_start``
        (Datetime) et les colonnes de prix (open/high/low/close).
    :param splits: DataFrame des splits (issu du cache corporate actions) avec
        colonnes ``execution_date`` (Date) et ``historical_adjustment_factor`` (Float).
    :return: DataFrame avec prix ajustés (mêmes colonnes, mêmes dtypes).
    """
    return _scale_prices_by_split_factor(df, splits, invert=False, log_label="Ajustement split appliqué")


def reverse_split_adjustment(
    df: pl.DataFrame,
    splits: pl.DataFrame,
) -> pl.DataFrame:
    """Annule un ajustement split déjà présent dans les prix (Yahoo chart).

    L'API chart Yahoo renvoie des OHLC **déjà back-adjustés splits**.
    Pour stocker des prix bruts (alignés Massive ``adjusted=false``) ::

        raw = adj / historical_adjustment_factor

    (inverse exact de :func:`apply_split_adjustment`). Volume inchangé.
    """
    return _scale_prices_by_split_factor(
        df, splits, invert=True, log_label="Désajustement split (Yahoo → brut) appliqué"
    )


def _scale_prices_by_split_factor(
    df: pl.DataFrame,
    splits: pl.DataFrame,
    *,
    invert: bool,
    log_label: str,
) -> pl.DataFrame:
    if splits is None or splits.is_empty():
        logger.debug("Aucun split — pas d'ajustement")
        return df
    if "window_start" not in df.columns:
        return df

    splits = splits.sort("execution_date")
    df = df.with_columns(pl.col("window_start").dt.date().alias("_candle_date"))
    mapping = _split_factor_by_candle_date(df["_candle_date"].unique().sort(), splits)

    df = _left_join_keep_order(
        df, mapping.select(["_candle_date", "_split_factor"]), "_candle_date"
    )
    df = df.with_columns(pl.col("_split_factor").fill_null(1.0))

    # Évite division par zéro (facteur invalide → no-op sur la barre)
    safe_factor = (
        pl.when(pl.col("_split_factor") == 0.0)
        .then(pl.lit(1.0))
        .otherwise(pl.col("_split_factor"))
    )
    scale = (1.0 / safe_factor) if invert else safe_factor

    for col in _PRICE_COLS:
        if col in df.columns:
            df = df.with_columns((pl.col(col) * scale).alias(col))

    df = df.drop(["_candle_date", "_split_factor"])
    logger.info(log_label)
    return df


def apply_dividend_adjustment(
    df: pl.DataFrame,
    dividends: pl.DataFrame,
) -> pl.DataFrame:
    """Applique l'ajustement dividend aux prix bruts d'un stock.

    Pour chaque chandelier à la date D, on multiplie les prix par le facteur
    cumulatif du premier dividend dont ``ex_dividend_date > D``.
    Si aucun, facteur = 1.0.

    Même logique que les splits (voir apply_split_adjustment).

    :param df: DataFrame de chandeliers bruts avec colonne ``window_start``
        (Datetime) et les colonnes de prix (open/high/low/close).
    :param dividends: DataFrame des dividends (issu du cache) avec
        colonnes ``ex_dividend_date`` (Date) et ``historical_adjustment_factor`` (Float).
    :return: DataFrame avec prix ajustés (mêmes colonnes, mêmes dtypes).
    """
    if dividends is None or dividends.is_empty():
        logger.debug("Aucun dividend — pas d'ajustement")
        return df
    if "window_start" not in df.columns:
        return df

    dividends = dividends.sort("ex_dividend_date")

    df = df.with_columns(pl.col("window_start").dt.date().alias("_candle_date"))

    candle_dates = df["_candle_date"].unique().sort()

    mapping = pl.DataFrame({"_candle_date": candle_dates}).with_columns(
        (pl.col("_candle_date") + pl.duration(days=1)).alias("_search_date")
    )

    divs_for_join = dividends.select(["ex_dividend_date", "historical_adjustment_factor"]).rename(
        {"ex_dividend_date": "_search_date", "historical_adjustment_factor": "_div_factor"}
    ).sort("_search_date")

    mapping = mapping.sort("_search_date").join_asof(
        divs_for_join,
        on="_search_date",
        strategy="forward",
    )
    mapping = mapping.with_columns(
        pl.col("_div_factor").fill_null(1.0).alias("_div_factor")
    )

    df = _left_join_keep_order(df, mapping.select(["_candle_date", "_div_factor"]), "_candle_date")
    df = df.with_columns(pl.col("_div_factor").fill_null(1.0))

    for col in _PRICE_COLS:
        if col in df.columns:
            df = df.with_columns((pl.col(col) * pl.col("_div_factor")).alias(col))

    df = df.drop(["_candle_date", "_div_factor"])
    logger.info("Ajustement dividend appliqué")
    return df


def apply_rollover_adjustment(
    df: pl.DataFrame,
    chain: RolloverChain,
) -> pl.DataFrame:
    """Applique l'ajustement back-adjusted pour les rollovers futures.

    Les prix des contrats antérieurs sont multipliés par des facteurs cumulés
    calculés à partir du close du segment précédent au rollover_date,
    de façon à obtenir une série continue (back-adjusted vers le contrat le plus récent).

    Accumulation des facteurs du contrat le plus récent (facteur=1) vers l'arrière.

    Ajuste OHLC + settlement_price (si présente).

    Le ticker original est conservé (pas de ticker synthétique).

    :param df: DataFrame avec colonne "ticker" et les prix (window_start en Datetime).
    :param chain: RolloverChain pour le produit.
    :return: DataFrame avec prix ajustés en Float64 (pour la période couverte).
    """
    if df.is_empty() or not getattr(chain, "segments", None):
        return df

    segments = sorted(chain.segments, key=lambda s: s.active_from)

    # Calcul des facteurs cumulés (du plus récent vers l'ancien)
    ticker_to_factor: dict[str, float] = {segments[-1].ticker: 1.0}
    cum_factor = 1.0

    for i in range(len(segments) - 2, -1, -1):
        prev_seg = segments[i]
        next_seg = segments[i + 1]
        rollover = next_seg.active_from  # date de bascule

        # Dernier close du contrat précédent avant le rollover
        prev_close = (
            df.filter(
                (pl.col("ticker") == prev_seg.ticker)
                & (pl.col("window_start").dt.date() < rollover)
            )
            .select(pl.col("close").last())
            .item(0, 0)
            if df.height > 0
            else None
        )

        # Premier close du nouveau contrat à partir du rollover
        next_close = (
            df.filter(
                (pl.col("ticker") == next_seg.ticker)
                & (pl.col("window_start").dt.date() >= rollover)
            )
            .select(pl.col("close").first())
            .item(0, 0)
            if df.height > 0
            else None
        )

        if prev_close is not None and next_close is not None and prev_close != 0:
            ratio = next_close / prev_close
            cum_factor *= ratio

        ticker_to_factor[prev_seg.ticker] = cum_factor

    # Colonnes de prix à ajuster (inclut settlement_price pour futures)
    price_cols = ["open", "high", "low", "close"]
    if "settlement_price" in df.columns:
        price_cols.append("settlement_price")

    for ticker, factor in ticker_to_factor.items():
        for col in price_cols:
            if col in df.columns:
                df = df.with_columns(
                    pl.when(pl.col("ticker") == ticker)
                    .then(pl.col(col) * factor)
                    .otherwise(pl.col(col))
                    .alias(col)
                )

    logger.info("Ajustement rollover back-adjusted appliqué")
    return df
