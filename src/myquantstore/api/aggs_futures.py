"""Fetch des chandeliers OHLCV futures via l'endpoint ``/futures/v1/aggs/{ticker}``.

Cet endpoint retourne les chandeliers agrégés (OHLCV) d'un contrat futures.
On utilise ``resolution=1min`` (paramètre ``timeframe`` de la config) et les
filtres ``window_start.gte`` / ``window_start.lte`` pour spécifier la plage
temporelle. Le timestamp ``window_start`` est en **nanosecondes** dans la
réponse API — on le convertit en ``Datetime[ns]`` Polars (UTC).

Schéma de réponse futures (champs longs) :
``close, dollar_volume, high, low, open, session_end_date, settlement_price,
ticker, transactions, volume, window_start``.

Note : ``session_end_date`` et ``settlement_price`` sont spécifiques aux futures
(n'existent pas pour forex/stocks/indices — voir :mod:`myquantstore.api.aggs_v2`).
"""

from __future__ import annotations

import polars as pl

from myquantstore.api.client import MassiveClient
from myquantstore.config import Settings
from myquantstore.logging_setup import get_logger

logger = get_logger("aggs.futures")

# Barre de base Massive hardcodée (stockage track ``1min``). Distinct du CLI
# ``--timeframe all|1min|1day`` qui sélectionne le track de stockage.
MASSIVE_BAR_RESOLUTION = "1min"


def _aggs_path(ticker: str) -> str:
    """Construit le path de l'endpoint /aggs futures pour un ticker de contrat."""
    return f"/futures/v1/aggs/{ticker}"


def fetch_aggs_futures(
    client: MassiveClient,
    ticker: str,
    settings: Settings,
    window_start_gte: str | None = None,
    window_start_lte: str | None = None,
) -> pl.DataFrame:
    """Récupère tous les chandeliers OHLCV d'un contrat futures.

    La pagination (``next_url``) est gérée automatiquement par le client.
    Resolution barre = ``MASSIVE_BAR_RESOLUTION`` (1min) ; ``page_limit`` depuis settings.

    :param client: Client Massive authentifié.
    :param ticker: Ticker du contrat futures (ex: "ESM5").
    :param settings: Configuration (page_limit, retries…).
    :param window_start_gte: Date/time de début (YYYY-MM-DD ou ns timestamp).
    :param window_start_lte: Date/time de fin (YYYY-MM-DD ou ns timestamp).
    :return: DataFrame Polars avec les chandeliers, triés par ``window_start``
        (Datetime[ns] UTC). Colonnes futures : open/high/low/close, volume,
        transactions, dollar_volume, settlement_price, session_end_date, ticker.
    """
    logger.info(
        f"Fetch /futures/v1/aggs/{ticker}?resolution={MASSIVE_BAR_RESOLUTION}"
        + (f"&gte={window_start_gte}" if window_start_gte else "")
        + (f"&lte={window_start_lte}" if window_start_lte else "")
    )

    results = client.get_paginated(
        _aggs_path(ticker),
        resolution=MASSIVE_BAR_RESOLUTION,
        **{"window_start.gte": window_start_gte} if window_start_gte else {},
        **{"window_start.lte": window_start_lte} if window_start_lte else {},
        limit=settings.page_limit,
        sort="window_start.asc",
    )

    if not results:
        logger.warning(f"Aucun chandelier trouvé pour {ticker}")
        return _empty_futures_aggs_frame()

    df = pl.DataFrame(results)

    # Conversion du timestamp window_start (nanosecondes Unix) -> Datetime[ns] UTC
    # L'API renvoie window_start en Int64 ou en string (grands entiers > 2^53)
    # selon la pagination. On s'assure qu'il est en Int64 avant la conversion.
    if "window_start" in df.columns:
        if df.schema["window_start"] == pl.Utf8:
            df = df.with_columns(pl.col("window_start").cast(pl.Int64).alias("window_start"))
        df = df.with_columns(
            pl.from_epoch(pl.col("window_start"), time_unit="ns").alias("window_start")
        )

    # Conversion de session_end_date (string YYYY-MM-DD) -> Date
    if "session_end_date" in df.columns:
        df = df.with_columns(
            pl.col("session_end_date").str.to_date("%Y-%m-%d").alias("session_end_date")
        )

    # Tri par window_start (chronologique)
    df = df.sort("window_start")

    logger.info(f"Récupéré {df.height} chandelier(s) pour {ticker}")
    return df


def _empty_futures_aggs_frame() -> pl.DataFrame:
    """Retourne un DataFrame vide avec le schéma canonique des aggs futures."""
    return pl.DataFrame(
        schema={
            "close": pl.Float64,
            "dollar_volume": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "open": pl.Float64,
            "session_end_date": pl.Date,
            "settlement_price": pl.Float64,
            "ticker": pl.Utf8,
            "transactions": pl.Int64,
            "volume": pl.Int64,
            "window_start": pl.Datetime("ns"),
        }
    )
