"""Fetch des chandeliers OHLCV via l'endpoint v2 ``/v2/aggs/ticker/{t}/range/...``.

Cet endpoint est partagé par 4 types d'instruments : forex, stocks, indices,
options. Contrairement à l'endpoint futures (``/futures/v1/aggs/{ticker}``),
il utilise :

- un path avec ``multiplier`` et ``timespan`` séparés (ex: ``/range/1/minute/...``)
  au lieu d'un seul ``resolution`` ;
- des timestamps en **millisecondes** (au lieu de nanosecondes) ;
- des **champs courts** (``o, h, l, c, v, n, t, vw``) au lieu des champs longs
  (``open, high, ...``) ;
- un paramètre ``adjusted`` (split-adjustment) — mis à ``false`` pour stocks
  car MyQuantStore stocke les prix **bruts** et applique l'ajustement split à la
  query (permet le toggle ``--no-split`` au runtime).

On normalise la réponse v2 vers le **schéma canonique** MyQuantStore (champs longs)
pour que l'agrégateur et le resampler restent génériques. Pour les types sans
``session_end_date`` (tous sauf futures), on synthétise
``session_end_date = window_start.date()`` pour que le resampler (ancré par
session) reste fonctionnel.
"""

from __future__ import annotations

import polars as pl

from myquantstore.api.client import MassiveClient
from myquantstore.config import Settings
from myquantstore.instruments import Instrument, InstrumentType, parse_timeframe
from myquantstore.logging_setup import get_logger

logger = get_logger("aggs.v2")


def fetch_aggs_v2(
    client: MassiveClient,
    instrument: Instrument,
    settings: Settings,
    date_from: str | None = None,
    date_to: str | None = None,
) -> pl.DataFrame:
    """Récupère les chandeliers OHLCV d'un instrument via l'endpoint v2.

    :param client: Client Massive authentifié.
    :param instrument: Instrument cible (forex, stocks, indices, options).
        Le ticker préfixé (``C:EURUSD``, ``I:NDX``, ``O:...``) est dérivé
        automatiquement via :attr:`Instrument.api_ticker`.
    :param settings: Configuration (pour timeframe, page_limit).
    :param date_from: Date de début (YYYY-MM-DD). Si None, l'API retourne les
        chandeliers les plus récents.
    :param date_to: Date de fin (YYYY-MM-DD).
    :return: DataFrame Polars au **schéma canonique** MyQuantStore (champs longs),
        trié par ``window_start`` (Datetime[ns] UTC). Colonnes :
        ``window_start, ticker, open, high, low, close, volume, transactions,
        dollar_volume, vwap, session_end_date`` (les colonnes absentes de la
        réponse API ne sont pas créées — ex: indices n'ont pas de volume).
    :raises ValueError: Si le type n'est pas supporté par l'endpoint v2
        (futures utilise un endpoint dédié).
    """
    if instrument.type == InstrumentType.FUTURES:
        raise ValueError(
            "Les futures utilisent l'endpoint dédié /futures/v1/aggs — "
            "utilisez myquantstore.api.aggs_futures.fetch_aggs_futures."
        )

    # Barre de base Massive hardcodée (track stockage 1min).
    multiplier, timespan = parse_timeframe("1min")
    api_ticker = instrument.api_ticker

    # Pour stocks, on demande les prix NON ajustés (adjusted=false) car MyQuantStore
    # stocke le brut et applique l'ajustement split à la query (toggle --no-split).
    # Pour forex/indices, le param adjusted n'a pas d'effet (pas de splits) — on
    # l'omet pour ne pas perturber l'API.
    adjusted_param: dict[str, object] = {}
    if instrument.type == InstrumentType.STOCKS:
        adjusted_param = {"adjusted": "false"}

    logger.info(
        f"Fetch /v2/aggs/ticker/{api_ticker}/range/{multiplier}/{timespan}/"
        f"{date_from}/{date_to}"
        + ("&adjusted=false" if adjusted_param else "")
    )

    # L'endpoint v2 met multiplier/timespan/from/to dans le PATH (pas en query).
    # date_from/to sont requis par l'API v2 — si None, on utilise un range large.
    if date_from is None:
        date_from = "1970-01-01"
    if date_to is None:
        from datetime import date as _date

        date_to = _date.today().isoformat()

    path = f"/v2/aggs/ticker/{api_ticker}/range/{multiplier}/{timespan}/{date_from}/{date_to}"

    results = client.get_paginated(
        path,
        sort="asc",
        limit=settings.page_limit,
        **adjusted_param,
    )

    if not results:
        logger.warning(f"Aucun chandelier trouvé pour {instrument.key}")
        return _empty_v2_aggs_frame()

    df = pl.DataFrame(results)

    # --- Normalisation vers le schéma canonique (champs courts -> champs longs) ---
    # Timestamp t (ms Unix) -> window_start (Datetime[ns] UTC)
    if "t" in df.columns:
        if df.schema["t"] == pl.Utf8:
            df = df.with_columns(pl.col("t").cast(pl.Int64).alias("t"))
        # pl.from_epoch avec time_unit="ms" produit Datetime[us] par défaut ;
        # on convertit ensuite en ns pour matcher le schéma futures.
        df = df.with_columns(
            pl.from_epoch(pl.col("t"), time_unit="ms").cast(pl.Datetime("ns")).alias("window_start")
        )

    rename_map = {"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "n": "transactions", "vw": "vwap"}
    # On ne renomme que les colonnes présentes
    present_rename = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(present_rename)

    # dollar_volume = volume * vwap (si les deux présents) — approximation
    # (vwap est pondéré par volume donc volume*vwap = dollar volume exact).
    if "volume" in df.columns and "vwap" in df.columns:
        df = df.with_columns((pl.col("volume") * pl.col("vwap")).alias("dollar_volume"))

    # Cast volume/transactions en Int64 (l'API v2 renvoie Float64 pour v)
    for col in ("volume", "transactions"):
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False).alias(col))

    # ticker : utiliser le symbole nu (pas le ticker préfixé) pour cohérence
    # avec futures (ticker = identifiant de trading, ici le symbole).
    df = df.with_columns(pl.lit(instrument.symbol).alias("ticker"))

    # session_end_date : synthétisé = date du window_start (pas de notion de
    # session trading-date pour forex/stocks/indices). Permet au resampler
    # (ancré par session_end_date) de fonctionner uniformément.
    if "window_start" in df.columns:
        df = df.with_columns(pl.col("window_start").dt.date().alias("session_end_date"))

    # Tri par window_start (chronologique)
    if "window_start" in df.columns:
        df = df.sort("window_start")

    logger.info(f"Récupéré {df.height} chandelier(s) pour {instrument.key}")
    return df


def _empty_v2_aggs_frame() -> pl.DataFrame:
    """Retourne un DataFrame vide avec le schéma canonique v2 (minimal).

    Les colonnes volume/transactions/dollar_volume/vwap sont omises car
    absentes chez indices (et potentiellement d'autres). L'agrégateur et le
    resampler gèrent leur absence via des ``if col in df.columns``.
    """
    return pl.DataFrame(
        schema={
            "window_start": pl.Datetime("ns"),
            "ticker": pl.Utf8,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "session_end_date": pl.Date,
        }
    )
