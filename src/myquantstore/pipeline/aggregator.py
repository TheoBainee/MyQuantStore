"""Agrégation des dumps pseudo-bruts en un cache agrégé (générique multi-type).

L'agrégation fusionne tous les dumps pseudo-bruts d'un instrument **pour une
résolution donnée** (tous tickers, tous runs confondus), déduplique les
chandeliers sur ``(window_start, ticker)``, et écrit le résultat dans
``data/aggregate/{type}/{symbol}/{resolution}.parquet``.

Cette fonction est **générique** — elle ne contient aucune logique de rollover
(spécifique futures). Le stitch 1min se fait au **fetch** (un contrat par
segment ``[active_from, active_until)``). La query n'applique le rollover que
pour ``--adjust`` (Panama) via :mod:`myquantstore.chains`.

**Déduplication** : clé ``(window_start, ticker)``, ``keep="last"`` car les
dumps sont lus par ordre chronologique des ``run_ts`` — le dernier dump
contient les données les plus récentes (re-fetch d'un même chandelier).

**Timestamps dupliqués (futures 1min)** : au jour de roll, l'ancien et le
nouveau contrat peuvent chacun avoir une barre au même ``window_start``
(filtres API ``gte``/``lte`` en date calendaire inclusive). Les deux lignes
sont **conservées ici** : ce sont deux faits distincts (deux contrats). Un
``unique(window_start)`` dans l'agrégat serait une décision de rollover
(quel contrat gagne). La série 1-timestamp = 1-barre est un choix de
``query()`` (``dedup_timestamps=True`` par défaut ; le contrat le plus
récent de la chaîne gagne). ``--no-dedup-timestamps`` conserve les deux.

**Cast Categorical** : ``run_id``, ``ticker``, ``symbol``, ``instrument_type``,
``product_code`` → ``Categorical`` (faible cardinalité, compact en Parquet).

**Cast entiers** : ``volume``, ``transactions`` → ``Int32`` si toutes les
valeurs tiennent dans Int32, sinon ``Int64`` (ex: volumes daily Yahoo
souvent > 2^31-1). Persisté dans le Parquet agrégé.
"""

from __future__ import annotations

import polars as pl

from myquantstore.config import Settings
from myquantstore.instruments import DEFAULT_RESOLUTION, RESOLUTION_1DAY, Instrument
from myquantstore.logging_setup import get_logger
from myquantstore.storage.aggregate_cache import write_aggregate
from myquantstore.storage.raw_dumps import read_all_runs

logger = get_logger("aggregator")

# Colonnes à caster en Categorical (faible cardinalité).
_CATEGORICAL_COLS = ["run_id", "ticker", "symbol", "instrument_type", "product_code"]

# Colonnes volume-like : Int32 si possible, sinon Int64.
_INT_VOLUME_COLS = ["volume", "transactions"]

_I32_MIN = -(2**31)
_I32_MAX = 2**31 - 1


def _source_for_resolution(resolution: str) -> str:
    """Provenance par défaut selon la résolution de stockage."""
    if resolution == RESOLUTION_1DAY:
        return "yahoo"
    return "massive"


def _cast_volume_col(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Cast ``col`` en Int32 si plage OK, sinon Int64 (pas de overflow)."""
    if col not in df.columns:
        return df
    # Nulls ignorés pour le test de plage
    stats = df.select(
        pl.col(col).min().alias("mn"),
        pl.col(col).max().alias("mx"),
    )
    mn, mx = stats["mn"][0], stats["mx"][0]
    if (
        mn is not None
        and mx is not None
        and float(mn) >= _I32_MIN
        and float(mx) <= _I32_MAX
    ):
        return df.with_columns(pl.col(col).cast(pl.Int32, strict=False))
    logger.info(
        f"Colonne '{col}' hors plage Int32 (min={mn}, max={mx}) — conservation Int64"
    )
    return df.with_columns(pl.col(col).cast(pl.Int64, strict=False))


def aggregate(
    instrument: Instrument,
    settings: Settings,
    resolution: str = DEFAULT_RESOLUTION,
) -> pl.DataFrame:
    """Agrège tous les dumps pseudo-bruts d'un instrument × résolution.

    :param instrument: Instrument cible.
    :param settings: Configuration.
    :param resolution: Résolution de stockage (``1min``, ``1day``, …).
    :return: Le DataFrame agrégé écrit.
    """
    logger.info(f"Début de l'agrégation pour {instrument.key} [{resolution}]")

    df = read_all_runs(instrument, settings, resolution=resolution)
    if df.is_empty():
        logger.warning(f"Aucun dump à agréger pour {instrument.key} [{resolution}]")
        return df

    nb_before = df.height

    # Cast Categorical sur les colonnes string répétées (optimisation mémoire)
    for col in _CATEGORICAL_COLS:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Categorical))

    # volume / transactions : Int32 si possible, sinon Int64
    for col in _INT_VOLUME_COLS:
        df = _cast_volume_col(df, col)

    # Déduplication sur (window_start, ticker) — keep="last"
    nb_before_dedup = df.height
    df = df.unique(subset=["window_start", "ticker"], keep="last")
    nb_after_dedup = df.height
    dedup_removed = nb_before_dedup - nb_after_dedup

    # Tri par window_start (chronologique)
    if "window_start" in df.columns:
        df = df.sort("window_start")

    source_dump_count = df["run_id"].n_unique() if "run_id" in df.columns else 0

    write_aggregate(
        df,
        instrument,
        settings,
        source_dump_count=source_dump_count,
        dedup_removed_count=dedup_removed,
        resolution=resolution,
        source=_source_for_resolution(resolution),
    )

    logger.info(
        f"Agrégation {instrument.key} [{resolution}] terminée: "
        f"{nb_before} -> {nb_after_dedup} lignes "
        f"({dedup_removed} doublons supprimés, {source_dump_count} dumps fusionnés)"
    )

    return df

