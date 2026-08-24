"""Requêtes sur l'historique continu (commande ``query``).

La fonction :func:`query` interroge le cache agrégé d'un instrument et retourne
un DataFrame Polars filtré par plage temporelle. Plusieurs transformations
sont disponibles :

- ``k_minutes`` (``--timescale-unit/nb``) : rééchantillonnage des candles 1min
  en candles k-min. Voir :mod:`myquantstore.query.resampler`.
- ``intraday_begin`` / ``intraday_end`` : filtrage des candles par heure du jour
  (supporte le wrap-around, ex: 20:00-04:00).
- ``no_split`` (``--no-split``) : pour stocks, désactive l'ajustement split
  (activé par défaut). Les prix bruts sont stockés ; l'ajustement split se fait
  ici via le cache corporate actions.
- ``adjust_rollover`` (``--adjust``) : ajustement back-adjusted rollover (futures
  via :func:`~myquantstore.query.adjust.apply_rollover_adjustment`) ou dividend
  (stocks, après splits, via :func:`~myquantstore.query.adjust.apply_dividend_adjustment`).
  Désactivé par défaut.
- ``normalize_tick_size`` (``--normalize-tick-size``) : conversion prix →
  multiples entiers de tick size (``Int32``). Futures uniquement (requiert
  ``chain`` avec ``tick_size_for_ticker``).
- ``check_ticksize_accuracy`` (``--check-ticksize-accuracy``) : analyse la
  conformité des prix au tick size (read-only). Futures uniquement.
- ``include_cols`` (``--include-cols``) : restreint les colonnes renvoyées.
  Toute colonne absente lève ``ValueError``.
- ``forward_fill`` (``--forward-fill``) : opt-in, **OFF par défaut**. Après
  resample, réinsère les barres manquantes (intra-session / jours ouvrés)
  avec OHLC = dernier close. Voir :func:`myquantstore.query.resampler.forward_fill_ohlcv`.

**Multi-type** : ``chain`` est optionnel (:class:`myquantstore.chains.InstrumentChain`).
Pour forex/stocks/indices, on peut passer ``chain=None`` ou une
``SingleSymbolChain``. ``normalize_tick_size`` / ``check_ticksize_accuracy``
requièrent une chaîne avec un ``tick_size_for_ticker`` non nul (futures).

**Timestamps dupliqués (futures 1min)** : l'agrégat peut contenir deux lignes
au même ``window_start`` (deux ``ticker``) au jour de roll. ``query()``
déduplique **par défaut** (``dedup_timestamps=True``) après les ajustements
(Panama voit encore les deux contrats) et le bilan tick size. Si une
``RolloverChain`` est fournie, le contrat le plus récent de la chaîne gagne ;
sinon ``keep="last"``. ``--no-dedup-timestamps`` conserve les deux lignes.
Le chart s'appuie sur ce défaut (plus de ``unique`` côté chart).
"""

from __future__ import annotations

from datetime import UTC, datetime, time

import polars as pl
from rich.console import Console
from rich.table import Table

from myquantstore.chains import InstrumentChain
from myquantstore.config import Settings
from myquantstore.instruments import (
    DEFAULT_RESOLUTION,
    RESOLUTION_1DAY,
    TF_FAMILY_EXTRADAY,
    Instrument,
    InstrumentType,
    timeframe_family,
)
from myquantstore.logging_setup import get_logger
from myquantstore.query.adjust import (
    apply_dividend_adjustment,
    apply_rollover_adjustment,
    apply_split_adjustment,
)
from myquantstore.query.resampler import (
    filter_intraday,
    forward_fill_ohlcv,
    resample_extraday,
    resample_ohlcv,
)
from myquantstore.storage.aggregate_cache import read_aggregate

logger = get_logger("query")

# Colonnes de prix concernées par la normalisation et le test de qualité
_PRICE_COLS = ["open", "high", "low", "close", "settlement_price"]

# Seuils du bilan de qualité (codés en dur)
DATA_QUALITY_WARNING_THRESHOLD = 0.01  # ≥1% -> statut ATTENTION (WARNING log)
DATA_QUALITY_ERROR_THRESHOLD = 0.05  # ≥5% -> statut ERREUR (ERROR log, exit code 1)


class DataQualityError(Exception):
    """Levée quand le bilan de qualité tick_size dépasse le seuil d'erreur."""


def parse_query_datetime(value: str, *, is_end: bool = False) -> datetime:
    """Parse ``YYYY-MM-DD`` ou ISO datetime.

    Une date seule en borne de fin est inclusive (fin de journée 23:59:59.999999).
    Un datetime explicite est conservé tel quel.
    """
    raw = value.strip()
    parsed = datetime.fromisoformat(raw)
    if is_end and len(raw) == 10:
        return parsed.replace(hour=23, minute=59, second=59, microsecond=999999)
    return parsed


def query(
    instrument: Instrument,
    settings: Settings,
    chain: InstrumentChain | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    k_minutes: int = 1,
    intraday_begin: time | None = None,
    intraday_end: time | None = None,
    timezone: str = "UTC",
    adjust_rollover: bool = False,
    normalize_tick_size: bool = False,
    check_ticksize_accuracy: bool = False,
    no_split: bool = False,
    limit: int | None = None,
    resolution: str | None = None,
    k_days: int = 1,
    week_aligned: bool = False,
    dedup_timestamps: bool = True,
    include_cols: list[str] | None = None,
    forward_fill: bool = False,
) -> pl.DataFrame:
    """Interroge l'historique continu d'un instrument.

    :param instrument: Instrument cible.
    :param settings: Configuration.
    :param chain: Chaîne d'instrument (RolloverChain futures, SingleSymbolChain
        autres). Optionnel — requis uniquement pour ``normalize_tick_size`` et
        ``check_ticksize_accuracy``.
    :param start: Date/time de début (inclusive). Si None, depuis le début.
    :param end: Date/time de fin (inclusive). Si None, jusqu'à la fin.
    :param k_minutes: Rééchantillonnage en k minutes (track ``1min``).
    :param intraday_begin: Heure de début intraday (HH:MM). Wrap-around supporté.
    :param intraday_end: Heure de fin intraday (HH:MM).
    :param timezone: Fuseau IANA pour interpréter ``intraday_begin/end`` (défaut UTC).
    :param adjust_rollover: Si True, applique l'ajustement (back-adjusted rollover pour futures,
        dividends pour stocks après splits). Non activé par défaut.
    :param normalize_tick_size: Si True, convertit OHLC + settlement en Int32
        (multiples de tick). Requiert ``chain`` (futures).
    :param check_ticksize_accuracy: Si True, analyse la conformité au tick size.
        Requiert ``chain`` (futures).
    :param no_split: Si True, désactive l'ajustement split (stocks). Par défaut
        (False), l'ajustement split est appliqué pour les stocks via le cache
        corporate actions (Massive pour 1min, Yahoo pour 1day).
    :param limit: Plafond data optionnel (head). La CLI ``query --limit`` ne
        l'utilise pas : elle borne uniquement l'affichage via ``display_max_rows``.
    :param resolution: Résolution de stockage (``1min`` | ``1day``). Défaut ``1min``.
    :param k_days: Rééchantillonnage extraday en k jours (track ``1day``).
    :param week_aligned: Si True (UT ``week``), buckets ancrés lundi ISO.
    :param dedup_timestamps: Si True (défaut), une barre par ``window_start``
        après ajustements. Au roll, le contrat le plus récent de ``chain``
        gagne. False = conserver les deux tickers (contrat de l'agrégat).
    :param include_cols: Si fourni, ne conserve que ces colonnes (ordre conservé).
        Toute colonne absente lève ``ValueError``.
    :param forward_fill: Si True, réinsère les barres manquantes après
        resample (intra-session / jours ouvrés) avec OHLC = dernier close.
        **OFF par défaut** — ``query()`` n'invente pas de données.
    :return: DataFrame Polars de l'historique (filtré, éventuellement ajusté,
        dédupliqué, resamplé, forward-fillé et normalisé).
    """
    res = resolution or DEFAULT_RESOLUTION
    is_extraday = res == RESOLUTION_1DAY or timeframe_family(res) == TF_FAMILY_EXTRADAY

    # --- Incompatibilité mutuelle ---
    if adjust_rollover and normalize_tick_size:
        raise ValueError(
            "normalize_tick_size et adjust_rollover sont incompatibles. "
            "L'ajustement futur devra calculer en prix réels (Float64) "
            "ou en unités de tick (Int32), mais pas les deux simultanément."
        )

    # --- Validation intraday ---
    if intraday_begin is not None and intraday_end is not None and intraday_begin == intraday_end:
        raise ValueError(
            "intraday_begin et intraday_end doivent être différents. "
            "Pour ne pas filtrer, omettez les deux paramètres."
        )

    # --- Validation k ---
    if k_minutes < 1:
        raise ValueError(f"k_minutes doit être >= 1 (reçu: {k_minutes})")
    if k_days < 1:
        raise ValueError(f"k_days doit être >= 1 (reçu: {k_days})")

    # --- Validation chain requise pour tick_size ---
    if (normalize_tick_size or check_ticksize_accuracy) and chain is None:
        raise ValueError(
            "normalize_tick_size et check_ticksize_accuracy requièrent une chaîne "
            "(chain) avec tick_size_for_ticker — passez une RolloverChain (futures)."
        )

    if is_extraday and normalize_tick_size:
        raise ValueError("normalize_tick_size n'est pas applicable au track extraday (1day).")

    # --- Lecture du cache agrégé (résolution) ---
    df = read_aggregate(instrument, settings, resolution=res)

    # --- Filtrage temporel (start/end datetime) ---
    # On strip la timezone des deux côtés (colonne + paramètre) pour comparer naive vs naive.
    if start is not None:
        start_naive = (
            start.astimezone(UTC).replace(tzinfo=None) if start.tzinfo is not None else start
        )
        df = df.filter(pl.col("window_start").dt.replace_time_zone(None) >= start_naive)
    if end is not None:
        end_naive = end.astimezone(UTC).replace(tzinfo=None) if end.tzinfo is not None else end
        df = df.filter(pl.col("window_start").dt.replace_time_zone(None) <= end_naive)

    # --- Ajustements de prix (avant filtrage intraday et resample) ---
    # Splits d'abord, puis dividends si --adjust (stocks)
    if instrument.type == InstrumentType.STOCKS and not no_split:
        df = _apply_stock_split_adjustment(df, instrument, settings, resolution=res)

    if adjust_rollover:
        if instrument.type == InstrumentType.STOCKS:
            df = _apply_stock_dividend_adjustment(df, instrument, settings, resolution=res)
        elif instrument.type == InstrumentType.FUTURES:
            if is_extraday:
                logger.warning(
                    f"{instrument.key}: --adjust no-op sur track 1day futures "
                    "(série Yahoo =F déjà continue / souvent back-adjusted)"
                )
            elif chain is not None:
                df = apply_rollover_adjustment(df, chain)

    # --- Filtrage intraday (par heure du jour, dans timezone) — track 1min only ---
    tz = timezone or "UTC"
    if not is_extraday and intraday_begin is not None and intraday_end is not None:
        df = filter_intraday(df, intraday_begin, intraday_end, timezone=tz)

    # --- Bilan qualité tick size (read-only, affiche un bilan) ---
    if check_ticksize_accuracy and chain is not None:
        bilan = check_ticksize_accuracy_fn(df, chain, settings.data_quality_trigger)
        _print_quality_bilan(str(instrument), bilan)
        if not bilan.is_empty() and bilan.filter(pl.col("statut") == "ERREUR").height > 0:
            raise DataQualityError(
                f"Qualité tick_size ERREUR pour {instrument} "
                f"(seuil {DATA_QUALITY_ERROR_THRESHOLD:.0%})"
            )

    # --- Dédup timestamps (après adjust / bilan, avant normalize + resample) ---
    if dedup_timestamps:
        df = _dedup_timestamps(df, chain)

    # --- Normalisation tick size (à la lecture) ---
    if normalize_tick_size and chain is not None:
        df = _normalize_tick_size(df, chain)

    # --- Rééchantillonnage ---
    if is_extraday:
        if k_days > 1:
            df = resample_extraday(df, k_days, week_aligned=week_aligned)
    elif k_minutes > 1:
        df = resample_ohlcv(df, k_minutes, intraday_begin, intraday_end, timezone=tz)

    # --- Forward-fill des barres manquantes (opt-in, après resample) ---
    if forward_fill:
        df = forward_fill_ohlcv(
            df,
            is_extraday=is_extraday,
            k_minutes=k_minutes,
            k_days=k_days,
            week_aligned=week_aligned,
            timezone=tz,
        )

    # --- Limit ---
    if limit is not None and limit > 0:
        df = df.head(limit)

    if include_cols is not None:
        missing = [c for c in include_cols if c not in df.columns]
        if missing:
            raise ValueError(
                "Colonnes inconnues pour include_cols: "
                + ", ".join(missing)
                + f" (disponibles: {', '.join(df.columns)})"
            )
        df = df.select(include_cols)

    return df


def _dedup_timestamps(
    df: pl.DataFrame,
    chain: InstrumentChain | None,
) -> pl.DataFrame:
    """Une barre par ``window_start`` (jour de roll : deux contrats).

    Avec une chaîne à segments (futures), le contrat le plus récent gagne.
    Sans chaîne, ``keep="last"`` après tri sur ``window_start``.
    """
    if df.is_empty() or "window_start" not in df.columns:
        return df

    segments = getattr(chain, "segments", None) if chain is not None else None
    if segments and "ticker" in df.columns:
        rank = {seg.ticker: i for i, seg in enumerate(segments)}
        df = df.with_columns(
            pl.col("ticker").cast(pl.Utf8).replace_strict(rank, default=-1).alias("_roll_rank")
        )
        df = df.sort(["window_start", "_roll_rank"]).unique(subset=["window_start"], keep="last")
        return df.drop("_roll_rank").sort("window_start")

    return df.unique(subset=["window_start"], keep="last").sort("window_start")


def _apply_stock_split_adjustment(
    df: pl.DataFrame,
    instrument: Instrument,
    settings: Settings,
    *,
    resolution: str = DEFAULT_RESOLUTION,
) -> pl.DataFrame:
    """Applique l'ajustement split (Massive corp actions ou Yahoo selon résolution)."""
    if resolution == RESOLUTION_1DAY:
        from myquantstore.yahoo_actions.cache import YahooActionsCache

        try:
            splits = YahooActionsCache(instrument.symbol, "splits", settings).get()
            return apply_split_adjustment(df, splits)
        except (FileNotFoundError, ValueError):
            logger.warning(
                f"Pas de cache yahoo_actions.splits pour {instrument.symbol} — prix bruts. "
                "Lancez 'myquantstore fetch --timeframe 1day'."
            )
            return df

    from myquantstore.corporate_actions.cache import CorporateActionsCache

    try:
        splits_cache = CorporateActionsCache(instrument.symbol, "splits", settings)
        splits = splits_cache.get()
        return apply_split_adjustment(df, splits)
    except FileNotFoundError:
        logger.warning(
            f"Pas de cache splits pour {instrument.symbol} — prix non ajustés (bruts). "
            "Lancez 'myquantstore fetch' pour peupler le cache splits."
        )
        return df


def _apply_stock_dividend_adjustment(
    df: pl.DataFrame,
    instrument: Instrument,
    settings: Settings,
    *,
    resolution: str = DEFAULT_RESOLUTION,
) -> pl.DataFrame:
    """Applique l'ajustement dividend (Massive ou Yahoo selon résolution)."""
    if resolution == RESOLUTION_1DAY:
        from myquantstore.yahoo_actions.cache import YahooActionsCache

        try:
            dividends = YahooActionsCache(instrument.symbol, "dividends", settings).get()
            return apply_dividend_adjustment(df, dividends)
        except (FileNotFoundError, ValueError):
            logger.warning(
                f"Pas de cache yahoo_actions.dividends pour {instrument.symbol} — sans adjust div."
            )
            return df

    from myquantstore.corporate_actions.cache import CorporateActionsCache

    try:
        div_cache = CorporateActionsCache(instrument.symbol, "dividends", settings)
        dividends = div_cache.get()
        return apply_dividend_adjustment(df, dividends)
    except FileNotFoundError:
        logger.warning(
            f"Pas de cache dividends pour {instrument.symbol} — prix non ajustés (bruts). "
            "Lancez 'myquantstore fetch' pour peupler le cache dividends."
        )
        return df


def _normalize_tick_size(df: pl.DataFrame, chain: InstrumentChain) -> pl.DataFrame:
    """Convertit les colonnes de prix en multiples entiers de tick size (Int32).

    Pour chaque ticker, on divise les colonnes de prix par le ``trade_tick_size``
    du contrat (via la chaîne), on arrondit, et on cast en Int32. Si le tick
    size est 0.0 (SingleSymbolChain), la normalisation est skippée pour ce ticker.
    """
    logger.info("Normalisation tick_size: conversion Float64 -> Int32")

    if "ticker" not in df.columns:
        return df

    tickers = df["ticker"].unique().to_list()

    for ticker in tickers:
        tick = chain.tick_size_for_ticker(ticker)
        if tick <= 0:
            logger.warning(f"tick_size=0 pour {ticker} — skip normalisation (type non-futures)")
            continue

        for col in _PRICE_COLS:
            if col not in df.columns:
                continue
            df = df.with_columns(
                pl.when(pl.col("ticker") == ticker)
                .then((pl.col(col) / tick).round())
                .otherwise(pl.col(col))
                .alias(col)
            )

    # Cast final en Int32 pour toutes les colonnes de prix présentes
    for col in _PRICE_COLS:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Int32))

    logger.info(f"Normalisation terminée pour {len(tickers)} ticker(s)")
    return df


def check_ticksize_accuracy_fn(
    df: pl.DataFrame,
    chain: InstrumentChain,
    trigger: float,
) -> pl.DataFrame:
    """Analyse la conformité des prix au tick size et retourne un bilan par ticker.

    Pour chaque ticker et chaque colonne de prix, on compte le nombre de
    valeurs non conformes : ``ABS((prix/tick) - round(prix/tick)) > trigger * tick``.
    Les tickers avec ``tick_size=0`` (non-futures) sont skippés.
    """
    rows: list[dict[str, object]] = []
    if "ticker" not in df.columns:
        return pl.DataFrame()
    tickers = df["ticker"].unique().to_list()

    for ticker in tickers:
        tick = chain.tick_size_for_ticker(ticker)
        if tick <= 0:
            continue

        subset = df.filter(pl.col("ticker") == ticker)
        total = subset.height

        bad_mask = pl.lit(False)
        for col in _PRICE_COLS:
            if col not in subset.columns:
                continue
            col_bad = (pl.col(col) / tick - (pl.col(col) / tick).round()).abs() > trigger * tick
            bad_mask = bad_mask | col_bad

        nb_bad = subset.filter(bad_mask).height
        ratio = nb_bad / total if total > 0 else 0.0

        if ratio >= DATA_QUALITY_ERROR_THRESHOLD:
            statut = "ERREUR"
        elif ratio >= DATA_QUALITY_WARNING_THRESHOLD:
            statut = "ATTENTION"
        else:
            statut = "OK"

        rows.append(
            {
                "ticker": ticker,
                "tick_size": tick,
                "total_candles": total,
                "non_conformes": nb_bad,
                "ratio": ratio,
                "statut": statut,
            }
        )

    return pl.DataFrame(rows)


def _print_quality_bilan(label: str, bilan: pl.DataFrame) -> None:
    """Affiche le bilan de qualité tick_size sur stdout (table riche)."""
    if bilan.is_empty():
        logger.info(f"Bilan qualité tick_size pour {label}: aucun ticker à analyser")
        return

    console = Console()
    table = Table(title=f"== {label} — Bilan qualité tick size ==")
    table.add_column("ticker", style="cyan")
    table.add_column("tick_size", justify="right")
    table.add_column("total_candles", justify="right")
    table.add_column("non_conformes", justify="right")
    table.add_column("ratio", justify="right")
    table.add_column("statut")

    total_candles = 0
    total_bad = 0

    for row in bilan.iter_rows(named=True):
        ticker = row["ticker"]
        tick = row["tick_size"]
        candles = row["total_candles"]
        bad = row["non_conformes"]
        ratio = row["ratio"]
        statut = row["statut"]

        if statut == "OK":
            statut_str = f"[green]{statut}[/green]"
            logger.info(
                f"Qualité tick_size {ticker}: {bad}/{candles} non conformes ({ratio:.4%}) — OK"
            )
        elif statut == "ATTENTION":
            statut_str = f"[yellow]{statut}[/yellow]"
            logger.warning(
                f"Qualité tick_size {ticker}: {bad}/{candles} non conformes ({ratio:.2%}) — ATTENTION"
            )
        else:
            statut_str = f"[red]{statut}[/red]"
            logger.error(
                f"Qualité tick_size {ticker}: {bad}/{candles} non conformes ({ratio:.2%}) — ERREUR"
            )

        table.add_row(str(ticker), str(tick), str(candles), str(bad), f"{ratio:.4%}", statut_str)
        total_candles += candles
        total_bad += bad

    total_ratio = total_bad / total_candles if total_candles > 0 else 0.0
    if total_ratio >= DATA_QUALITY_ERROR_THRESHOLD:
        total_statut = "[red]ERREUR[/red]"
    elif total_ratio >= DATA_QUALITY_WARNING_THRESHOLD:
        total_statut = "[yellow]ATTENTION[/yellow]"
    else:
        total_statut = "[green]OK[/green]"

    table.add_row(
        "[bold]TOTAL[/bold]",
        "—",
        str(total_candles),
        str(total_bad),
        f"{total_ratio:.4%}",
        total_statut,
    )

    console.print(table)
