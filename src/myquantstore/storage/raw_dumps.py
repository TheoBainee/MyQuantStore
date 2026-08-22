"""Gestion des dumps pseudo-bruts (1 fichier Parquet par run).

Les "dumps bruts" (ou pseudo-bruts) ne sont **pas** la réponse JSON brute de l'API.
Ce sont les données retournées par l'API après normalisation minimale au format
interne canonique de MyQuantStore :
- conversion des timestamps (ns/ms → Datetime[ns] UTC)
- normalisation/renommage des champs
- ajout des colonnes d'identité (symbol, instrument_type, product_code, run_id)
- casts (volume/transactions → Int32, etc.)

Cette pratique est choisie pour praticité et performance.

**Contrainte absolue (même en alpha)** : il doit toujours être possible de
reconstruire l'agrégat d'une résolution à partir des dumps de **cette**
résolution (read_all_runs + concat + dédup keep=last sur (window_start, ticker)
+ casts).

Structure (layout multi-type × multi-résolution) ::

    data/raw/
    ├─ futures/                 # {type}
    │  ├─ ES/                   # {symbol}  (produit futures)
    │  │  ├─ ESM5/              # {ticker}  (contrat individuel)
    │  │  │  └─ 1min/           # {resolution}
    │  │  │     ├─ 20260704T180000.parquet
    │  │  │     └─ 20260704T180000.meta.json
    │  │  └─ ...
    │  └─ NQ/
    ├─ stocks/
    │  └─ AAPL/                 # {symbol}
    │     └─ AAPL/              # {ticker} = symbole
    │        ├─ 1min/           # Massive
    │        └─ 1day/           # Yahoo
    └─ ...

Chaque fichier est **immuable** (jamais écrasé) — un nouveau run crée un
nouveau fichier avec un ``run_ts`` unique. Cela permet l'audit et la
re-agrégation à partir des dumps pseudo-bruts.

Pour les types à symbole unique (forex, stocks, indices), le niveau ``ticker``
est identique au ``symbol`` (pas de notion de contrat individuel).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from myquantstore.config import Settings
from myquantstore.instruments import DEFAULT_RESOLUTION, Instrument
from myquantstore.logging_setup import get_logger
from myquantstore.storage.parquet_io import read_parquet, write_parquet

logger = get_logger("raw_dumps")


def save_raw_dump(
    df: pl.DataFrame,
    instrument: Instrument,
    ticker: str,
    run_ts: str,
    settings: Settings,
    source_url: str = "",
    page_count: int = 0,
    resolution: str = DEFAULT_RESOLUTION,
    source: str = "massive",
) -> Path:
    """Sauvegarde un dump pseudo-brut pour un run donné.

    Le DataFrame reçu est déjà normalisé au format canonique interne.
    Le fichier est écrit dans
    ``data/raw/{type}/{symbol}/{ticker}/{resolution}/{run_ts}.parquet``
    avec son sidecar ``.meta.json``.

    :param df: DataFrame des chandeliers OHLCV (normalisé) à sauvegarder.
    :param instrument: Instrument cible (porte le type et le symbole).
    :param ticker: Ticker de trading (contrat futures, ou symbole pour les autres).
    :param run_ts: Identifiant du run (format YYYYMMDDTHHMMSS).
    :param settings: Configuration.
    :param source_url: URL source de l'appel API (pour audit).
    :param page_count: Nombre de pages paginées pour ce dump.
    :param resolution: Résolution de stockage (``1min``, ``1day``, …).
    :param source: Provenance des données (``massive``, ``yahoo``, …).
    :return: Le chemin du fichier Parquet écrit.
    """
    path = settings.raw_dump_path(instrument, ticker, run_ts, resolution=resolution)

    extra_meta: dict[str, object] = {
        "instrument_type": instrument.type.value,
        "symbol": instrument.symbol,
        "ticker": ticker,
        "run_ts": run_ts,
        "resolution": resolution,
        "source": source,
        "source_url": source_url,
        "page_count": page_count,
    }

    # window_start_min / max pour audit rapide (si la colonne existe)
    if "window_start" in df.columns and df.height > 0:
        ws_min = df["window_start"].min()
        ws_max = df["window_start"].max()
        if ws_min is not None:
            extra_meta["window_start_min"] = str(ws_min)
        if ws_max is not None:
            extra_meta["window_start_max"] = str(ws_max)

    write_parquet(df, path, **extra_meta)
    logger.info(f"Dump brut sauvegardé: {path} ({df.height} chandeliers)")
    return path


def list_runs(
    instrument: Instrument,
    ticker: str,
    settings: Settings,
    resolution: str = DEFAULT_RESOLUTION,
) -> list[str]:
    """Liste tous les ``run_ts`` disponibles pour un ticker × résolution.

    :return: Liste triée des run_ts (YYYYMMDDTHHMMSS) par ordre chronologique.
    """
    res_dir = settings.raw_resolution_dir(instrument, ticker, resolution)
    if not res_dir.exists():
        return []
    return sorted(f.stem for f in res_dir.glob("*.parquet") if f.suffix == ".parquet")


def list_tickers(instrument: Instrument, settings: Settings) -> list[str]:
    """Liste tous les tickers ayant au moins un dump brut pour un instrument.

    Pour futures : les contrats individuels (ex: ["ESH5", "ESM5", "ESU5"]).
    Pour les autres types : le symbole unique (ex: ["AAPL"]).

    Un ticker est retenu s'il contient au moins un sous-répertoire de résolution
    non vide, ou (compat legacy non migré) des ``*.parquet`` directement.
    """
    symbol_dir = settings.raw_dumps_dir() / instrument.path_segment / instrument.symbol
    if not symbol_dir.exists():
        return []
    tickers: list[str] = []
    for d in symbol_dir.iterdir():
        if not d.is_dir():
            continue
        has_new = any(
            sub.is_dir() and any(sub.glob("*.parquet")) for sub in d.iterdir() if sub.is_dir()
        )
        has_legacy = any(d.glob("*.parquet"))
        if has_new or has_legacy:
            tickers.append(d.name)
    return sorted(tickers)


def list_resolutions(
    instrument: Instrument,
    settings: Settings,
    ticker: str | None = None,
) -> list[str]:
    """Liste les résolutions présentes pour un instrument (optionnellement un ticker)."""
    tickers = [ticker] if ticker is not None else list_tickers(instrument, settings)
    found: set[str] = set()
    for t in tickers:
        tdir = settings.raw_ticker_dir(instrument, t)
        if not tdir.exists():
            continue
        for sub in tdir.iterdir():
            if sub.is_dir() and any(sub.glob("*.parquet")):
                found.add(sub.name)
    return sorted(found)


def read_all_runs(
    instrument: Instrument,
    settings: Settings,
    resolution: str = DEFAULT_RESOLUTION,
) -> pl.DataFrame:
    """Lit et concatène tous les dumps pseudo-bruts d'un instrument × résolution.

    Utilisé par l'agrégateur pour reconstruire l'historique complet à partir
    des dumps pseudo-bruts. Les dumps sont lus par ordre chronologique des ``run_ts``
    pour que la déduplication ``keep="last"`` conserve les données les plus récentes.

    :return: DataFrame Polars concaténé avec colonne ``run_id`` (le run_ts source).
    """
    tickers = list_tickers(instrument, settings)
    if not tickers:
        logger.warning(f"Aucun dump pseudo-brut trouvé pour {instrument.key} ({resolution})")
        return pl.DataFrame()

    frames: list[pl.DataFrame] = []

    for ticker in tickers:
        run_ts_list = list_runs(instrument, ticker, settings, resolution=resolution)
        for run_ts in run_ts_list:
            path = settings.raw_dump_path(instrument, ticker, run_ts, resolution=resolution)
            if not path.exists():
                continue
            df = read_parquet(path)
            df = df.with_columns(pl.lit(run_ts).alias("run_id"))
            # Assurer la présence des colonnes identité
            if "symbol" not in df.columns:
                df = df.with_columns(pl.lit(instrument.symbol).alias("symbol"))
            if "instrument_type" not in df.columns:
                df = df.with_columns(pl.lit(instrument.type.value).alias("instrument_type"))
            if "product_code" not in df.columns:
                df = df.with_columns(pl.lit(instrument.symbol).alias("product_code"))
            frames.append(df)

    if not frames:
        return pl.DataFrame()

    result = pl.concat(frames, how="diagonal_relaxed")
    logger.info(
        f"Lu {len(frames)} dump(s) pseudo-brut(s) pour {instrument.key} [{resolution}]: "
        f"{result.height} lignes au total"
    )
    return result


def has_run_today(
    instrument: Instrument,
    settings: Settings,
    resolution: str = DEFAULT_RESOLUTION,
    *,
    ticker: str | None = None,
) -> tuple[bool, str | None]:
    """Vérifie si une historisation a déjà été faite aujourd'hui.

    Si ``ticker`` est fourni, ne regarde que ce ticker (skip par segment futures).
    Sinon, vrai dès qu'**un** ticker du produit a un dump daté d'aujourd'hui
    (comportement mono-symbole stocks/forex/indices/yahoo).

    :return: Tuple (déjà_fait_aujourd'hui, run_ts_trouvé).
    """
    today_str = datetime.now(UTC).strftime("%Y%m%d")
    tickers = [ticker] if ticker is not None else list_tickers(instrument, settings)

    for t in tickers:
        for run_ts in list_runs(instrument, t, settings, resolution=resolution):
            if run_ts.startswith(today_str):
                return True, run_ts

    return False, None


def get_latest_run_date(
    instrument: Instrument,
    settings: Settings,
    resolution: str = DEFAULT_RESOLUTION,
) -> str | None:
    """Retourne la date (YYYYMMDD) du run le plus récent pour un instrument × résolution."""
    tickers = list_tickers(instrument, settings)
    if not tickers:
        return None

    all_runs: list[str] = []
    for ticker in tickers:
        all_runs.extend(list_runs(instrument, ticker, settings, resolution=resolution))

    if not all_runs:
        return None

    latest = sorted(all_runs)[-1]
    return latest[:8]


def raw_dumps_exist(
    instrument: Instrument,
    settings: Settings,
    resolution: str = DEFAULT_RESOLUTION,
) -> bool:
    """Vérifie s'il existe au moins un dump pseudo-brut pour un instrument × résolution."""
    for ticker in list_tickers(instrument, settings):
        if list_runs(instrument, ticker, settings, resolution=resolution):
            return True
    return False
