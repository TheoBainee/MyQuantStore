"""Stockage historisé des calendriers de marché : dumps immuables + agrégats.

Layout ::

    {data_dir}/raw/calendar/
    ├─ market_holidays/{run_ts}.parquet            (+ .meta.json)
    └─ futures_schedules/{product_code}/{run_ts}.parquet (+ .meta.json)
    {data_dir}/aggregate/calendar/
    ├─ market_holidays.parquet                     (+ .meta.json)
    └─ futures_schedules/{product_code}.parquet    (+ .meta.json)

**Données historisées, pas un cache.** Un férié passé disparaît de
``/v1/marketstatus/upcoming`` et la fenêtre d'historique des schedules futures
avance avec le temps : une fois sortie de l'API, la donnée n'existe plus que
chez nous. D'où ``data_dir`` (pas ``cache_dir``) et la même contrainte que
l'OHLCV : l'agrégat se reconstruit toujours depuis les dumps (:func:`rebuild`).

**Fusion par fenêtre d'autorité.** Chaque dump enregistre dans son sidecar la
plage de dates qu'il couvre (``coverage_start`` / ``coverage_end``). À la
fusion, les lignes existantes de cette plage sont remplacées par celles du dump
(un férié futur ajouté, modifié ou supprimé est pris en compte ; un jour
futures sans séance dans la plage est bien un jour sans séance) ; hors de la
plage, rien n'est jamais effacé.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from myquantstore.api.market_calendar import (
    FUTURES_SCHEDULES_SCHEMA,
    MARKET_HOLIDAYS_SCHEMA,
)
from myquantstore.config import Settings
from myquantstore.logging_setup import get_logger
from myquantstore.storage.parquet_io import read_meta, read_parquet, write_parquet

logger = get_logger("market_calendar")

CALENDAR_SUBDIR = "calendar"
MARKET_HOLIDAYS = "market_holidays"
FUTURES_SCHEDULES = "futures_schedules"

Window = tuple[date, date]


@dataclass(frozen=True)
class CalendarSource:
    """Un jeu de données calendrier historisé : les fériés, ou les schedules d'un produit."""

    kind: str
    """:data:`MARKET_HOLIDAYS` ou :data:`FUTURES_SCHEDULES`."""

    product_code: str | None = None
    """Produit futures (``ES``…) pour :data:`FUTURES_SCHEDULES`, sinon ``None``."""

    @classmethod
    def market_holidays(cls) -> CalendarSource:
        return cls(MARKET_HOLIDAYS)

    @classmethod
    def futures_schedule(cls, product_code: str) -> CalendarSource:
        return cls(FUTURES_SCHEDULES, product_code)

    @property
    def label(self) -> str:
        """``market_holidays`` ou ``futures_schedules:ES`` (affichage, logs)."""
        return f"{self.kind}:{self.product_code}" if self.product_code else self.kind

    @property
    def date_column(self) -> str:
        """Colonne date sur laquelle porte la fenêtre d'autorité."""
        return "date" if self.kind == MARKET_HOLIDAYS else "session_end_date"

    @property
    def key(self) -> list[str]:
        """Clé naturelle d'une ligne (déduplication keep=last)."""
        if self.kind == MARKET_HOLIDAYS:
            return ["date", "exchange"]
        return ["product_code", "session_end_date", "event", "timestamp"]

    @property
    def sort_columns(self) -> list[str]:
        """Ordre de l'agrégat (chronologique au sein d'une séance pour les schedules)."""
        if self.kind == MARKET_HOLIDAYS:
            return ["date", "exchange"]
        return ["product_code", "session_end_date", "timestamp", "event"]

    @property
    def schema(self) -> dict[str, pl.DataType]:
        return MARKET_HOLIDAYS_SCHEMA if self.kind == MARKET_HOLIDAYS else FUTURES_SCHEDULES_SCHEMA

    def raw_dir(self, settings: Settings) -> Path:
        base = settings.raw_dumps_dir() / CALENDAR_SUBDIR / self.kind
        return base / self.product_code if self.product_code else base

    def aggregate_path(self, settings: Settings) -> Path:
        base = settings.aggregate_dir() / CALENDAR_SUBDIR
        if self.product_code:
            return base / self.kind / f"{self.product_code}.parquet"
        return base / f"{self.kind}.parquet"


def save_dump(
    source: CalendarSource,
    df: pl.DataFrame,
    run_ts: str,
    settings: Settings,
    *,
    coverage: Window | None,
    source_url: str,
    page_count: int = 0,
    requested_start: date | None = None,
) -> Path:
    """Écrit un dump immuable ``{raw_dir}/{run_ts}.parquet`` + sidecar (fenêtre couverte)."""
    path = source.raw_dir(settings) / f"{run_ts}.parquet"
    write_parquet(
        df,
        path,
        calendar=source.kind,
        product_code=source.product_code,
        run_ts=run_ts,
        source="massive",
        source_url=source_url,
        page_count=page_count,
        requested_start=requested_start.isoformat() if requested_start else None,
        coverage_start=coverage[0].isoformat() if coverage else None,
        coverage_end=coverage[1].isoformat() if coverage else None,
    )
    logger.info(f"Dump calendrier {source.label}: {path} ({df.height} ligne(s))")
    return path


def list_dumps(source: CalendarSource, settings: Settings) -> list[Path]:
    """Dumps d'une source, triés par ``run_ts`` (ordre chronologique)."""
    raw_dir = source.raw_dir(settings)
    if not raw_dir.is_dir():
        return []
    return sorted(raw_dir.glob("*.parquet"), key=lambda p: p.stem)


def read_calendar(source: CalendarSource, settings: Settings) -> pl.DataFrame:
    """Agrégat d'une source (DataFrame vide au bon schéma s'il n'existe pas)."""
    path = source.aggregate_path(settings)
    if not path.exists():
        return pl.DataFrame(schema=source.schema)
    return read_parquet(path)


def read_calendar_meta(source: CalendarSource, settings: Settings) -> dict[str, Any] | None:
    """Sidecar de l'agrégat, ou ``None`` s'il n'existe pas."""
    return read_meta(source.aggregate_path(settings))


def dump_coverage(meta: dict[str, Any] | None) -> Window | None:
    """Fenêtre d'autorité enregistrée dans le sidecar d'un dump."""
    if not meta or not meta.get("coverage_start") or not meta.get("coverage_end"):
        return None
    return date.fromisoformat(meta["coverage_start"]), date.fromisoformat(meta["coverage_end"])


def merge_dump(
    existing: pl.DataFrame,
    dump: pl.DataFrame,
    source: CalendarSource,
    coverage: Window | None,
) -> pl.DataFrame:
    """Fusionne un dump dans l'agrégat : le dump fait autorité sur ``coverage``.

    Les lignes existantes dont la date tombe dans ``coverage`` sont retirées,
    puis dump et existant sont concaténés et dédupliqués sur la clé naturelle
    (keep=last → le dump gagne). ``coverage=None`` : ajout sans suppression.
    """
    if coverage is not None and not existing.is_empty():
        lo, hi = coverage
        existing = existing.filter(~pl.col(source.date_column).is_between(lo, hi, closed="both"))
    parts = [f for f in (existing, dump) if not f.is_empty()]
    if not parts:
        return pl.DataFrame(schema=source.schema)
    combined = pl.concat(parts, how="diagonal_relaxed") if len(parts) > 1 else parts[0]
    if source.kind == FUTURES_SCHEDULES:
        combined = _drop_combo_duplicates(combined)
    return combined.unique(subset=source.key, keep="last", maintain_order=True).sort(
        source.sort_columns
    )


def apply_dump(
    source: CalendarSource,
    dump: pl.DataFrame,
    coverage: Window | None,
    run_ts: str,
    settings: Settings,
    *,
    requested_start: date | None = None,
) -> pl.DataFrame:
    """Fusionne un dump dans l'agrégat sur disque et réécrit l'agrégat + son sidecar."""
    previous = read_calendar_meta(source, settings) or {}
    merged = merge_dump(read_calendar(source, settings), dump, source, coverage)
    _write_aggregate(source, merged, settings, run_ts, previous, coverage, requested_start)
    return merged


def rebuild(source: CalendarSource, settings: Settings, *, write: bool = False) -> pl.DataFrame:
    """Reconstruit l'agrégat en rejouant tous les dumps dans l'ordre des ``run_ts``.

    Garantit l'invariant d'historisation : l'agrégat ne contient rien que les
    dumps ne permettent de retrouver. ``write=True`` réécrit l'agrégat sur disque.
    """
    merged = pl.DataFrame(schema=source.schema)
    meta: dict[str, Any] = {}
    for path in list_dumps(source, settings):
        dump_meta = read_meta(path)
        coverage = dump_coverage(dump_meta)
        merged = merge_dump(merged, read_parquet(path), source, coverage)
        if write:
            requested = (dump_meta or {}).get("requested_start")
            meta = _aggregate_meta(
                source,
                merged,
                path.stem,
                meta,
                coverage,
                date.fromisoformat(requested) if requested else None,
            )
    if write:
        write_parquet(merged, source.aggregate_path(settings), **meta)
    return merged


def known_windows(meta: dict[str, Any] | None) -> list[Window]:
    """Union des fenêtres d'autorité des dumps (sidecar d'agrégat), triée."""
    if not meta:
        return []
    return [
        (date.fromisoformat(a), date.fromisoformat(b)) for a, b in meta.get("known_windows", [])
    ]


def _write_aggregate(
    source: CalendarSource,
    merged: pl.DataFrame,
    settings: Settings,
    run_ts: str,
    previous: dict[str, Any],
    coverage: Window | None,
    requested_start: date | None,
) -> None:
    meta = _aggregate_meta(source, merged, run_ts, previous, coverage, requested_start)
    path = write_parquet(merged, source.aggregate_path(settings), **meta)
    logger.info(f"Agrégat calendrier {source.label}: {path} ({merged.height} ligne(s))")


def _aggregate_meta(
    source: CalendarSource,
    merged: pl.DataFrame,
    run_ts: str,
    previous: dict[str, Any],
    coverage: Window | None,
    requested_start: date | None,
) -> dict[str, Any]:
    """Sidecar d'agrégat : dernier run, plage des données, fenêtres connues, 1er début demandé."""
    windows = known_windows(previous)
    if coverage is not None:
        windows = _union_windows([*windows, coverage])
    requested_from = previous.get("requested_from")
    if requested_start is not None and (
        requested_from is None or requested_start < date.fromisoformat(requested_from)
    ):
        requested_from = requested_start.isoformat()
    col = source.date_column
    data_min = merged[col].min() if not merged.is_empty() else None
    data_max = merged[col].max() if not merged.is_empty() else None
    return {
        "calendar": source.kind,
        "product_code": source.product_code,
        "source": "massive",
        "last_run_ts": run_ts,
        "last_refreshed_at": datetime.now(UTC).isoformat(),
        "data_start": str(data_min) if data_min is not None else None,
        "data_end": str(data_max) if data_max is not None else None,
        "known_windows": [[a.isoformat(), b.isoformat()] for a, b in windows],
        "requested_from": requested_from,
    }


def _union_windows(windows: list[Window]) -> list[Window]:
    """Fusionne des plages de dates qui se chevauchent ou se touchent."""
    out: list[Window] = []
    for lo, hi in sorted(windows):
        if out and lo.toordinal() <= out[-1][1].toordinal() + 1:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


_COMBO_PATTERN = r"(?i)spread|butterfly|condor|strip|bundle|pack"
"""Noms de produits combinés renvoyés sous le même ``product_code`` que l'outright."""


def _drop_combo_duplicates(df: pl.DataFrame) -> pl.DataFrame:
    """Garde le produit outright quand des combos du même code sont présents.

    ``product_code=ES`` renvoie aussi « ES Equity Calendar Spread », et ``YM``
    aussi « YM Butterfly » (horaires identiques constatés) : par séance, si des
    lignes outright existent, on écarte celles des combos pour que le calendrier
    soit celui du contrat.
    """
    if df.is_empty() or "product_name" not in df.columns:
        return df
    combo = pl.col("product_name").fill_null("").str.contains(_COMBO_PATTERN)
    has_outright = (~pl.col("_combo")).any().over(["product_code", "session_end_date"])
    return (
        df.with_columns(combo.alias("_combo"))
        .filter(~(pl.col("_combo") & has_outright))
        .drop("_combo")
    )
