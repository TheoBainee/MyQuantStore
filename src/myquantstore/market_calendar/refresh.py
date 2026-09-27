"""Orchestration de ``myquantstore calendar refresh`` : plages, fetch, dump, fusion.

- **Fériés** (stocks, forex, indices) : un seul snapshot partagé de
  ``/v1/marketstatus/upcoming`` par run, quel que soit le nombre de types
  demandés. Pas de rattrapage possible : l'API ne renvoie que le futur.
- **Schedules futures** (un appel paginé par produit) :

  1. premier run → backfill depuis ``today - history_months.futures`` ;
  2. runs suivants → depuis ``date du dernier run - overlap_buffer_days``,
     sans borne haute : les séances futures déjà connues sont re-téléchargées
     et font autorité (révisions de calendrier prises en compte) ;
  3. extension arrière si ``history_months.futures`` augmente (le premier
     début demandé est mémorisé : pas de re-téléchargement en boucle quand
     l'API ne remonte pas plus loin).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from myquantstore.api.client import MassiveClient
from myquantstore.api.market_calendar import (
    FUTURES_SCHEDULES_PATH,
    MARKET_HOLIDAYS_PATH,
    fetch_futures_schedules,
    fetch_market_holidays,
    futures_schedules_url,
)
from myquantstore.config import Settings, generate_run_ts
from myquantstore.instruments import Instrument, InstrumentType
from myquantstore.logging_setup import get_logger
from myquantstore.market_calendar.store import (
    MARKET_HOLIDAYS,
    CalendarSource,
    Window,
    apply_dump,
    read_calendar,
    read_calendar_meta,
    save_dump,
)

logger = get_logger("market_calendar")

HOLIDAY_TYPES = frozenset({InstrumentType.STOCKS, InstrumentType.FOREX, InstrumentType.INDICES})
"""Types servis par ``/v1/marketstatus/upcoming`` (calendrier actions US)."""


@dataclass(frozen=True)
class RefreshPlan:
    """Une requête à exécuter pour une source calendrier."""

    source: CalendarSource
    start: date | None
    """``session_end_date.gte`` (futures) ; ``None`` pour le snapshot des fériés."""
    reason: str

    @property
    def endpoint(self) -> str:
        return (
            MARKET_HOLIDAYS_PATH if self.source.kind == MARKET_HOLIDAYS else FUTURES_SCHEDULES_PATH
        )

    @property
    def url(self) -> str:
        if self.source.kind == MARKET_HOLIDAYS:
            return MARKET_HOLIDAYS_PATH
        return futures_schedules_url(self.source.product_code or "", self.start)


@dataclass
class RefreshResult:
    """Bilan d'une source après ``run_refresh``."""

    plan: RefreshPlan
    ok: bool
    fetched_rows: int = 0
    coverage: Window | None = None
    rows_before: int = 0
    rows_after: int = 0
    dump_path: Path | None = None
    error: str | None = None


def plan_refresh(
    settings: Settings, instruments: list[Instrument], today: date | None = None
) -> list[RefreshPlan]:
    """Plans pour ces instruments : un par produit futures, un seul pour les fériés.

    Les types sans calendrier Massive géré (options) sont ignorés : l'appelant
    les signale.
    """
    today = today or datetime.now(UTC).date()
    plans: list[RefreshPlan] = []
    if any(inst.type in HOLIDAY_TYPES for inst in instruments):
        plans.append(
            RefreshPlan(
                CalendarSource.market_holidays(), None, "snapshot à venir (stocks, forex, indices)"
            )
        )
    seen: set[str] = set()
    for inst in instruments:
        if inst.type == InstrumentType.FUTURES and inst.symbol not in seen:
            seen.add(inst.symbol)
            plans.append(plan_futures_schedule(settings, inst.symbol, today))
    return plans


def plan_futures_schedule(settings: Settings, product_code: str, today: date) -> RefreshPlan:
    """Plage à demander pour un produit : backfill, extension arrière ou incrémental."""
    source = CalendarSource.futures_schedule(product_code)
    # Même approximation (30 j/mois) que fetch_contracts_history.
    target_start = today - timedelta(days=30 * settings.history_months_for("futures"))
    meta = read_calendar_meta(source, settings)
    last_run = _run_date((meta or {}).get("last_run_ts"))
    if meta is None or last_run is None or not source.aggregate_path(settings).exists():
        return RefreshPlan(source, target_start, "backfill initial")
    requested_from = meta.get("requested_from")
    if requested_from is None or target_start < date.fromisoformat(requested_from):
        return RefreshPlan(source, target_start, "extension arrière (history_months)")
    return RefreshPlan(
        source, last_run - timedelta(days=settings.overlap_buffer_days), "incrémental"
    )


def run_refresh(
    plans: list[RefreshPlan],
    client: MassiveClient,
    settings: Settings,
    run_ts: str | None = None,
) -> list[RefreshResult]:
    """Exécute les plans : fetch → dump immuable → fusion dans l'agrégat.

    Une source en erreur n'arrête pas les autres (bilan ``ok=False``).
    """
    run_ts = run_ts or generate_run_ts()
    run_date = datetime.strptime(run_ts[:8], "%Y%m%d").date()
    results: list[RefreshResult] = []
    for plan in plans:
        before = read_calendar(plan.source, settings).height
        try:
            if plan.source.kind == MARKET_HOLIDAYS:
                df = fetch_market_holidays(client)
                coverage = holidays_coverage(df, run_date)
                pages = 1
            else:
                df = fetch_futures_schedules(client, plan.source.product_code or "", plan.start)
                coverage = schedule_coverage(df, plan.start)
                pages = client.page_count
            path = save_dump(
                plan.source,
                df,
                run_ts,
                settings,
                coverage=coverage,
                source_url=plan.url,
                page_count=pages,
                requested_start=plan.start,
            )
            merged = apply_dump(
                plan.source, df, coverage, run_ts, settings, requested_start=plan.start
            )
            results.append(
                RefreshResult(plan, True, df.height, coverage, before, merged.height, path)
            )
        except Exception as exc:  # une source KO ne bloque pas les autres (cf. historian)
            logger.error(f"Erreur calendar refresh {plan.source.label}: {exc}")
            results.append(
                RefreshResult(plan, False, rows_before=before, rows_after=before, error=str(exc))
            )
    return results


def holidays_coverage(df: pl.DataFrame, run_date: date) -> Window | None:
    """Autorité d'un snapshot de fériés : du lendemain du run au dernier férié listé.

    Le jour même est exclu (on ignore si l'API liste encore un férié du jour) :
    un férié passé n'est donc jamais effacé par un snapshot plus récent.
    """
    if df.is_empty():
        return None
    first = run_date + timedelta(days=1)
    last: date = df["date"].max()  # type: ignore[assignment]
    return (first, last) if last >= first else None


def schedule_coverage(df: pl.DataFrame, start: date | None) -> Window | None:
    """Autorité d'un dump schedules : du début demandé à la dernière séance reçue."""
    if df.is_empty():
        return None
    last: date = df["session_end_date"].max()  # type: ignore[assignment]
    first: date = start or df["session_end_date"].min()  # type: ignore[assignment]
    return first, last


def _run_date(run_ts: object) -> date | None:
    """Date (UTC) d'un ``run_ts`` ``YYYYMMDDTHHMMSS``, ou ``None`` si illisible."""
    if not isinstance(run_ts, str) or len(run_ts) < 8:
        return None
    try:
        return datetime.strptime(run_ts[:8], "%Y%m%d").date()
    except ValueError:
        return None
