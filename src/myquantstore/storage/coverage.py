"""Couverture OHLCV et santé de fraîcheur (lag / écart multi-résolution).

Utilisé par ``status`` et le résumé ``fetch`` pour signaler des agrégés
périmés (ex. dernière barre trop vieille) ou un écart 1min vs 1day.
Seuils en jours calendaires via ``Settings.health_*``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum

from myquantstore.config import Settings
from myquantstore.instruments import (
    RESOLUTION_1DAY,
    RESOLUTION_1MIN,
    Instrument,
)
from myquantstore.storage.aggregate_cache import aggregate_exists, read_aggregate
from myquantstore.tickers.yahoo_map import YAHOO_DAILY_TYPES


class HealthLevel(StrEnum):
    OK = "ok"
    WARN = "warn"
    STALE = "stale"


@dataclass(frozen=True, slots=True)
class ResolutionCoverage:
    """Couverture d'un agrégé pour une résolution donnée."""

    resolution: str
    present: bool
    rows: int | None = None
    min_date: date | None = None
    max_date: date | None = None
    lag_days: int | None = None

    def is_stale(self, settings: Settings) -> bool:
        if self.lag_days is None:
            return False
        return self.lag_days > settings.stale_lag_days_for(self.resolution)


@dataclass(frozen=True, slots=True)
class HealthIssue:
    level: HealthLevel
    code: str
    message: str
    resolution: str | None = None


@dataclass(slots=True)
class InstrumentHealth:
    instrument_key: str
    coverages: dict[str, ResolutionCoverage] = field(default_factory=dict)
    issues: list[HealthIssue] = field(default_factory=list)

    @property
    def worst_level(self) -> HealthLevel:
        if any(i.level == HealthLevel.STALE for i in self.issues):
            return HealthLevel.STALE
        if any(i.level == HealthLevel.WARN for i in self.issues):
            return HealthLevel.WARN
        return HealthLevel.OK

    @property
    def has_problems(self) -> bool:
        return self.worst_level != HealthLevel.OK

    def has_check_failures(self, *, strict_missing: bool = False) -> bool:
        """Problèmes pour ``status --check``.

        Par défaut : STALE seulement (instrument neuf / agrégé absent = OK).
        ``strict_missing=True`` : aussi missing_aggregate et cross_resolution_lag
        (cron ``schedule run``).
        """
        for issue in self.issues:
            if issue.level == HealthLevel.STALE:
                return True
            if strict_missing and issue.code in {
                "missing_aggregate",
                "cross_resolution_lag",
            }:
                return True
        return False


def _to_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def get_aggregate_date_range(
    instrument: Instrument,
    settings: Settings,
    resolution: str,
) -> tuple[date | None, date | None]:
    """Retourne ``(oldest, latest)`` depuis l'agrégé, ou ``(None, None)``."""
    if not aggregate_exists(instrument, settings, resolution=resolution):
        return None, None
    try:
        agg = read_aggregate(instrument, settings, resolution=resolution)
    except FileNotFoundError:
        return None, None
    if agg.is_empty() or "window_start" not in agg.columns:
        return None, None
    return _to_date(agg["window_start"].min()), _to_date(agg["window_start"].max())


def get_resolution_coverage(
    instrument: Instrument,
    settings: Settings,
    resolution: str,
    *,
    today: date | None = None,
) -> ResolutionCoverage:
    """Couverture d'une résolution (min/max/lag calendaire)."""
    ref = today or datetime.now(UTC).date()
    if not aggregate_exists(instrument, settings, resolution=resolution):
        return ResolutionCoverage(resolution=resolution, present=False)

    try:
        agg = read_aggregate(instrument, settings, resolution=resolution)
    except FileNotFoundError:
        return ResolutionCoverage(resolution=resolution, present=False)

    if agg.is_empty() or "window_start" not in agg.columns:
        return ResolutionCoverage(resolution=resolution, present=True, rows=agg.height)

    min_d = _to_date(agg["window_start"].min())
    max_d = _to_date(agg["window_start"].max())
    lag = (ref - max_d).days if max_d is not None else None
    return ResolutionCoverage(
        resolution=resolution,
        present=True,
        rows=agg.height,
        min_date=min_d,
        max_date=max_d,
        lag_days=lag,
    )


def applicable_resolutions(instrument: Instrument) -> list[str]:
    """Résolutions de stockage attendues pour cet instrument."""
    res = [RESOLUTION_1MIN]
    if instrument.type in YAHOO_DAILY_TYPES:
        res.append(RESOLUTION_1DAY)
    return res


def assess_instrument_health(
    instrument: Instrument,
    settings: Settings,
    *,
    today: date | None = None,
    resolutions: list[str] | None = None,
) -> InstrumentHealth:
    """Évalue la fraîcheur OHLCV d'un instrument (lag + écart multi-résolution)."""
    ref = today or datetime.now(UTC).date()
    res_list = resolutions if resolutions is not None else applicable_resolutions(instrument)
    health = InstrumentHealth(instrument_key=instrument.key)

    for res in res_list:
        cov = get_resolution_coverage(instrument, settings, res, today=ref)
        health.coverages[res] = cov
        if not cov.present:
            health.issues.append(
                HealthIssue(
                    level=HealthLevel.WARN,
                    code="missing_aggregate",
                    message=f"Agrégé [{res}] absent",
                    resolution=res,
                )
            )
            continue
        if cov.is_stale(settings):
            thr = settings.stale_lag_days_for(res)
            health.issues.append(
                HealthIssue(
                    level=HealthLevel.STALE,
                    code="stale",
                    message=(
                        f"Agrégé [{res}] périmé (lag={cov.lag_days}j > {thr}j, "
                        f"latest={cov.max_date})"
                    ),
                    resolution=res,
                )
            )

    c1 = health.coverages.get(RESOLUTION_1MIN)
    c2 = health.coverages.get(RESOLUTION_1DAY)
    if (
        c1 is not None
        and c2 is not None
        and c1.present
        and c2.present
        and c1.lag_days is not None
        and c2.lag_days is not None
    ):
        delta = abs(c1.lag_days - c2.lag_days)
        thr = settings.health_cross_resolution_lag_days
        if delta > thr:
            health.issues.append(
                HealthIssue(
                    level=HealthLevel.WARN,
                    code="cross_resolution_lag",
                    message=(
                        f"Écart 1min/1day : lag 1min={c1.lag_days}j vs "
                        f"1day={c2.lag_days}j (Δ={delta}j > {thr})"
                    ),
                )
            )

    return health


def attach_coverage_fields(
    result: dict[str, object],
    instrument: Instrument,
    settings: Settings,
    resolution: str,
    *,
    today: date | None = None,
) -> None:
    """Ajoute oldest/latest/lag_days/stale au dict résultat d'un fetch."""
    cov = get_resolution_coverage(instrument, settings, resolution, today=today)
    if cov.min_date is not None:
        result["oldest"] = cov.min_date.isoformat()
    if cov.max_date is not None:
        result["latest"] = cov.max_date.isoformat()
    if cov.lag_days is not None:
        result["lag_days"] = cov.lag_days
    result["stale"] = cov.is_stale(settings)
