"""Lecture des calendriers historisés : séances prévues, fériés, clôtures anticipées.

Deux vues, une même interface pour ``doctor gaps`` (:class:`TradingCalendar`) :

- :class:`FuturesSchedule` : séances d'un produit futures reconstruites depuis
  les événements ``/futures/v1/schedules``. Un intervalle de trading va d'un
  ``open`` au ``pre_open`` ou ``close`` suivant de la même séance (``pre_open``
  en cours de séance = arrêt). On en dérive, par trade date : **férié** (jour
  ouvré sans séance), **clôture anticipée** (``close`` avant l'heure normale),
  **séance interrompue** (plusieurs intervalles, ex. arrêt 12:00→17:00 un lundi
  férié, rattaché à la séance du mardi). Une séance incomplète (événements
  orphelins en début d'historique) est un trou de connaissance, pas un férié.
- :class:`ExchangeHolidays` : fermetures et clôtures anticipées NYSE / NASDAQ
  (``/v1/marketstatus/upcoming``), pour stocks et indices. Seules les
  exceptions sont connues (pas les horaires normaux) ; la vue ne fait autorité
  que sur les fenêtres effectivement couvertes par des snapshots.

Le forex n'a pas de calendrier propre chez Massive : le calendrier actions US
ne dit rien des séances FX (24/5), il n'est donc pas utilisé pour expliquer
des trous forex.
"""

from __future__ import annotations

from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from functools import cached_property
from typing import Protocol
from zoneinfo import ZoneInfo

import polars as pl

from myquantstore.config import Settings
from myquantstore.instruments import Instrument, InstrumentType
from myquantstore.market_calendar.store import (
    CalendarSource,
    Window,
    known_windows,
    read_calendar,
    read_calendar_meta,
)

FUTURES_SESSION_TZ = "America/Chicago"
"""Fuseau des trade dates futures (une session Massive se termine à 17:00 CT)."""

EQUITIES_TZ = "America/New_York"
"""Fuseau des dates NYSE / NASDAQ."""

NO_SESSION = "férié"
EARLY_CLOSE = "clôture anticipée"
INTERRUPTED = "séance interrompue"
CLOSED = "fermé"
OUTSIDE_SESSION = "hors séance prévue"

_PREFERRED_EXCHANGES = ("NYSE", "NASDAQ")
_ONE_MINUTE = timedelta(minutes=1)

Interval = tuple[datetime, datetime]


class TradingCalendar(Protocol):
    """Ce dont ``doctor gaps`` a besoin pour expliquer un trou."""

    def covers(self, start: datetime, end: datetime) -> bool:
        """Le calendrier fait-il autorité sur tout ``[start, end)`` ?"""
        ...

    def open_minutes(self, start: datetime, end: datetime) -> int:
        """Minutes de séance prévues dans ``[start, end)``."""
        ...

    def closure_label(self, when: datetime) -> str:
        """Libellé de la fermeture prévue à l'instant ``when``."""
        ...


@dataclass(frozen=True)
class CalendarEvent:
    """Un jour hors norme : férié, fermeture, clôture anticipée ou séance interrompue."""

    day: date
    kinds: tuple[str, ...]
    intervals: tuple[Interval, ...] = ()
    """Séances prévues (UTC) : celles de la trade date, ou le trading du jour pour un férié."""
    name: str | None = None

    @property
    def label(self) -> str:
        base = ", ".join(self.kinds)
        return f"{base} ({self.name})" if self.name else base


@dataclass(frozen=True)
class HolidayRow:
    """Ligne de ``calendar holidays`` : un événement, regroupé sur les marchés identiques."""

    day: date
    markets: tuple[str, ...]
    kinds: tuple[str, ...]
    intervals: tuple[Interval, ...]
    name: str | None


class FuturesSchedule:
    """Séances d'un produit futures (événements agrégés de ``/futures/v1/schedules``)."""

    def __init__(
        self,
        product_code: str,
        events: pl.DataFrame,
        names: dict[date, str] | None = None,
    ) -> None:
        self.product_code = product_code
        self._names = names or {}
        self._tz = ZoneInfo(FUTURES_SESSION_TZ)
        self._sessions, self._incomplete = _build_sessions(events)
        days = sorted(self._sessions)
        self.coverage: Window | None = (days[0], days[-1]) if days else None
        self._intervals = sorted(iv for ivs in self._sessions.values() for iv in ivs)
        self._starts = [a for a, _ in self._intervals]
        closes = Counter(ivs[-1][1].astimezone(self._tz).time() for ivs in self._sessions.values())
        self.normal_close: time | None = closes.most_common(1)[0][0] if closes else None

    @property
    def session_count(self) -> int:
        """Nombre de séances complètes connues."""
        return len(self._sessions)

    def covers(self, start: datetime, end: datetime) -> bool:
        if self.coverage is None:
            return False
        first = start.astimezone(self._tz).date()
        # Une trade date D couvre la veille 17:00 → D 16:00 CT : la dernière trade
        # date touchée peut être le lendemain de la date locale de fin.
        last = (end - _ONE_MINUTE).astimezone(self._tz).date() + timedelta(days=1)
        if first < self.coverage[0] or last - timedelta(days=1) > self.coverage[1]:
            return False
        return not any(first <= d <= last for d in self._incomplete)

    def open_minutes(self, start: datetime, end: datetime) -> int:
        return _overlap_minutes(self._intervals, self._starts, start, end)

    def closure_label(self, when: datetime) -> str:
        event = self._events_by_day.get(when.astimezone(self._tz).date())
        return event.label if event else OUTSIDE_SESSION

    def events(self, start: date | None = None, end: date | None = None) -> list[CalendarEvent]:
        """Fériés, clôtures anticipées et séances interrompues, triés par trade date."""
        return [
            ev
            for day, ev in sorted(self._events_by_day.items())
            if (start is None or day >= start) and (end is None or day <= end)
        ]

    @cached_property
    def _events_by_day(self) -> dict[date, CalendarEvent]:
        events: dict[date, CalendarEvent] = {}
        for day, ivs in self._sessions.items():
            kinds: list[str] = []
            last_close = ivs[-1][1].astimezone(self._tz).time()
            if self.normal_close is not None and last_close < self.normal_close:
                kinds.append(EARLY_CLOSE)
            if len(ivs) > 1:
                kinds.append(INTERRUPTED)
            if kinds:
                events[day] = CalendarEvent(day, tuple(kinds), tuple(ivs), self._names.get(day))
        if self.coverage is not None:
            day = self.coverage[0]
            while day <= self.coverage[1]:
                if day.weekday() < 5 and day not in self._sessions and day not in self._incomplete:
                    events[day] = CalendarEvent(
                        day, (NO_SESSION,), self._trading_on(day), self._names.get(day)
                    )
                day += timedelta(days=1)
        return events

    def _trading_on(self, day: date) -> tuple[Interval, ...]:
        """Trading prévu pendant la journée locale ``day`` (ex. matin d'un lundi férié)."""
        lo = datetime.combine(day, time(0), tzinfo=self._tz).astimezone(UTC)
        hi = datetime.combine(day + timedelta(days=1), time(0), tzinfo=self._tz).astimezone(UTC)
        return tuple((max(a, lo), min(b, hi)) for a, b in self._intervals if a < hi and b > lo)


class ExchangeHolidays:
    """Fermetures et clôtures anticipées NYSE / NASDAQ (stocks, indices)."""

    def __init__(self, holidays: pl.DataFrame, windows: list[Window]) -> None:
        self._tz = ZoneInfo(EQUITIES_TZ)
        self._windows = windows
        self._days: dict[date, CalendarEvent] = {}
        closures: list[Interval] = []
        for row in _preferred_exchange_rows(holidays).iter_rows(named=True):
            day: date = row["date"]
            day_end = datetime.combine(day + timedelta(days=1), time(0), tzinfo=self._tz)
            if row["status"] == "early-close" and row["close"] is not None:
                ivs: tuple[Interval, ...] = ((row["open"], row["close"]),) if row["open"] else ()
                self._days[day] = CalendarEvent(day, (EARLY_CLOSE,), ivs, row["name"])
                closures.append((row["close"], day_end.astimezone(UTC)))
            else:
                self._days[day] = CalendarEvent(day, (CLOSED,), (), row["name"])
                day_start = datetime.combine(day, time(0), tzinfo=self._tz)
                closures.append((day_start.astimezone(UTC), day_end.astimezone(UTC)))
        self._closures = sorted(closures)
        self._starts = [a for a, _ in self._closures]

    def covers(self, start: datetime, end: datetime) -> bool:
        first = start.astimezone(self._tz).date()
        last = (end - _ONE_MINUTE).astimezone(self._tz).date()
        return any(lo <= first and last <= hi for lo, hi in self._windows)

    def open_minutes(self, start: datetime, end: datetime) -> int:
        closed = _overlap_minutes(self._closures, self._starts, start, end)
        return int((end - start) / _ONE_MINUTE) - closed

    def closure_label(self, when: datetime) -> str:
        event = self._days.get(when.astimezone(self._tz).date())
        return event.label if event else OUTSIDE_SESSION


def load_calendar(settings: Settings, instrument: Instrument) -> TradingCalendar | None:
    """Calendrier historisé applicable à un instrument, ou ``None`` (absent / forex / options)."""
    if instrument.type == InstrumentType.FUTURES:
        events = read_calendar(CalendarSource.futures_schedule(instrument.symbol), settings)
        if events.is_empty():
            return None
        return FuturesSchedule(instrument.symbol, events, holiday_names(settings))
    if instrument.type in (InstrumentType.STOCKS, InstrumentType.INDICES):
        source = CalendarSource.market_holidays()
        windows = known_windows(read_calendar_meta(source, settings))
        if not windows:
            return None
        return ExchangeHolidays(read_calendar(source, settings), windows)
    return None


def holiday_names(settings: Settings) -> dict[date, str]:
    """Nom des fériés connus par date (NYSE de préférence), pour nommer les fériés futures."""
    df = _preferred_exchange_rows(read_calendar(CalendarSource.market_holidays(), settings))
    return {row["date"]: row["name"] for row in df.iter_rows(named=True) if row["name"]}


def list_holidays(
    settings: Settings,
    *,
    futures: list[str],
    exchanges: bool,
    exchange_filter: str | None = None,
    start: date | None = None,
    end: date | None = None,
) -> list[HolidayRow]:
    """Lignes de ``calendar holidays`` : événements futures + fériés actions, regroupés.

    Deux produits (ou deux bourses) au calendrier identique un même jour ne
    donnent qu'une ligne (``markets`` = ``ES, NQ`` ou ``NYSE, NASDAQ``).
    """
    grouped: dict[tuple[object, ...], list[str]] = {}
    events: dict[tuple[object, ...], CalendarEvent] = {}

    def add(market: str, ev: CalendarEvent, family: str) -> None:
        key = (ev.day, family, ev.kinds, ev.intervals, ev.name)
        grouped.setdefault(key, []).append(market)
        events[key] = ev

    names = holiday_names(settings) if futures else {}
    for product in futures:
        events_df = read_calendar(CalendarSource.futures_schedule(product), settings)
        if events_df.is_empty():
            continue
        for ev in FuturesSchedule(product, events_df, names).events(start, end):
            add(f"futures:{product}", ev, "futures")
    if exchanges:
        df = read_calendar(CalendarSource.market_holidays(), settings)
        if exchange_filter:
            df = df.filter(pl.col("exchange").str.to_uppercase() == exchange_filter.upper())
        for row in df.iter_rows(named=True):
            day = row["date"]
            if (start is not None and day < start) or (end is not None and day > end):
                continue
            if row["status"] == "early-close":
                ivs: tuple[Interval, ...] = ((row["open"], row["close"]),) if row["open"] else ()
                ev = CalendarEvent(day, (EARLY_CLOSE,), ivs, row["name"])
            else:
                ev = CalendarEvent(day, (CLOSED,), (), row["name"])
            add(row["exchange"], ev, "exchanges")

    rows = [
        HolidayRow(ev.day, tuple(_compact_markets(grouped[key])), ev.kinds, ev.intervals, ev.name)
        for key, ev in events.items()
    ]
    return sorted(rows, key=lambda r: (r.day, r.markets))


def _compact_markets(markets: list[str]) -> list[str]:
    """``futures:ES, futures:NQ`` → ``futures:ES, NQ`` (lecture plus rapide)."""
    out: list[str] = []
    prefix = ""
    for m in markets:
        head, _, tail = m.partition(":")
        if tail and head == prefix:
            out.append(tail)
        else:
            out.append(m)
            prefix = head if tail else ""
    return out


def _build_sessions(events: pl.DataFrame) -> tuple[dict[date, list[Interval]], set[date]]:
    """Intervalles de trading par trade date + trade dates incomplètes.

    Complète = au moins un intervalle, chaque ``open`` refermé, dernier événement ``close``.
    """
    sessions: dict[date, list[Interval]] = {}
    incomplete: set[date] = set()
    if events.is_empty():
        return sessions, incomplete
    ordered = (
        events.select("session_end_date", "timestamp", "event")
        .unique()
        .sort("session_end_date", "timestamp")
    )
    for (day,), part in ordered.group_by(["session_end_date"], maintain_order=True):
        intervals: list[Interval] = []
        opened: datetime | None = None
        last_event = ""
        for ts, event in zip(part["timestamp"].to_list(), part["event"].to_list(), strict=True):
            last_event = event
            if event == "open":
                opened = opened or ts
            elif event in ("pre_open", "close") and opened is not None:
                intervals.append((opened, ts))
                opened = None
        if intervals and opened is None and last_event == "close":
            sessions[day] = intervals
        else:
            incomplete.add(day)
    return sessions, incomplete


def _preferred_exchange_rows(df: pl.DataFrame) -> pl.DataFrame:
    """Une ligne par date : NYSE, sinon NASDAQ, sinon la première bourse listée."""
    if df.is_empty():
        return df
    rank = (
        pl.when(pl.col("exchange") == _PREFERRED_EXCHANGES[0])
        .then(0)
        .when(pl.col("exchange") == _PREFERRED_EXCHANGES[1])
        .then(1)
        .otherwise(2)
    )
    return (
        df.with_columns(rank.alias("_rank"))
        .sort("date", "_rank")
        .unique(subset=["date"], keep="first", maintain_order=True)
        .drop("_rank")
    )


def _overlap_minutes(
    intervals: list[Interval], starts: list[datetime], start: datetime, end: datetime
) -> int:
    """Minutes de ``[start, end)`` couvertes par des intervalles triés (sans chevauchement)."""
    total = timedelta(0)
    idx = max(bisect_right(starts, start) - 1, 0)
    while idx < len(intervals) and intervals[idx][0] < end:
        a, b = intervals[idx]
        lo, hi = max(a, start), min(b, end)
        if hi > lo:
            total += hi - lo
        idx += 1
    return int(total / _ONE_MINUTE)
