"""Audit des trous de données 1min (``myquantstore doctor gaps``).

Lecture seule : on ne modifie ni ne fabrique aucune donnée. Pour un instrument,
on repère, dans une **plage horaire intraday** (heures murales d'un fuseau IANA),
les minutes sans chandelier au-delà d'un seuil (``min_gap_minutes``).

**Sessions** : une session = une occurrence de la plage horaire.

- Plage normale (``begin < end``, ex: 07:00-15:00) : session = date locale.
- Plage wrap-around (``begin > end``, ex: 17:00-04:00) : la session porte la date
  du soir où elle commence (une barre à 02:00 appartient à la session de la veille).

Seules les sessions contenant **au moins une barre** sont auditées : une session
entièrement vide (férié, week-end) n'est pas un trou mais est listée à part
(``empty_sessions``, jours attendus : lun-ven en plage normale, dim-jeu en
wrap-around). Le bord de début de la toute première session et le bord de fin de
la toute dernière session ne sont pas signalés (début d'historique / fetch en
cours de séance).

**Confirmation croisée** : un trou est *confirmé* si au moins un autre
instrument (même type) a des barres pendant ce créneau. Un férié ou une clôture
anticipée touchent tous les instruments → trou non confirmé ; une panne de flux
sur un seul produit → trou confirmé (le signal vraiment problématique).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import polars as pl

from myquantstore.query.timezone import ensure_window_start_utc

_ONE_MINUTE = timedelta(minutes=1)


@dataclass
class Gap:
    """Un trou détecté dans une session."""

    session: date
    """Date de la session (date du début de plage)."""

    start: datetime
    """Première minute manquante (UTC aware)."""

    end: datetime
    """Fin du trou, exclusive (UTC aware) : barre suivante ou fin de plage."""

    position: str
    """``début`` / ``milieu`` / ``fin`` de plage."""

    ticker: str = ""
    """Contrat(s) des barres adjacentes (informatif)."""

    confirmed_by: list[str] = field(default_factory=list)
    """Autres instruments ayant des barres pendant le trou."""

    @property
    def minutes(self) -> int:
        """Durée du trou en minutes."""
        return int((self.end - self.start) / _ONE_MINUTE)


@dataclass
class GapReport:
    """Résultat d'audit pour un instrument."""

    instrument_key: str
    sessions_checked: int
    gaps: list[Gap]
    empty_sessions: list[date]


def unique_timestamps(df: pl.DataFrame) -> pl.Series:
    """Timestamps UTC uniques et triés (une barre par ``window_start``)."""
    if df.is_empty() or "window_start" not in df.columns:
        return pl.Series("window_start", [], dtype=pl.Datetime("ns", "UTC"))
    ws = ensure_window_start_utc(df.select("window_start"))["window_start"]
    return ws.unique().sort()


def find_gaps(
    df: pl.DataFrame,
    *,
    instrument_key: str,
    intraday_begin: time,
    intraday_end: time,
    timezone: str,
    min_gap_minutes: int,
    start: date | None = None,
    end: date | None = None,
) -> GapReport:
    """Détecte les trous d'un DataFrame 1min dans la plage horaire donnée.

    :param df: Chandeliers (colonne ``window_start``, ``ticker`` optionnelle).
    :param instrument_key: Clé instrument (ex: ``futures:NQ``), pour le rapport.
    :param intraday_begin: Début de plage (heure murale dans ``timezone``).
    :param intraday_end: Fin de plage, exclusive (heure murale dans ``timezone``).
    :param timezone: Fuseau IANA de la plage.
    :param min_gap_minutes: Durée minimale d'un trou signalé.
    :param start: Première session auditée (incluse), ou None.
    :param end: Dernière session auditée (incluse), ou None.
    :raises ValueError: Si ``intraday_begin == intraday_end`` ou seuil < 1.
    """
    if intraday_begin == intraday_end:
        raise ValueError("intraday_begin et intraday_end doivent différer")
    if min_gap_minutes < 1:
        raise ValueError("min_gap_minutes doit être >= 1")

    wrap = intraday_begin > intraday_end
    empty = GapReport(instrument_key, 0, [], [])
    if df.is_empty() or "window_start" not in df.columns:
        return empty

    work = ensure_window_start_utc(df)
    has_ticker = "ticker" in work.columns
    cols = [pl.col("window_start")]
    if has_ticker:
        cols.append(pl.col("ticker").cast(pl.Utf8))
    else:
        cols.append(pl.lit("").alias("ticker"))
    work = work.select(cols).unique(subset=["window_start"], keep="first")

    local = pl.col("window_start").dt.convert_time_zone(timezone)
    tod = local.dt.time()
    in_window = (
        (tod >= intraday_begin) | (tod < intraday_end)
        if wrap
        else (tod >= intraday_begin) & (tod < intraday_end)
    )
    session = local.dt.date()
    if wrap:
        session = pl.when(tod < intraday_end).then(session - pl.duration(days=1)).otherwise(session)
    work = (
        work.with_columns(session.alias("session"), in_window.alias("_in"))
        .filter(pl.col("_in"))
        .drop("_in")
    )
    if start is not None:
        work = work.filter(pl.col("session") >= start)
    if end is not None:
        work = work.filter(pl.col("session") <= end)
    if work.is_empty():
        return empty

    work = work.sort("window_start")
    min_gap = timedelta(minutes=min_gap_minutes)
    tz = ZoneInfo(timezone)
    sessions = work["session"].unique().sort().to_list()
    first_session, last_session = sessions[0], sessions[-1]

    gaps: list[Gap] = []

    # Trous internes : écart > 1 min entre deux barres consécutives d'une session
    inner = work.with_columns(
        pl.col("window_start").shift(1).over("session").alias("_prev"),
        pl.col("ticker").shift(1).over("session").alias("_prev_ticker"),
    ).filter(
        pl.col("_prev").is_not_null()
        & ((pl.col("window_start") - pl.col("_prev")) >= pl.lit(min_gap + _ONE_MINUTE))
    )
    for row in inner.iter_rows(named=True):
        gaps.append(
            Gap(
                session=row["session"],
                start=row["_prev"] + _ONE_MINUTE,
                end=row["window_start"],
                position="milieu",
                ticker=_ticker_label(row["_prev_ticker"], row["ticker"]),
            )
        )

    # Bords de plage : première barre en retard / dernière barre en avance
    bounds = work.group_by("session").agg(
        pl.col("window_start").first().alias("_first"),
        pl.col("ticker").first().alias("_first_ticker"),
        pl.col("window_start").last().alias("_last"),
        pl.col("ticker").last().alias("_last_ticker"),
    )
    for row in bounds.iter_rows(named=True):
        sess: date = row["session"]
        win_start, win_end = _session_bounds(sess, intraday_begin, intraday_end, wrap, tz)
        if sess != first_session and row["_first"] - win_start >= min_gap:
            gaps.append(
                Gap(sess, win_start, row["_first"], "début", ticker=row["_first_ticker"] or "")
            )
        last_end = row["_last"] + _ONE_MINUTE
        if sess != last_session and win_end - last_end >= min_gap:
            gaps.append(Gap(sess, last_end, win_end, "fin", ticker=row["_last_ticker"] or ""))

    gaps.sort(key=lambda g: g.start)
    return GapReport(
        instrument_key=instrument_key,
        sessions_checked=len(sessions),
        gaps=gaps,
        empty_sessions=_empty_sessions(set(sessions), first_session, last_session, wrap),
    )


def confirm_gaps(report: GapReport, peers: dict[str, pl.Series]) -> None:
    """Renseigne ``Gap.confirmed_by`` à partir des timestamps d'autres instruments.

    :param report: Rapport à compléter (modifié en place).
    :param peers: ``{instrument_key: timestamps UTC triés}`` (cf. :func:`unique_timestamps`).
    """
    for gap in report.gaps:
        confirmed: list[str] = []
        for key, ts in peers.items():
            if key == report.instrument_key or ts.is_empty():
                continue
            idx = int(ts.search_sorted(gap.start, side="left"))
            if idx < ts.len() and ts[idx] < gap.end:
                confirmed.append(key)
        gap.confirmed_by = confirmed


def _session_bounds(
    sess: date, begin: time, end: time, wrap: bool, tz: ZoneInfo
) -> tuple[datetime, datetime]:
    """Bornes UTC ``[début, fin)`` d'une session (DST géré par ``zoneinfo``)."""
    end_day = sess + timedelta(days=1) if wrap else sess
    start_dt = datetime.combine(sess, begin, tzinfo=tz).astimezone(UTC)
    end_dt = datetime.combine(end_day, end, tzinfo=tz).astimezone(UTC)
    return start_dt, end_dt


def _empty_sessions(present: set[date], first: date, last: date, wrap: bool) -> list[date]:
    """Sessions attendues sans aucune barre (lun-ven, ou dim-jeu en wrap-around)."""
    expected_weekdays = {6, 0, 1, 2, 3} if wrap else {0, 1, 2, 3, 4}
    out: list[date] = []
    day = first
    while day <= last:
        if day.weekday() in expected_weekdays and day not in present:
            out.append(day)
        day += timedelta(days=1)
    return out


def _ticker_label(before: str | None, after: str | None) -> str:
    """Contrat(s) autour d'un trou : ``YMU6`` ou ``YMU6/YMZ6`` au roll."""
    b, a = before or "", after or ""
    if not b or b == a:
        return a
    if not a:
        return b
    return f"{b}/{a}"
