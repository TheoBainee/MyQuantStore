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

**Calendrier de marché** (:func:`explain_gaps`, données de ``calendar refresh``) :
un trou tombant hors séance prévue (férié, arrêt, clôture anticipée) est
*attendu* et n'est jamais compté, même confirmé (deux produits peuvent avoir
des calendriers différents). Les sessions vides sont réparties entre fermées
selon le calendrier et sans barre alors qu'une séance était prévue.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import polars as pl

from myquantstore.query.timezone import ensure_window_start_utc

if TYPE_CHECKING:
    from myquantstore.market_calendar.views import TradingCalendar

_ONE_MINUTE = timedelta(minutes=1)
_UTC = "UTC"
_TS = pl.Datetime("ns", _UTC)
_ONE_MINUTE_NS = pl.duration(minutes=1, time_unit="ns")


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

    calendar_note: str = ""
    """Fermeture prévue par le calendrier (non vide = trou attendu, jamais compté)."""

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
    closed_sessions: dict[date, str] = field(default_factory=dict)
    """Sessions vides fermées selon le calendrier → libellé (rempli par :func:`explain_gaps`)."""
    expected_empty_sessions: list[date] = field(default_factory=list)
    """Sessions vides alors que le calendrier prévoyait une séance dans la plage."""


def unique_timestamps(df: pl.DataFrame) -> pl.Series:
    """Timestamps UTC uniques et triés (une barre par ``window_start``)."""
    if df.is_empty() or "window_start" not in df.columns:
        return pl.Series("window_start", [], dtype=_TS)
    ws = ensure_window_start_utc(df.select("window_start"))["window_start"]
    return ws.unique().sort()


def audit_gaps(
    frames: Mapping[str, pl.LazyFrame | pl.DataFrame],
    targets: Mapping[str, str],
    peers: Mapping[str, Sequence[str]],
    *,
    intraday_begin: time,
    intraday_end: time,
    min_gap_minutes: int,
    start: date | None = None,
    end: date | None = None,
) -> dict[str, GapReport]:
    """Détecte et confirme les trous de plusieurs instruments en un seul ``collect_all``.

    Tout est construit en lazy (projection ``window_start`` / ``ticker``, filtre de
    période poussé à la lecture) : détection vectorisée par instrument, puis
    confirmation croisée de **tous** les trous en une requête (``join_asof`` avant
    sur les timestamps des pairs), enfin un unique :func:`polars.collect_all`.

    :param frames: ``{instrument_key: chandeliers}`` (LazyFrame, ex. ``scan_parquet``,
        ou DataFrame) — cibles **et** pairs.
    :param targets: ``{instrument_key: fuseau IANA}`` des instruments audités.
    :param peers: ``{instrument_key: clés des pairs}`` (ordre = ordre d'affichage ;
        la clé elle-même est ignorée si présente).
    :param intraday_begin: Début de plage (heure murale).
    :param intraday_end: Fin de plage, exclusive (heure murale).
    :param min_gap_minutes: Durée minimale d'un trou signalé.
    :param start: Première session auditée (incluse), ou None.
    :param end: Dernière session auditée (incluse), ou None.
    :raises ValueError: Si ``intraday_begin == intraday_end`` ou seuil < 1.
    """
    if intraday_begin == intraday_end:
        raise ValueError("intraday_begin et intraday_end doivent différer")
    if min_gap_minutes < 1:
        raise ValueError("min_gap_minutes doit être >= 1")
    if not targets:
        return {}

    # Phase 1 (lazy → mémoire) : barres de la plage par session (un seul passage fuseau
    # horaire) pour chaque cible + timestamps triés de chaque pair, en un collect_all.
    prepared = {key: _prepare(frame, start, end) for key, frame in frames.items()}
    peer_keys = list(
        dict.fromkeys(
            p for key in targets for p in peers.get(key, ()) if p != key and p in prepared
        )
    )
    phase1 = pl.collect_all(
        [
            *(
                _session_bars(
                    prepared[key],
                    intraday_begin=intraday_begin,
                    intraday_end=intraday_end,
                    timezone=timezone,
                    start=start,
                    end=end,
                )
                for key, timezone in targets.items()
            ),
            *(prepared[p].select(pl.col("window_start").unique().sort()) for p in peer_keys),
        ]
    )
    bars = dict(zip(targets, phase1[: len(targets)], strict=True))
    stamps = {p: df.lazy() for p, df in zip(peer_keys, phase1[len(targets) :], strict=True)}

    # Phase 2 (lazy sur les frames en mémoire) : trous de toutes les cibles puis
    # confirmation croisée de tous les trous, en un collect_all.
    all_gaps = pl.concat(
        [
            _gap_frame(
                bars[key].lazy(),
                instrument_key=key,
                intraday_begin=intraday_begin,
                intraday_end=intraday_end,
                timezone=timezone,
                min_gap_minutes=min_gap_minutes,
            )
            for key, timezone in targets.items()
        ]
    ).with_row_index("_idx")
    confirmations = _confirmations(all_gaps, peers, stamps)
    final = all_gaps.join(confirmations, on="_idx", how="left").sort("_idx")
    collected = pl.collect_all([final])
    sessions_dfs = [bars[key].select(pl.col("session").unique().sort()) for key in targets]

    gaps_df = collected[0]
    by_key = gaps_df.partition_by("instrument", as_dict=True, maintain_order=True)
    wrap = intraday_begin > intraday_end
    reports: dict[str, GapReport] = {}
    for key, sessions_df in zip(targets, sessions_dfs, strict=True):
        sessions: list[date] = sessions_df["session"].to_list()
        part = by_key.get((key,))
        gaps = [
            Gap(
                session=row["session"],
                start=row["start"],
                end=row["end"],
                position=row["position"],
                ticker=row["ticker"],
                confirmed_by=list(row["confirmed_by"] or []),
            )
            for row in (part.iter_rows(named=True) if part is not None else [])
        ]
        reports[key] = GapReport(
            instrument_key=key,
            sessions_checked=len(sessions),
            gaps=gaps,
            empty_sessions=(
                _empty_sessions(set(sessions), sessions[0], sessions[-1], wrap) if sessions else []
            ),
        )
    return reports


def find_gaps(
    df: pl.DataFrame | pl.LazyFrame,
    *,
    instrument_key: str,
    intraday_begin: time,
    intraday_end: time,
    timezone: str,
    min_gap_minutes: int,
    start: date | None = None,
    end: date | None = None,
) -> GapReport:
    """Détecte les trous d'un instrument (sans confirmation) — cf. :func:`audit_gaps`.

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
    return audit_gaps(
        {instrument_key: df},
        {instrument_key: timezone},
        {instrument_key: []},
        intraday_begin=intraday_begin,
        intraday_end=intraday_end,
        min_gap_minutes=min_gap_minutes,
        start=start,
        end=end,
    )[instrument_key]


def confirm_gaps(report: GapReport, peers: dict[str, pl.Series]) -> None:
    """Renseigne ``Gap.confirmed_by`` à partir des timestamps d'autres instruments.

    Une seule requête pour tous les trous (``join_asof``), cf. :func:`audit_gaps`.

    :param report: Rapport à compléter (modifié en place).
    :param peers: ``{instrument_key: timestamps UTC}`` (cf. :func:`unique_timestamps`).
    """
    if not report.gaps:
        return
    gaps_lf = pl.LazyFrame(
        {
            "_idx": list(range(len(report.gaps))),
            "instrument": [report.instrument_key] * len(report.gaps),
            "start": [g.start for g in report.gaps],
            "end": [g.end for g in report.gaps],
        },
        schema={"_idx": pl.UInt32, "instrument": pl.Utf8, "start": _TS, "end": _TS},
    )
    stamps = {
        key: _prepare(ts.to_frame("window_start"), None, None)
        .select(pl.col("window_start").unique().sort())
        .collect()
        .lazy()
        for key, ts in peers.items()
    }
    found = _confirmations(gaps_lf, {report.instrument_key: list(peers)}, stamps).collect()
    by_idx = dict(zip(found["_idx"].to_list(), found["confirmed_by"].to_list(), strict=True))
    for idx, gap in enumerate(report.gaps):
        gap.confirmed_by = list(by_idx.get(idx) or [])


def _prepare(
    frame: pl.LazyFrame | pl.DataFrame, start: date | None, end: date | None
) -> pl.LazyFrame:
    """Projection ``window_start`` (UTC, ns) + ``ticker`` (texte), filtre de période large.

    Le filtre de période (marge de quelques jours pour fuseau et wrap-around) est
    poussé jusqu'à la lecture ; le filtre exact par session est appliqué ensuite.
    """
    lf = frame.lazy()
    schema = lf.collect_schema()
    if "window_start" not in schema:
        return pl.LazyFrame(schema={"window_start": _TS, "ticker": pl.Utf8})
    dtype = schema["window_start"]
    ws = pl.col("window_start")
    if not isinstance(dtype, pl.Datetime):
        ws = ws.cast(pl.Datetime("ns")).dt.replace_time_zone(_UTC)
    elif dtype.time_zone is None:
        ws = ws.dt.replace_time_zone(_UTC)
    elif dtype.time_zone != _UTC:
        ws = ws.dt.convert_time_zone(_UTC)
    ticker = pl.col("ticker").cast(pl.Utf8) if "ticker" in schema else pl.lit("", dtype=pl.Utf8)
    lf = lf.select(ws.dt.cast_time_unit("ns").alias("window_start"), ticker.alias("ticker"))
    if start is not None:
        lo = datetime.combine(start - timedelta(days=2), time(0), tzinfo=UTC)
        lf = lf.filter(pl.col("window_start") >= lo)
    if end is not None:
        hi = datetime.combine(end + timedelta(days=3), time(0), tzinfo=UTC)
        lf = lf.filter(pl.col("window_start") < hi)
    return lf


def _session_bars(
    lf: pl.LazyFrame,
    *,
    intraday_begin: time,
    intraday_end: time,
    timezone: str,
    start: date | None,
    end: date | None,
) -> pl.LazyFrame:
    """Barres uniques de la plage avec leur ``session``, triées par ``window_start``."""
    wrap = intraday_begin > intraday_end
    tod = pl.col("_local").dt.time()
    in_window = (
        (tod >= intraday_begin) | (tod < intraday_end)
        if wrap
        else (tod >= intraday_begin) & (tod < intraday_end)
    )
    session = pl.col("_local").dt.date()
    if wrap:
        session = pl.when(tod < intraday_end).then(session - pl.duration(days=1)).otherwise(session)
    work = (
        lf.unique(subset=["window_start"], keep="first")
        .with_columns(pl.col("window_start").dt.convert_time_zone(timezone).alias("_local"))
        .filter(in_window)
        .select("window_start", "ticker", session.alias("session"))
    )
    if start is not None:
        work = work.filter(pl.col("session") >= start)
    if end is not None:
        work = work.filter(pl.col("session") <= end)
    return work.sort("window_start")


def _gap_frame(
    work: pl.LazyFrame,
    *,
    instrument_key: str,
    intraday_begin: time,
    intraday_end: time,
    timezone: str,
    min_gap_minutes: int,
) -> pl.LazyFrame:
    """Trous (``session, start, end, position, ticker, instrument``) triés par début."""
    wrap = intraday_begin > intraday_end
    min_gap = timedelta(minutes=min_gap_minutes)
    columns = ["session", "start", "end", "position", "ticker"]

    # Trous internes : écart > min_gap entre deux barres consécutives d'une session
    before = pl.col("_prev_ticker").fill_null("")
    after = pl.col("ticker").fill_null("")
    inner = (
        work.with_columns(
            pl.col("window_start").shift(1).over("session").alias("_prev"),
            pl.col("ticker").shift(1).over("session").alias("_prev_ticker"),
        )
        .filter(
            pl.col("_prev").is_not_null()
            & ((pl.col("window_start") - pl.col("_prev")) >= pl.lit(min_gap + _ONE_MINUTE))
        )
        .select(
            pl.col("session"),
            (pl.col("_prev") + _ONE_MINUTE_NS).alias("start"),
            pl.col("window_start").alias("end"),
            pl.lit("milieu").alias("position"),
            pl.when((before == "") | (before == after))
            .then(after)
            .when(after == "")
            .then(before)
            .otherwise(pl.concat_str([before, pl.lit("/"), after]))
            .alias("ticker"),
        )
    )

    # Bords de plage : première barre en retard / dernière en avance (hors 1re / dernière session)
    end_day = pl.col("session") + pl.duration(days=1) if wrap else pl.col("session")
    bounds = (
        work.group_by("session")
        .agg(
            pl.col("window_start").first().alias("_first"),
            pl.col("ticker").first().alias("_first_ticker"),
            pl.col("window_start").last().alias("_last"),
            pl.col("ticker").last().alias("_last_ticker"),
        )
        .with_columns(
            _local_to_utc(pl.col("session"), intraday_begin, timezone).alias("_win_start"),
            _local_to_utc(end_day, intraday_end, timezone).alias("_win_end"),
            (pl.col("_last") + _ONE_MINUTE_NS).alias("_last_end"),
            (pl.col("session") == pl.col("session").min()).alias("_is_first"),
            (pl.col("session") == pl.col("session").max()).alias("_is_last"),
        )
    )
    head = bounds.filter(
        ~pl.col("_is_first") & ((pl.col("_first") - pl.col("_win_start")) >= pl.lit(min_gap))
    ).select(
        pl.col("session"),
        pl.col("_win_start").alias("start"),
        pl.col("_first").alias("end"),
        pl.lit("début").alias("position"),
        pl.col("_first_ticker").fill_null("").alias("ticker"),
    )
    tail = bounds.filter(
        ~pl.col("_is_last") & ((pl.col("_win_end") - pl.col("_last_end")) >= pl.lit(min_gap))
    ).select(
        pl.col("session"),
        pl.col("_last_end").alias("start"),
        pl.col("_win_end").alias("end"),
        pl.lit("fin").alias("position"),
        pl.col("_last_ticker").fill_null("").alias("ticker"),
    )

    return (
        pl.concat([inner.select(columns), head.select(columns), tail.select(columns)])
        .sort("start")
        .with_columns(pl.lit(instrument_key).alias("instrument"))
    )


def _confirmations(
    gaps: pl.LazyFrame,
    peers: Mapping[str, Sequence[str]],
    stamps: Mapping[str, pl.LazyFrame],
) -> pl.LazyFrame:
    """``(_idx, confirmed_by)`` pour tous les trous, en une requête.

    Pour chaque pair, un ``join_asof`` avant (timestamps du pair déjà triés) donne sa
    première barre à partir du début de chaque trou : le trou est confirmé si elle
    tombe avant sa fin. ``confirmed_by`` suit l'ordre de ``peers``.
    """
    ranks = {
        (key, peer): rank
        for key, keys in peers.items()
        for rank, peer in enumerate(keys)
        if peer != key and peer in stamps
    }
    empty = pl.LazyFrame(schema={"_idx": pl.UInt32, "confirmed_by": pl.List(pl.Utf8)})
    if not ranks:
        return empty
    left = gaps.select("_idx", "instrument", "start", "end").sort("start")
    hits: list[pl.LazyFrame] = []
    for peer in dict.fromkeys(p for _, p in ranks):
        owners = [key for key, p in ranks if p == peer]
        hits.append(
            left.filter(pl.col("instrument").is_in(owners))
            .join_asof(
                stamps[peer].with_columns(pl.col("window_start").set_sorted()),
                left_on="start",
                right_on="window_start",
                strategy="forward",
                check_sortedness=False,
            )
            .filter(pl.col("window_start") < pl.col("end"))
            .select("_idx", "instrument", pl.lit(peer).alias("peer"))
        )
    order = pl.LazyFrame(
        {
            "instrument": [k for k, _ in ranks],
            "peer": [p for _, p in ranks],
            "_rank": list(ranks.values()),
        },
        schema={"instrument": pl.Utf8, "peer": pl.Utf8, "_rank": pl.Int32},
    )
    return (
        pl.concat(hits)
        .join(order, on=["instrument", "peer"])
        .sort("_idx", "_rank")
        .group_by("_idx", maintain_order=True)
        .agg(pl.col("peer").alias("confirmed_by"))
    )


def _local_to_utc(day: pl.Expr, at: time, timezone: str) -> pl.Expr:
    """``day`` + heure murale ``at`` dans ``timezone`` → instant UTC (ns).

    Même règle que ``datetime.combine(..., tzinfo=ZoneInfo)`` (``fold=0``) : heure
    ambiguë → première occurrence ; heure inexistante (passage à l'heure d'été) →
    interprétée avec le décalage d'avant la transition.
    """
    naive = day.cast(pl.Datetime("ns")) + pl.duration(
        hours=at.hour, minutes=at.minute, seconds=at.second, time_unit="ns"
    )
    direct = naive.dt.replace_time_zone(timezone, ambiguous="earliest", non_existent="null")
    shifted = (naive - pl.duration(hours=1)).dt.replace_time_zone(
        timezone, ambiguous="earliest", non_existent="null"
    ) + pl.duration(hours=1)
    return pl.coalesce(direct, shifted).dt.convert_time_zone(_UTC).dt.cast_time_unit("ns")


def explain_gaps(
    report: GapReport,
    calendar: TradingCalendar,
    *,
    intraday_begin: time,
    intraday_end: time,
    timezone: str,
    min_gap_minutes: int,
) -> None:
    """Confronte trous et sessions vides au calendrier de marché (modifie ``report``).

    - Trou sur lequel le calendrier fait autorité et qui compte moins de
      ``min_gap_minutes`` de séance prévue → attendu (``Gap.calendar_note``).
    - Session vide couverte : fermée (moins de ``min_gap_minutes`` de séance
      prévue dans la plage) → ``closed_sessions`` ; sinon → ``expected_empty_sessions``.

    Hors couverture du calendrier, rien ne change.
    """
    wrap = intraday_begin > intraday_end
    tz = ZoneInfo(timezone)
    for gap in report.gaps:
        if (
            calendar.covers(gap.start, gap.end)
            and calendar.open_minutes(gap.start, gap.end) < min_gap_minutes
        ):
            gap.calendar_note = calendar.closure_label(gap.start)
    for sess in report.empty_sessions:
        win_start, win_end = _session_bounds(sess, intraday_begin, intraday_end, wrap, tz)
        if not calendar.covers(win_start, win_end):
            continue
        if calendar.open_minutes(win_start, win_end) < min_gap_minutes:
            report.closed_sessions[sess] = calendar.closure_label(win_start)
        else:
            report.expected_empty_sessions.append(sess)


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
