"""Tests des calendriers de marché historisés (``calendar`` + intégration ``doctor gaps``).

Les fixtures reprennent les motifs réels de ``/futures/v1/schedules`` (sonde du
2026-09-27) : séance normale ``pre_open`` 16:45 / ``open`` 17:00 la veille,
``close`` 16:00 CT ; lundi férié absent et arrêt 12:00→17:00 porté par la séance
du mardi ; clôture anticipée 12:15 ; lignes doublées par le spread calendaire.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import polars as pl
import pytest
import respx
from rich.console import Console

from myquantstore.api.client import ClientError, MassiveClient
from myquantstore.api.market_calendar import (
    fetch_futures_schedules,
    normalize_futures_schedules,
    normalize_market_holidays,
)
from myquantstore.cli import main
from myquantstore.instruments import Instrument, InstrumentType
from myquantstore.market_calendar.refresh import (
    holidays_coverage,
    plan_futures_schedule,
    plan_refresh,
    run_refresh,
)
from myquantstore.market_calendar.store import (
    CalendarSource,
    apply_dump,
    known_windows,
    list_dumps,
    merge_dump,
    read_calendar,
    read_calendar_meta,
    rebuild,
    save_dump,
)
from myquantstore.market_calendar.views import (
    CLOSED,
    EARLY_CLOSE,
    INTERRUPTED,
    NO_SESSION,
    ExchangeHolidays,
    FuturesSchedule,
    list_holidays,
    load_calendar,
)
from myquantstore.pipeline.aggregator import aggregate
from myquantstore.storage.gaps import explain_gaps, find_gaps
from myquantstore.storage.raw_dumps import save_raw_dump

CT = ZoneInfo("America/Chicago")
ET = ZoneInfo("America/New_York")
OUTRIGHT = "E-mini S&P 500 Futures"
SPREAD = "ES Equity Calendar Spread"

# Semaine de Labor Day 2026 : lun. 07/09 férié (arrêt 12:00→17:00), puis mar.-ven.
LABOR_DAY = date(2026, 9, 7)
WEEK = [date(2026, 9, 4), date(2026, 9, 8), date(2026, 9, 9), date(2026, 9, 10), date(2026, 9, 11)]


def _ct(d: date, hh: int, mm: int = 0) -> str:
    return datetime.combine(d, time(hh, mm), tzinfo=CT).astimezone(UTC).isoformat()


def _session(
    code: str, day: date, events: list[tuple[str, date, int, int]]
) -> list[dict[str, Any]]:
    """Événements d'une trade date, doublés par le spread calendaire (comme l'API)."""
    rows = []
    for name in (OUTRIGHT, SPREAD):
        for event, d, hh, mm in events:
            rows.append(
                {
                    "product_code": code,
                    "product_name": name,
                    "trading_venue": "XCME",
                    "session_end_date": day.isoformat(),
                    "event": event,
                    "timestamp": _ct(d, hh, mm),
                }
            )
    return rows


def _normal(code: str, day: date) -> list[dict[str, Any]]:
    """Séance normale : ouverture la veille à 17:00 (dimanche pour un lundi), close 16:00."""
    prev = day - timedelta(days=1)
    return _session(
        code,
        day,
        [("pre_open", prev, 16, 45), ("open", prev, 17, 0), ("close", day, 16, 0)],
    )


def _labor_day_week(code: str = "ES") -> list[dict[str, Any]]:
    """Ven. 04/09 normal, lun. 07/09 absent, mar. 08/09 interrompu, mer.-ven. normaux."""
    rows = _normal(code, date(2026, 9, 4))
    rows += _session(
        code,
        date(2026, 9, 8),
        [
            ("pre_open", date(2026, 9, 6), 16, 0),
            ("open", date(2026, 9, 6), 17, 0),
            ("pre_open", LABOR_DAY, 12, 0),
            ("open", LABOR_DAY, 17, 0),
            ("close", date(2026, 9, 8), 16, 0),
        ],
    )
    for d in WEEK[2:]:
        rows += _normal(code, d)
    return rows


def _early_close(code: str, day: date) -> list[dict[str, Any]]:
    prev = day - timedelta(days=1)
    return _session(
        code, day, [("pre_open", prev, 16, 45), ("open", prev, 17, 0), ("close", day, 12, 15)]
    )


HOLIDAYS_ROWS = [
    {"date": "2026-11-26", "exchange": "NYSE", "name": "Thanksgiving", "status": "closed"},
    {"date": "2026-11-26", "exchange": "NASDAQ", "name": "Thanksgiving", "status": "closed"},
    {
        "date": "2026-11-27",
        "exchange": "NYSE",
        "name": "Thanksgiving",
        "status": "early-close",
        "open": "2026-11-27T14:30:00.000Z",
        "close": "2026-11-27T18:00:00.000Z",
    },
    {
        "date": "2026-11-27",
        "exchange": "NASDAQ",
        "name": "Thanksgiving",
        "status": "early-close",
        "open": "2026-11-27T14:30:00.000Z",
        "close": "2026-11-27T18:00:00.000Z",
    },
    {"date": "2026-12-25", "exchange": "NYSE", "name": "Christmas", "status": "closed"},
]


@pytest.fixture
def client(tmp_settings):
    c = MassiveClient(tmp_settings)
    yield c
    c.close()


def _schedules_df(rows: list[dict[str, Any]]) -> pl.DataFrame:
    return normalize_futures_schedules(rows)


def _es_aggregate(rows: list[dict[str, Any]]) -> pl.DataFrame:
    source = CalendarSource.futures_schedule("ES")
    return merge_dump(pl.DataFrame(schema=source.schema), _schedules_df(rows), source, None)


# ---------------------------------------------------------------------------
# API : normalisation, liste JSON, pagination
# ---------------------------------------------------------------------------


class TestApi:
    def test_normalize_market_holidays(self):
        df = normalize_market_holidays(HOLIDAYS_ROWS)
        assert df.schema["date"] == pl.Date
        assert df.schema["close"] == pl.Datetime("ns", "UTC")
        early = df.filter((pl.col("date") == date(2026, 11, 27)) & (pl.col("exchange") == "NYSE"))
        assert early["close"][0] == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)
        closed = df.filter(pl.col("date") == date(2026, 11, 26))
        assert closed["open"].is_null().all()

    def test_normalize_empty_keeps_schema(self):
        assert normalize_market_holidays([]).columns == [
            "date",
            "exchange",
            "name",
            "status",
            "open",
            "close",
        ]
        assert normalize_futures_schedules([]).is_empty()

    def test_normalize_futures_schedules_sorted(self):
        df = _schedules_df(list(reversed(_normal("ES", date(2026, 9, 9)))))
        assert df.schema["timestamp"] == pl.Datetime("ns", "UTC")
        assert df["event"].to_list()[:2] == ["pre_open", "pre_open"]
        assert df["event"].to_list()[-1] == "close"

    @respx.mock
    def test_get_list_returns_json_array(self, client):
        respx.get("/v1/marketstatus/upcoming").mock(
            return_value=httpx.Response(200, json=HOLIDAYS_ROWS)
        )
        assert len(client.get_list("/v1/marketstatus/upcoming")) == 5

    @respx.mock
    def test_get_list_rejects_object(self, client):
        respx.get("/v1/marketstatus/upcoming").mock(
            return_value=httpx.Response(200, json={"results": []})
        )
        with pytest.raises(ClientError, match="tableau JSON attendu"):
            client.get_list("/v1/marketstatus/upcoming")

    @respx.mock
    def test_fetch_futures_schedules_params_and_pagination(self, client):
        rows = _labor_day_week()
        page2 = "https://api.test.massive.com/futures/v1/schedules?cursor=abc"
        route = respx.get("/futures/v1/schedules").mock(
            side_effect=[
                httpx.Response(200, json={"results": rows[:10], "next_url": page2}),
                httpx.Response(200, json={"results": rows[10:]}),
            ]
        )
        df = fetch_futures_schedules(client, "ES", date(2026, 9, 1))
        assert df.height == len(rows)
        first = route.calls[0].request.url.params
        assert first["product_code"] == "ES"
        assert first["session_end_date.gte"] == "2026-09-01"
        assert first["sort"] == "session_end_date.asc"
        assert first["limit"] == "1000"


# ---------------------------------------------------------------------------
# Stockage : fusion par fenêtre, dumps, reconstruction
# ---------------------------------------------------------------------------


class TestStore:
    def test_combo_duplicates_dropped_and_deduplicated(self):
        rows = _labor_day_week()
        rows += [{**r, "product_name": "YM Butterfly"} for r in rows if r["product_name"] == SPREAD]
        agg = _es_aggregate(rows)
        assert set(agg["product_name"].to_list()) == {OUTRIGHT}
        assert agg.height == agg.unique(subset=["session_end_date", "event", "timestamp"]).height
        assert agg.height == len(rows) // 3

    def test_window_replaces_sessions_and_keeps_outside(self):
        """Un jour absent du nouveau dump dans sa fenêtre disparaît ; hors fenêtre, rien ne bouge."""
        source = CalendarSource.futures_schedule("ES")
        old = _es_aggregate(_normal("ES", date(2026, 9, 4)) + _normal("ES", date(2026, 9, 9)))
        # nouveau dump couvrant 08/09 → 11/09 sans le 09/09 (devenu férié)
        new = _schedules_df(_normal("ES", date(2026, 9, 10)))
        merged = merge_dump(old, new, source, (date(2026, 9, 8), date(2026, 9, 11)))
        days = set(merged["session_end_date"].to_list())
        assert date(2026, 9, 4) in days  # hors fenêtre : conservé
        assert date(2026, 9, 9) not in days  # dans la fenêtre, absent du dump : retiré
        assert date(2026, 9, 10) in days

    def test_holidays_past_kept_future_revised(self):
        source = CalendarSource.market_holidays()
        first = normalize_market_holidays(HOLIDAYS_ROWS)
        # snapshot suivant (run du 2026-11-30) : Thanksgiving passé n'y est plus,
        # Christmas passe en clôture anticipée.
        second = normalize_market_holidays(
            [
                {
                    "date": "2026-12-25",
                    "exchange": "NYSE",
                    "name": "Christmas",
                    "status": "early-close",
                    "open": "2026-12-25T14:30:00.000Z",
                    "close": "2026-12-25T18:00:00.000Z",
                }
            ]
        )
        coverage = holidays_coverage(second, date(2026, 11, 30))
        assert coverage == (date(2026, 12, 1), date(2026, 12, 25))
        merged = merge_dump(first, second, source, coverage)
        assert merged.filter(pl.col("date") == date(2026, 11, 26)).height == 2
        xmas = merged.filter(
            (pl.col("date") == date(2026, 12, 25)) & (pl.col("exchange") == "NYSE")
        )
        assert xmas["status"].to_list() == ["early-close"]

    def test_holidays_coverage_excludes_run_day(self):
        df = normalize_market_holidays(HOLIDAYS_ROWS)
        assert holidays_coverage(df, date(2026, 11, 26)) == (date(2026, 11, 27), date(2026, 12, 25))
        assert holidays_coverage(df, date(2026, 12, 25)) is None
        assert holidays_coverage(normalize_market_holidays([]), date(2026, 1, 1)) is None

    def test_apply_dump_then_rebuild_matches(self, tmp_settings):
        """Invariant d'historisation : rejouer les dumps redonne l'agrégat."""
        source = CalendarSource.futures_schedule("ES")
        dumps = [
            ("20260901T030000", _labor_day_week()[:15], (date(2026, 9, 1), date(2026, 9, 8))),
            ("20260908T030000", _labor_day_week()[15:], (date(2026, 9, 8), date(2026, 9, 11))),
        ]
        for run_ts, rows, coverage in dumps:
            df = _schedules_df(rows)
            save_dump(source, df, run_ts, tmp_settings, coverage=coverage, source_url="/x")
            apply_dump(source, df, coverage, run_ts, tmp_settings, requested_start=coverage[0])
        assert len(list_dumps(source, tmp_settings)) == 2
        on_disk = read_calendar(source, tmp_settings)
        assert rebuild(source, tmp_settings).equals(on_disk)
        meta = read_calendar_meta(source, tmp_settings)
        assert meta is not None
        assert meta["last_run_ts"] == "20260908T030000"
        assert meta["requested_from"] == "2026-09-01"
        assert known_windows(meta) == [(date(2026, 9, 1), date(2026, 9, 11))]

    def test_rebuild_write_restores_aggregate(self, tmp_settings):
        source = CalendarSource.market_holidays()
        df = normalize_market_holidays(HOLIDAYS_ROWS)
        cov = (date(2026, 9, 28), date(2026, 12, 25))
        save_dump(source, df, "20260927T030000", tmp_settings, coverage=cov, source_url="/x")
        rebuilt = rebuild(source, tmp_settings, write=True)
        assert read_calendar(source, tmp_settings).equals(rebuilt)
        assert known_windows(read_calendar_meta(source, tmp_settings)) == [cov]


# ---------------------------------------------------------------------------
# Refresh : plages, orchestration
# ---------------------------------------------------------------------------


class TestRefreshPlan:
    TODAY = date(2026, 9, 27)

    def _record_run(self, settings, run_ts: str, start: date) -> None:
        source = CalendarSource.futures_schedule("ES")
        df = _schedules_df(_normal("ES", date(2026, 9, 25)))
        apply_dump(source, df, (start, date(2026, 9, 25)), run_ts, settings, requested_start=start)

    def test_first_run_backfills_history_months(self, tmp_settings):
        plan = plan_futures_schedule(tmp_settings, "ES", self.TODAY)
        assert plan.reason == "backfill initial"
        assert plan.start == self.TODAY - timedelta(days=30 * 24)

    def test_next_run_incremental_from_last_run(self, tmp_settings):
        self._record_run(tmp_settings, "20260920T030000", self.TODAY - timedelta(days=720))
        plan = plan_futures_schedule(tmp_settings, "ES", self.TODAY)
        assert plan.reason == "incrémental"
        assert plan.start == date(2026, 9, 19)  # dernier run - overlap_buffer_days (1)

    def test_history_months_increase_extends_backwards(self, tmp_settings):
        self._record_run(tmp_settings, "20260920T030000", self.TODAY - timedelta(days=720))
        tmp_settings.history_months = {**tmp_settings.history_months, "futures": 36}
        plan = plan_futures_schedule(tmp_settings, "ES", self.TODAY)
        assert plan.reason.startswith("extension arrière")
        assert plan.start == self.TODAY - timedelta(days=30 * 36)

    def test_plan_refresh_one_holidays_call_for_three_types(self, tmp_settings):
        instruments = [
            Instrument(InstrumentType.STOCKS, "AAPL"),
            Instrument(InstrumentType.FOREX, "EURUSD"),
            Instrument(InstrumentType.INDICES, "SPX"),
            Instrument(InstrumentType.FUTURES, "ES"),
            Instrument(InstrumentType.FUTURES, "ES"),
            Instrument(InstrumentType.OPTIONS, "SPY"),
        ]
        plans = plan_refresh(tmp_settings, instruments, self.TODAY)
        assert [p.source.label for p in plans] == ["market_holidays", "futures_schedules:ES"]
        assert plans[0].endpoint == "/v1/marketstatus/upcoming"


class TestRunRefresh:
    @respx.mock
    def test_run_refresh_writes_dumps_and_aggregates(self, tmp_settings, client):
        respx.get("/v1/marketstatus/upcoming").mock(
            return_value=httpx.Response(200, json=HOLIDAYS_ROWS)
        )
        respx.get("/futures/v1/schedules").mock(
            return_value=httpx.Response(200, json={"results": _labor_day_week()})
        )
        instruments = [
            Instrument(InstrumentType.INDICES, "SPX"),
            Instrument(InstrumentType.FUTURES, "ES"),
        ]
        plans = plan_refresh(tmp_settings, instruments, date(2026, 9, 1))
        results = run_refresh(plans, client, tmp_settings, run_ts="20260901T030000")
        assert all(r.ok for r in results)
        holidays, schedule = results
        assert holidays.coverage == (date(2026, 9, 2), date(2026, 12, 25))
        assert schedule.coverage == (plans[1].start, date(2026, 9, 11))
        assert schedule.rows_after == schedule.fetched_rows // 2  # spread écarté
        assert read_calendar(CalendarSource.market_holidays(), tmp_settings).height == 5

    @respx.mock
    def test_one_source_failing_does_not_block_others(self, tmp_settings, client):
        respx.get("/v1/marketstatus/upcoming").mock(return_value=httpx.Response(403, text="NO"))
        respx.get("/futures/v1/schedules").mock(
            return_value=httpx.Response(200, json={"results": _labor_day_week()})
        )
        instruments = [
            Instrument(InstrumentType.STOCKS, "AAPL"),
            Instrument(InstrumentType.FUTURES, "ES"),
        ]
        results = run_refresh(plan_refresh(tmp_settings, instruments), client, tmp_settings)
        assert [r.ok for r in results] == [False, True]
        assert "403" in (results[0].error or "")
        assert not CalendarSource.market_holidays().aggregate_path(tmp_settings).exists()


# ---------------------------------------------------------------------------
# Vues : séances futures, fériés actions
# ---------------------------------------------------------------------------


class TestFuturesSchedule:
    @pytest.fixture
    def schedule(self) -> FuturesSchedule:
        rows = _labor_day_week() + _early_close("ES", date(2026, 12, 24))
        return FuturesSchedule("ES", _es_aggregate(rows), {LABOR_DAY: "Labor Day"})

    def test_events_holiday_interruption_early_close(self, schedule):
        events = {ev.day: ev for ev in schedule.events()}
        assert events[LABOR_DAY].kinds == (NO_SESSION,)
        assert events[LABOR_DAY].label == "férié (Labor Day)"
        # trading du lundi férié : 00:00→12:00 puis 17:00→24:00 CT
        local = [
            (a.astimezone(CT).time(), b.astimezone(CT).time())
            for a, b in events[LABOR_DAY].intervals
        ]
        assert local == [(time(0), time(12)), (time(17), time(0))]
        assert events[date(2026, 9, 8)].kinds == (INTERRUPTED,)
        assert events[date(2026, 12, 24)].kinds == (EARLY_CLOSE,)
        assert schedule.normal_close == time(16)

    def test_no_session_only_inside_coverage(self, schedule):
        """Jours sans séance cherchés entre la première et la dernière séance complète."""
        assert schedule.coverage == (date(2026, 9, 4), date(2026, 12, 24))
        assert min(ev.day for ev in schedule.events()) == LABOR_DAY
        assert schedule.events(end=date(2026, 9, 6)) == []

    def test_open_minutes_and_closure_label(self, schedule):
        start = datetime.combine(LABOR_DAY, time(7), tzinfo=CT)
        noon = datetime.combine(LABOR_DAY, time(12), tzinfo=CT)
        end = datetime.combine(LABOR_DAY, time(15), tzinfo=CT)
        assert schedule.open_minutes(start, end) == 300
        assert schedule.open_minutes(noon, end) == 0
        assert schedule.closure_label(noon) == "férié (Labor Day)"
        normal = datetime.combine(date(2026, 9, 9), time(7), tzinfo=CT)
        assert schedule.open_minutes(normal, normal + timedelta(hours=8)) == 480

    def test_covers(self, schedule):
        inside = datetime.combine(date(2026, 9, 9), time(7), tzinfo=CT)
        assert schedule.covers(inside, inside + timedelta(hours=8))
        before = datetime.combine(date(2026, 9, 1), time(7), tzinfo=CT)
        assert not schedule.covers(before, before + timedelta(hours=8))

    def test_incomplete_session_is_not_a_holiday(self):
        rows = _labor_day_week()
        # séance du 09/09 incomplète (close seul, comme les orphelins en tête d'historique)
        rows = [
            r for r in rows if not (r["session_end_date"] == "2026-09-09" and r["event"] != "close")
        ]
        schedule = FuturesSchedule("ES", _es_aggregate(rows))
        assert date(2026, 9, 9) not in {ev.day for ev in schedule.events()}
        day = datetime.combine(date(2026, 9, 9), time(7), tzinfo=CT)
        assert not schedule.covers(day, day + timedelta(hours=8))


class TestExchangeHolidays:
    @pytest.fixture
    def holidays(self) -> ExchangeHolidays:
        df = normalize_market_holidays(HOLIDAYS_ROWS)
        return ExchangeHolidays(df, [(date(2026, 9, 28), date(2026, 12, 25))])

    def test_closed_and_early_close(self, holidays):
        thanksgiving = datetime.combine(date(2026, 11, 26), time(9, 30), tzinfo=ET)
        assert holidays.open_minutes(thanksgiving, thanksgiving + timedelta(hours=6)) == 0
        assert holidays.closure_label(thanksgiving) == f"{CLOSED} (Thanksgiving)"
        after_close = datetime.combine(date(2026, 11, 27), time(13), tzinfo=ET)
        assert holidays.open_minutes(after_close, after_close + timedelta(hours=3)) == 0
        before_close = datetime.combine(date(2026, 11, 27), time(12), tzinfo=ET)
        assert holidays.open_minutes(before_close, before_close + timedelta(hours=2)) == 60

    def test_covers_only_known_windows(self, holidays):
        known = datetime.combine(date(2026, 10, 5), time(9, 30), tzinfo=ET)
        assert holidays.covers(known, known + timedelta(hours=6))
        past = datetime.combine(date(2026, 9, 7), time(9, 30), tzinfo=ET)
        assert not holidays.covers(past, past + timedelta(hours=6))


class TestListHolidays:
    def test_identical_markets_grouped(self, tmp_settings):
        for code in ("ES", "NQ"):
            source = CalendarSource.futures_schedule(code)
            df = _schedules_df(_labor_day_week(code))
            apply_dump(
                source, df, (date(2026, 9, 1), date(2026, 9, 11)), "20260901T030000", tmp_settings
            )
        source = CalendarSource.market_holidays()
        df = normalize_market_holidays(HOLIDAYS_ROWS)
        apply_dump(
            source, df, (date(2026, 9, 2), date(2026, 12, 25)), "20260901T030000", tmp_settings
        )
        rows = list_holidays(tmp_settings, futures=["ES", "NQ"], exchanges=True)
        labor = [r for r in rows if r.day == LABOR_DAY]
        assert len(labor) == 1 and labor[0].markets == ("futures:ES", "NQ")
        thanksgiving = [r for r in rows if r.day == date(2026, 11, 26)]
        assert thanksgiving[0].markets == ("NASDAQ", "NYSE")
        only_nyse = list_holidays(
            tmp_settings, futures=[], exchanges=True, exchange_filter="nasdaq"
        )
        assert {r.markets for r in only_nyse} == {("NASDAQ",)}

    def test_load_calendar_by_type(self, tmp_settings):
        es = Instrument(InstrumentType.FUTURES, "ES")
        fx = Instrument(InstrumentType.FOREX, "EURUSD")
        assert load_calendar(tmp_settings, es) is None
        source = CalendarSource.futures_schedule("ES")
        apply_dump(source, _schedules_df(_labor_day_week()), None, "20260901T030000", tmp_settings)
        assert isinstance(load_calendar(tmp_settings, es), FuturesSchedule)
        assert load_calendar(tmp_settings, fx) is None  # pas de calendrier FX chez Massive


# ---------------------------------------------------------------------------
# doctor gaps : trous et sessions vides expliqués par le calendrier
# ---------------------------------------------------------------------------


def _bars(days: list[date], begin: time, end: time) -> list[datetime]:
    out: list[datetime] = []
    for d in days:
        t = datetime.combine(d, begin, tzinfo=CT)
        while t < datetime.combine(d, end, tzinfo=CT):
            out.append(t.astimezone(UTC))
            t += timedelta(minutes=1)
    return out


class TestExplainGaps:
    def test_gap_on_scheduled_halt_is_expected(self):
        schedule = FuturesSchedule("ES", _es_aggregate(_labor_day_week()), {LABOR_DAY: "Labor Day"})
        ts = _bars([date(2026, 9, 4)], time(7), time(15)) + _bars([LABOR_DAY], time(7), time(12))
        ts += _bars([date(2026, 9, 9)], time(7), time(10)) + _bars(
            [date(2026, 9, 10)], time(7), time(15)
        )
        df = pl.DataFrame({"window_start": ts, "ticker": ["ESU6"] * len(ts)})
        kw: dict[str, Any] = {
            "intraday_begin": time(7),
            "intraday_end": time(15),
            "timezone": "America/Chicago",
            "min_gap_minutes": 5,
        }
        report = find_gaps(df, instrument_key="futures:ES", **kw)
        explain_gaps(report, schedule, **kw)
        by_day = {g.session: g for g in report.gaps}
        assert by_day[LABOR_DAY].calendar_note == "férié (Labor Day)"
        assert by_day[date(2026, 9, 9)].calendar_note == ""  # vrai trou 10:00→15:00
        # 08/09 : séance prévue mais aucune barre → signalée
        assert report.expected_empty_sessions == [date(2026, 9, 8)]

    def test_empty_session_closed_by_calendar(self):
        rows = _labor_day_week()
        # Good Friday : aucune séance le vendredi 11/09 fictif → jour ouvré fermé
        rows = [r for r in rows if r["session_end_date"] != "2026-09-11"]
        rows += _normal("ES", date(2026, 9, 14))
        schedule = FuturesSchedule("ES", _es_aggregate(rows))
        ts = _bars([date(2026, 9, 10), date(2026, 9, 14)], time(7), time(15))
        df = pl.DataFrame({"window_start": ts, "ticker": ["ESU6"] * len(ts)})
        kw: dict[str, Any] = {
            "intraday_begin": time(7),
            "intraday_end": time(15),
            "timezone": "America/Chicago",
            "min_gap_minutes": 5,
        }
        report = find_gaps(df, instrument_key="futures:ES", **kw)
        explain_gaps(report, schedule, **kw)
        assert report.closed_sessions == {date(2026, 9, 11): "férié"}
        assert report.expected_empty_sessions == []


def _seed(settings, symbol: str, ts: list[datetime]) -> None:
    inst = Instrument(type=InstrumentType.FUTURES, symbol=symbol)
    ticker = f"{symbol}U6"
    n = len(ts)
    df = pl.DataFrame(
        {
            "window_start": ts,
            "ticker": [ticker] * n,
            "open": [100.0] * n,
            "high": [101.0] * n,
            "low": [99.0] * n,
            "close": [100.5] * n,
            "volume": [10] * n,
            "session_end_date": [t.date() for t in ts],
        }
    )
    save_raw_dump(df, inst, ticker, "20260927T170000", settings, resolution="1min")
    aggregate(inst, settings, resolution="1min")


class TestCli:
    @pytest.fixture
    def settings(self, tmp_settings, monkeypatch):
        tmp_settings.chart_intraday_begin = time(7)
        tmp_settings.chart_intraday_end = time(15)
        tmp_settings.chart_timezone = "America/Chicago"
        monkeypatch.setattr("myquantstore.cli.load_settings", lambda *a, **k: tmp_settings)
        # Tableaux rich sans retour à la ligne (assertions sur le texte)
        monkeypatch.setattr("myquantstore.cli.console", Console(width=250))
        return tmp_settings

    def _store_schedules(self, settings) -> None:
        for code in ("ES", "NQ"):
            source = CalendarSource.futures_schedule(code)
            df = _schedules_df(_labor_day_week(code))
            apply_dump(
                source, df, (date(2026, 9, 1), date(2026, 9, 11)), "20260901T030000", settings
            )

    def test_refresh_dry_run(self, settings, capsys):
        rc = main(["calendar", "refresh", "--dry-run"])
        out = capsys.readouterr().out
        assert rc == 0
        assert "futures:ES" in out and "backfill initial" in out
        assert "aucun appel API" in out

    def test_refresh_options_rejected(self, settings, capsys):
        assert main(["calendar", "refresh", "--type", "options"]) == 1
        assert "options" in capsys.readouterr().out

    @respx.mock
    def test_refresh_exit_codes(self, settings, capsys):
        respx.get("/futures/v1/schedules").mock(
            side_effect=[
                httpx.Response(200, json={"results": _labor_day_week("ES")}),
                httpx.Response(403, text="NOT_AUTHORIZED"),
            ]
        )
        rc = main(["calendar", "refresh", "--type", "futures"])
        out = capsys.readouterr().out
        assert rc == 1  # NQ en erreur
        assert "erreur" in out
        assert read_calendar(CalendarSource.futures_schedule("ES"), settings).height > 0

    def test_holidays_and_status(self, settings, capsys):
        self._store_schedules(settings)
        assert main(["calendar", "holidays", "--start", "2026-09-07", "--end", "2026-09-08"]) == 0
        out = capsys.readouterr().out
        assert "2026-09-07" in out and "férié" in out and "futures:ES, NQ" in out
        assert "séance interrompue" in out
        assert main(["calendar", "status"]) == 0
        status = capsys.readouterr().out
        assert "futures:ES" in status and "2026-09-04 → 2026-09-11" in status

    def test_doctor_gaps_expected_even_if_confirmed(self, settings, capsys):
        """NQ sans barre après 12:00 le jour férié, ES avec : confirmé mais attendu → exit 0."""
        self._store_schedules(settings)
        days = [date(2026, 9, 4), LABOR_DAY, date(2026, 9, 9)]
        _seed(settings, "ES", _bars(days, time(7), time(15)))
        _seed(
            settings,
            "NQ",
            _bars(days[:1], time(7), time(15))
            + _bars([LABOR_DAY], time(7), time(12))
            + _bars(days[2:], time(7), time(15)),
        )
        rc = main(["doctor", "gaps", "--timezone", "America/Chicago"])
        out = capsys.readouterr().out
        assert rc == 0
        assert "attendu" in out and "férié" in out
        assert main(["doctor", "gaps", "--timezone", "America/Chicago", "--no-calendar"]) == 1
