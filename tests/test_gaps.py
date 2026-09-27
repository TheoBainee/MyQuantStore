"""Tests de l'audit des trous 1min (storage/gaps.py + ``doctor gaps``)."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from myquantstore.cli import main
from myquantstore.config import load_settings
from myquantstore.instruments import Instrument, InstrumentType
from myquantstore.pipeline.aggregator import aggregate
from myquantstore.storage.gaps import confirm_gaps, find_gaps, unique_timestamps
from myquantstore.storage.raw_dumps import save_raw_dump

CHI = ZoneInfo("America/Chicago")
TZ = "America/Chicago"


def _bars(
    days: list[date],
    begin: time,
    end: time,
    *,
    holes: dict[date, list[tuple[time, time]]] | None = None,
) -> list[datetime]:
    """Barres 1min (UTC) de ``begin`` à ``end`` (exclu, heure de Chicago), trous exclus."""
    out: list[datetime] = []
    for d in days:
        t = datetime.combine(d, begin, tzinfo=CHI)
        stop = datetime.combine(d, end, tzinfo=CHI)
        while t < stop:
            skip = any(a <= t.time() < b for a, b in (holes or {}).get(d, []))
            if not skip:
                out.append(t.astimezone(UTC))
            t += timedelta(minutes=1)
    return out


def _df(ts: list[datetime], ticker: str) -> pl.DataFrame:
    return pl.DataFrame({"window_start": ts, "ticker": [ticker] * len(ts)})


# jeu. 10, ven. 11, lun. 14 septembre 2026
DAYS = [date(2026, 9, 10), date(2026, 9, 11), date(2026, 9, 14)]
HOLE_DAY = date(2026, 9, 11)


def _find(df: pl.DataFrame, **kw) -> object:
    params = {
        "instrument_key": "futures:NQ",
        "intraday_begin": time(7),
        "intraday_end": time(15),
        "timezone": TZ,
        "min_gap_minutes": 5,
    }
    params.update(kw)
    return find_gaps(df, **params)


class TestFindGaps:
    def test_internal_gap_confirmed_by_peer(self):
        nq = _df(_bars(DAYS, time(4), time(16), holes={HOLE_DAY: [(time(12), time(14))]}), "NQU6")
        es = _df(_bars(DAYS, time(4), time(16)), "ESU6")
        report = _find(nq)
        confirm_gaps(
            report, {"futures:NQ": unique_timestamps(nq), "futures:ES": unique_timestamps(es)}
        )

        assert report.sessions_checked == 3
        assert len(report.gaps) == 1
        gap = report.gaps[0]
        assert gap.session == HOLE_DAY
        assert gap.start.astimezone(CHI).time() == time(12)
        assert gap.end.astimezone(CHI).time() == time(14)
        assert gap.minutes == 120
        assert gap.position == "milieu"
        assert gap.ticker == "NQU6"
        assert gap.confirmed_by == ["futures:ES"]

    def test_gap_not_confirmed_when_peer_also_missing(self):
        """Férié / clôture anticipée : tous les instruments ont le même trou."""
        holes = {HOLE_DAY: [(time(12), time(14))]}
        nq = _df(_bars(DAYS, time(4), time(16), holes=holes), "NQU6")
        es = _df(_bars(DAYS, time(4), time(16), holes=holes), "ESU6")
        report = _find(nq)
        confirm_gaps(report, {"futures:ES": unique_timestamps(es)})
        assert len(report.gaps) == 1
        assert report.gaps[0].confirmed_by == []

    def test_below_threshold_ignored(self):
        nq = _df(
            _bars(DAYS, time(7), time(15), holes={HOLE_DAY: [(time(10), time(10, 4))]}), "NQU6"
        )
        assert _find(nq).gaps == []
        assert len(_find(nq, min_gap_minutes=4).gaps) == 1

    def test_outside_window_ignored(self):
        """Un trou hors plage (ex: 16:00-17:00 CME) n'est pas signalé."""
        nq = _df(_bars(DAYS, time(4), time(16), holes={HOLE_DAY: [(time(5), time(6))]}), "NQU6")
        assert _find(nq).gaps == []

    def test_window_edges(self):
        """Première barre en retard / dernière en avance → trous début / fin."""
        nq = _df(
            _bars(
                DAYS,
                time(7),
                time(15),
                holes={HOLE_DAY: [(time(7), time(8)), (time(14), time(15))]},
            ),
            "NQU6",
        )
        gaps = _find(nq).gaps
        assert [(g.position, g.minutes) for g in gaps] == [("début", 60), ("fin", 60)]

    def test_edges_of_first_and_last_session_not_reported(self):
        """Début d'historique / fetch en cours de séance : pas de faux positif."""
        ts = _bars([DAYS[0]], time(10), time(15)) + _bars(DAYS[1:2], time(7), time(15))
        ts += _bars([DAYS[2]], time(7), time(11))
        assert _find(_df(ts, "NQU6")).gaps == []

    def test_wrap_around_window(self):
        """Plage 17:00-04:00 : une barre à 02:00 appartient à la session de la veille."""
        ts: list[datetime] = []
        for d in [date(2026, 9, 9), date(2026, 9, 10), date(2026, 9, 11)]:
            ts += _bars([d], time(17), time(23, 59, 59))
            ts += _bars([d + timedelta(days=1)], time(0), time(4))
        hole_start = datetime(2026, 9, 11, 1, 0, tzinfo=CHI).astimezone(UTC)
        hole_end = datetime(2026, 9, 11, 2, 0, tzinfo=CHI).astimezone(UTC)
        ts = [t for t in ts if not hole_start <= t < hole_end]

        report = _find(
            _df(ts, "NQU6"), intraday_begin=time(17), intraday_end=time(4), min_gap_minutes=30
        )
        assert report.sessions_checked == 3
        assert len(report.gaps) == 1
        assert report.gaps[0].session == date(2026, 9, 10)
        assert report.gaps[0].minutes == 60

    def test_empty_weekday_sessions_listed(self):
        """Mardi 15 sans barre (férié probable) listé ; le week-end ne l'est pas."""
        days = [date(2026, 9, 11), date(2026, 9, 14), date(2026, 9, 16)]
        report = _find(_df(_bars(days, time(7), time(15)), "NQU6"))
        assert report.gaps == []
        assert report.empty_sessions == [date(2026, 9, 15)]

    def test_start_end_filter_sessions(self):
        nq = _df(_bars(DAYS, time(4), time(16), holes={HOLE_DAY: [(time(12), time(14))]}), "NQU6")
        assert _find(nq, start=date(2026, 9, 14)).gaps == []
        assert _find(nq, end=date(2026, 9, 10)).gaps == []
        # Session unique : les bords sont ignorés, mais un trou interne reste signalé
        assert len(_find(nq, start=HOLE_DAY, end=HOLE_DAY).gaps) == 1

    def test_invalid_parameters(self):
        with pytest.raises(ValueError):
            _find(_df([], "NQU6"), intraday_begin=time(7), intraday_end=time(7))
        with pytest.raises(ValueError):
            _find(_df([], "NQU6"), min_gap_minutes=0)


class TestQualityConfig:
    def test_defaults(self, tmp_settings):
        assert tmp_settings.quality_intraday_begin == time(7)
        assert tmp_settings.quality_intraday_end == time(15)
        assert tmp_settings.quality_min_gap_minutes == 5

    def test_load_from_toml(self, tmp_path: Path):
        cfg = tmp_path / "config.toml"
        cfg.write_text(
            '[quality]\nintraday_begin = "08:30"\nintraday_end = "15:15"\nmin_gap_minutes = 10\n',
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert s.quality_intraday_begin == time(8, 30)
        assert s.quality_intraday_end == time(15, 15)
        assert s.quality_min_gap_minutes == 10


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


class TestDoctorGapsCli:
    @pytest.fixture
    def seeded(self, tmp_settings, monkeypatch):
        _seed(tmp_settings, "ES", _bars(DAYS, time(4), time(16)))
        _seed(
            tmp_settings,
            "NQ",
            _bars(DAYS, time(4), time(16), holes={HOLE_DAY: [(time(12), time(14))]}),
        )
        monkeypatch.setattr("myquantstore.cli.load_settings", lambda *a, **k: tmp_settings)
        return tmp_settings

    def test_confirmed_gap_exit_1(self, seeded, capsys):
        rc = main(["doctor", "gaps", "--timezone", TZ])
        out = capsys.readouterr().out
        assert rc == 1
        assert "futures:NQ" in out
        assert "CONFIRMÉ (ES)" in out
        assert "120 min" in out

    def test_cli_window_overrides_config(self, seeded, capsys):
        """Plage CLI hors du trou → rien à signaler, exit 0."""
        rc = main(
            [
                "doctor",
                "gaps",
                "-i",
                "NQ",
                "--timezone",
                TZ,
                "--intraday-begin",
                "07:00",
                "--intraday-end",
                "11:00",
            ]
        )
        assert rc == 0
        assert "0 trou(s) confirmé(s)" in capsys.readouterr().out

    def test_config_fallback(self, seeded, capsys):
        """Sans flag : [quality] (ici 13:00-15:00, seuil 90 min → trou de 60 min ignoré)."""
        seeded.quality_intraday_begin = time(13)
        seeded.quality_min_gap_minutes = 90
        rc = main(["doctor", "gaps", "--timezone", TZ])
        assert rc == 0
        assert "plage 13:00–15:00 · seuil 90 min" in capsys.readouterr().out

    def test_invalid_time_flag(self, seeded, capsys):
        assert main(["doctor", "gaps", "--intraday-begin", "7h"]) == 1
        assert "Erreur" in capsys.readouterr().out
