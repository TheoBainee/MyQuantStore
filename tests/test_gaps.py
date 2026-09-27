"""Tests de l'audit des trous 1min (storage/gaps.py + ``doctor gaps``)."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
import pytest

from myquantstore.cli import main
from myquantstore.config import load_settings
from myquantstore.instruments import Instrument, InstrumentType
from myquantstore.pipeline.aggregator import aggregate
from myquantstore.storage.gaps import audit_gaps, confirm_gaps, find_gaps, unique_timestamps
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


def _oracle(
    bars: dict[str, list[datetime]],
    key: str,
    begin: time,
    end: time,
    min_gap: int,
) -> list[tuple[date, datetime, datetime, str, tuple[str, ...]]]:
    """Référence minute par minute, indépendante de l'implémentation vectorisée."""
    wrap = begin > end
    present = set(bars[key])
    sessions: set[date] = set()
    for t in present:
        local = t.astimezone(CHI)
        inside = (
            (local.time() >= begin or local.time() < end) if wrap else begin <= local.time() < end
        )
        if inside:
            sessions.add(
                local.date() - timedelta(days=1) if wrap and local.time() < end else local.date()
            )
    first, last = min(sessions), max(sessions)
    out = []
    for sess in sorted(sessions):
        lo = datetime.combine(sess, begin, tzinfo=CHI).astimezone(UTC)
        hi = datetime.combine(
            sess + timedelta(days=1) if wrap else sess, end, tzinfo=CHI
        ).astimezone(UTC)
        minutes = []
        t = lo
        while t < hi:
            minutes.append(t)
            t += timedelta(minutes=1)
        run: list[datetime] = []
        for m in [*minutes, None]:
            if m is not None and m not in present:
                run.append(m)
                continue
            if len(run) >= min_gap:
                g_start, g_end = run[0], run[-1] + timedelta(minutes=1)
                position = "début" if g_start == lo else "fin" if g_end == hi else "milieu"
                skip = (position == "début" and sess == first) or (
                    position == "fin" and sess == last
                )
                if not skip:
                    peers = tuple(
                        k
                        for k, ts in bars.items()
                        if k != key and any(g_start <= x < g_end for x in ts)
                    )
                    out.append((sess, g_start, g_end, position, peers))
            run = []
    return sorted(out, key=lambda g: g[1])


class TestAuditGapsOracle:
    """audit_gaps (lazy, collect_all, join_asof) = oracle minute par minute."""

    @pytest.mark.parametrize("seed", [0, 1, 2])
    @pytest.mark.parametrize(
        ("begin", "end", "min_gap"),
        [(time(7), time(15), 5), (time(4), time(16), 1), (time(17), time(4), 3)],
    )
    def test_matches_oracle(self, seed, begin, end, min_gap):
        rng = np.random.default_rng(seed)
        # 5 au 13 mars 2026 : inclut le passage à l'heure d'été US (8 mars)
        base = datetime(2026, 3, 5, tzinfo=UTC)
        grid = [base + timedelta(minutes=i) for i in range(9 * 24 * 60)]
        grid = [t for t in grid if t.astimezone(CHI).weekday() < 5]
        bars: dict[str, list[datetime]] = {}
        for sym, blocks in [("ES", 3), ("NQ", 12), ("RTY", 25)]:
            keep = np.ones(len(grid), bool)
            for _ in range(blocks):
                a = int(rng.integers(0, len(grid)))
                keep[a : a + int(rng.integers(1, 90))] = False
            bars[f"futures:{sym}"] = [t for t, k in zip(grid, keep, strict=True) if k]
        frames = {k: _df(ts, k.split(":")[1] + "H6") for k, ts in bars.items()}
        reports = audit_gaps(
            frames,
            dict.fromkeys(frames, TZ),
            {k: list(frames) for k in frames},
            intraday_begin=begin,
            intraday_end=end,
            min_gap_minutes=min_gap,
        )
        for key in frames:
            got = [
                (g.session, g.start, g.end, g.position, tuple(g.confirmed_by))
                for g in reports[key].gaps
            ]
            assert got == _oracle(bars, key, begin, end, min_gap), key

    def test_lazy_input_and_period_pushdown(self, tmp_path):
        """scan_parquet (lazy) + start/end = même résultat que le DataFrame en mémoire."""
        holes = {HOLE_DAY: [(time(12), time(14))]}
        nq = _df(_bars(DAYS, time(4), time(16), holes=holes), "NQU6")
        es = _df(_bars(DAYS, time(4), time(16)), "ESU6")
        paths = {}
        for key, df in {"futures:NQ": nq, "futures:ES": es}.items():
            paths[key] = tmp_path / f"{key.split(':')[1]}.parquet"
            df.write_parquet(paths[key])
        kw = {
            "intraday_begin": time(7),
            "intraday_end": time(15),
            "min_gap_minutes": 5,
            "start": HOLE_DAY,
            "end": HOLE_DAY,
        }
        targets = {"futures:NQ": TZ}
        peers = {"futures:NQ": ["futures:NQ", "futures:ES"]}
        lazy = audit_gaps({k: pl.scan_parquet(p) for k, p in paths.items()}, targets, peers, **kw)
        eager = audit_gaps({"futures:NQ": nq, "futures:ES": es}, targets, peers, **kw)
        assert lazy["futures:NQ"] == eager["futures:NQ"]
        assert [g.confirmed_by for g in lazy["futures:NQ"].gaps] == [["futures:ES"]]


class TestQualityConfig:
    def test_defaults(self, tmp_settings):
        assert tmp_settings.quality_min_gap_minutes == 5
        assert tmp_settings.data_quality_trigger == 0.1

    def test_load_from_toml(self, tmp_path: Path):
        cfg = tmp_path / "config.toml"
        cfg.write_text(
            "[quality]\ndata_quality_trigger = 0.2\nmin_gap_minutes = 10\n",
            encoding="utf-8",
        )
        s = load_settings(cfg)
        assert s.data_quality_trigger == 0.2
        assert s.quality_min_gap_minutes == 10

    def test_min_gap_minutes_ge_1(self, tmp_path: Path):
        cfg = tmp_path / "config.toml"
        cfg.write_text("[quality]\nmin_gap_minutes = 0\n", encoding="utf-8")
        with pytest.raises(Exception, match="min_gap_minutes"):
            load_settings(cfg)


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
        # Plage auditée = [chart] intraday_begin / intraday_end
        tmp_settings.chart_intraday_begin = time(7)
        tmp_settings.chart_intraday_end = time(15)
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
        """Sans flag : plage [chart] (13:00-15:00) + seuil [quality] (90 min → trou de 60 min ignoré)."""
        seeded.chart_intraday_begin = time(13)
        seeded.quality_min_gap_minutes = 90
        rc = main(["doctor", "gaps", "--timezone", TZ])
        assert rc == 0
        assert "plage 13:00–15:00 · seuil 90 min" in capsys.readouterr().out

    def test_missing_window_error(self, seeded, capsys):
        """Ni flags ni [chart] intraday_begin/end → erreur explicite."""
        seeded.chart_intraday_begin = None
        seeded.chart_intraday_end = None
        assert main(["doctor", "gaps", "--timezone", TZ]) == 1
        assert "plage horaire requise" in capsys.readouterr().out

    def test_invalid_time_flag(self, seeded, capsys):
        assert main(["doctor", "gaps", "--intraday-begin", "7h"]) == 1
        assert "Erreur" in capsys.readouterr().out
