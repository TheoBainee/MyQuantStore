"""Tests de ``myquantstore doctor stability`` (stabilité aggregate → query, données réelles)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from myquantstore.cli import main
from myquantstore.instruments import RESOLUTION_1MIN, Instrument, InstrumentType
from myquantstore.pipeline.aggregator import aggregate
from myquantstore.storage.raw_dumps import save_raw_dump
from myquantstore.storage.stability import (
    DISK_IDENTICAL,
    DISK_STALE,
    check_stability,
    describe_difference,
    query_variants,
)
from tests.test_aggregate_stability import _minute_bars, _seed_es_roll, _write_contracts_cache

TZ = "America/Chicago"


def _snapshot(root: Path) -> dict[str, tuple[int, bytes]]:
    """Contenu + mtime de chaque fichier sous ``root`` (preuve de lecture seule)."""
    return {
        str(p.relative_to(root)): (p.stat().st_mtime_ns, p.read_bytes())
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


@pytest.fixture
def seeded(tmp_settings, es_instrument, sample_contracts_df, monkeypatch):
    """ES : roll ESH5 → ESM5 + refetch, cache contrats local, agrégat à jour sur disque."""
    _seed_es_roll(tmp_settings, es_instrument)
    _write_contracts_cache(tmp_settings, sample_contracts_df)
    aggregate(es_instrument, tmp_settings)
    monkeypatch.setattr("myquantstore.cli.load_settings", lambda *a, **k: tmp_settings)
    return tmp_settings


def _shuffle_writes(monkeypatch) -> None:
    """Simule un agrégateur instable : ordre des lignes différent à chaque écriture."""
    import myquantstore.pipeline.aggregator as aggregator_module

    original = aggregator_module.write_aggregate
    calls = {"n": 0}

    def shuffled(df, *args, **kwargs):
        calls["n"] += 1
        return original(df.sample(fraction=1.0, shuffle=True, seed=calls["n"]), *args, **kwargs)

    monkeypatch.setattr(aggregator_module, "write_aggregate", shuffled)


class TestDoctorStabilityCli:
    def test_stable_data_exit_0(self, seeded, capsys):
        rc = main(["doctor", "stability", "--timeframe", "1min", "--timezone", TZ])
        out = capsys.readouterr().out
        assert rc == 0
        assert "futures:ES" in out
        assert "10/10 stables" in out  # 5 variantes avec chaîne + 5 sans chaîne
        assert "identique" in out
        assert "INSTABLE" not in out

    def test_real_data_is_never_written(self, seeded, capsys):
        data_dir = Path(seeded.data_dir)
        before = _snapshot(data_dir)
        assert main(["doctor", "stability", "--repeats", "2", "--timezone", TZ]) == 0
        assert _snapshot(data_dir) == before

    def test_instability_detected_exit_1(self, seeded, monkeypatch, capsys):
        _shuffle_writes(monkeypatch)
        rc = main(["doctor", "stability", "-i", "ES", "--timeframe", "1min", "--timezone", TZ])
        out = capsys.readouterr().out
        assert rc == 1
        assert "INSTABLE" in out
        assert "agrégat · reconstruction 2" in out
        assert "ligne(s) diffèrent" in out

    def test_query_only_instability_is_attributed_to_query(self, seeded, monkeypatch, capsys):
        import myquantstore.storage.stability as stability_module

        original = stability_module.query
        calls = {"n": 0}

        def unstable_query(*args, **kwargs):
            calls["n"] += 1
            df = original(*args, **kwargs)
            return df.sample(fraction=1.0, shuffle=True, seed=calls["n"])

        monkeypatch.setattr(stability_module, "query", unstable_query)
        rc = main(["doctor", "stability", "-i", "ES", "--timeframe", "1min", "--timezone", TZ])
        out = capsys.readouterr().out
        assert rc == 1
        assert "stable" in out and "10/10 instables" in out
        assert "l'écart vient de query()" in out

    def test_stale_aggregate_on_disk_is_warn_not_failure(self, seeded, es_instrument, capsys):
        save_raw_dump(
            _minute_bars("ESM5", date(2025, 3, 13), date(2025, 3, 13), 5860.0),
            es_instrument,
            "ESM5",
            "20250314T080000",
            seeded,
        )
        rc = main(["doctor", "stability", "--timeframe", "1min", "--timezone", TZ])
        out = capsys.readouterr().out
        assert rc == 0
        assert "WARN" in out
        assert "périmé" in out
        assert "myquantstore aggregate" in out

    def test_nothing_to_check(self, tmp_settings, monkeypatch, capsys):
        monkeypatch.setattr("myquantstore.cli.load_settings", lambda *a, **k: tmp_settings)
        assert main(["doctor", "stability"]) == 0
        assert "rien à vérifier" in capsys.readouterr().out

    def test_repeats_below_two_rejected(self, seeded, capsys):
        assert main(["doctor", "stability", "--repeats", "1"]) == 1
        assert "--repeats doit être >= 2" in capsys.readouterr().out

    def test_invalid_timeframe_rejected(self, seeded, capsys):
        assert main(["doctor", "stability", "--timeframe", "3min"]) == 1


class TestCheckStability:
    def test_report_and_disk_status(self, seeded, es_instrument, sample_chain):
        report = check_stability(
            es_instrument, seeded, RESOLUTION_1MIN, sample_chain, repeats=2, timezone=TZ
        )
        assert report.ok
        assert report.rows > 0
        assert report.disk_status == DISK_IDENTICAL
        assert "chaîne · 1min" in report.variants
        assert "sans chaîne · 1min" in report.variants

    def test_query_window_is_forwarded(self, seeded, es_instrument, sample_chain, monkeypatch):
        seen: list[tuple[object, object]] = []

        import myquantstore.storage.stability as stability_module

        original = stability_module.query

        def spy(*args, **kwargs):
            seen.append((kwargs["start"], kwargs["end"]))
            return original(*args, **kwargs)

        monkeypatch.setattr(stability_module, "query", spy)
        start = datetime(2025, 3, 10, tzinfo=UTC)
        end = datetime(2025, 3, 11, 23, 59, tzinfo=UTC)
        check_stability(
            es_instrument, seeded, RESOLUTION_1MIN, sample_chain, repeats=2, start=start, end=end
        )
        assert seen and all(s == (start, end) for s in seen)

    def test_stale_detail(self, seeded, es_instrument):
        save_raw_dump(
            _minute_bars("ESM5", date(2025, 3, 13), date(2025, 3, 13), 5860.0),
            es_instrument,
            "ESM5",
            "20250314T080000",
            seeded,
        )
        report = check_stability(es_instrument, seeded, RESOLUTION_1MIN, None, repeats=2)
        assert report.ok
        assert report.disk_status == DISK_STALE
        assert "lignes" in report.disk_detail

    def test_repeats_below_two_raises(self, seeded, es_instrument):
        with pytest.raises(ValueError, match="repeats"):
            check_stability(es_instrument, seeded, RESOLUTION_1MIN, None, repeats=1)


class TestVariantsAndDiff:
    def test_stocks_variants_are_offline(self):
        aapl = Instrument(type=InstrumentType.STOCKS, symbol="AAPL")
        variants = query_variants(aapl, "1day", None)
        assert set(variants) == {"1day", "5 jours", "semaine"}
        assert all(kwargs["no_split"] for _, kwargs in variants.values())

    def test_futures_without_chain_has_only_no_chain_variants(self, es_instrument):
        variants = query_variants(es_instrument, RESOLUTION_1MIN, None)
        assert variants and all(name.startswith("sans chaîne · ") for name in variants)

    def test_describe_difference_locates_first_row(self):
        ts = [datetime(2026, 9, 14, 13, m, tzinfo=UTC) for m in range(3)]
        ref = pl.DataFrame({"window_start": ts, "close": [1.0, 2.0, 3.0]})
        cur = pl.DataFrame({"window_start": ts, "close": [1.0, 2.5, 3.5]})
        detail = describe_difference(ref, cur)
        assert detail is not None
        assert detail.startswith("2 ligne(s) diffèrent")
        assert "2026-09-14 13:01:00" in detail
        assert "colonnes : close" in detail
        assert describe_difference(ref, ref.clone()) is None

    def test_describe_difference_row_count(self):
        ref = pl.DataFrame({"x": [1, 2, 3]})
        assert describe_difference(ref, ref.head(2)) == "3 → 2 lignes"
