"""Déterminisme de ``query()`` et parité CLI ``query`` ↔ serve ``/v1/query``.

Les ``join`` / ``unique`` Polars ne garantissent pas l'ordre des lignes : il peut
changer d'un appel à l'autre selon les threads (beaucoup de cœurs + millions de
lignes = écarts fréquents, rares sur 2 cœurs). Pour ne pas dépendre de la
machine, la fixture ``shuffled_polars`` **mélange** la sortie de chaque
``join`` / ``unique`` sans ``maintain_order`` : ``query()`` doit rendre
strictement la même réponse que sans mélange. Avant correctif, toutes les
variantes resamplées (5min, 1h, intraday, forward fill…) échouaient.
"""

from __future__ import annotations

import itertools
from datetime import UTC, date, datetime, time, timedelta
from io import BytesIO

import polars as pl
import pytest
from fastapi.testclient import TestClient
from polars.testing import assert_frame_equal

from myquantstore.chains import build_local_chain
from myquantstore.cli import main
from myquantstore.pipeline.aggregator import aggregate
from myquantstore.pipeline.fetchers.futures import _determine_segment_range
from myquantstore.query.adjust import apply_split_adjustment
from myquantstore.query.reader import query
from myquantstore.serve.server import create_serve_app
from myquantstore.storage.parquet_io import write_parquet
from myquantstore.storage.raw_dumps import save_raw_dump
from tests.test_roll_boundary import _cme_session_bars, _contracts

TZ = "America/Chicago"
_TICKERS = ["ESU6", "ESZ6"]
_EXPIRIES = [date(2026, 9, 18), date(2026, 12, 18)]


def _assert_identical(reference: pl.DataFrame, current: pl.DataFrame) -> None:
    assert_frame_equal(reference, current, check_exact=True, categorical_as_str=True)


def _write_contracts(settings, *, age_days: int = 0) -> None:
    write_parquet(
        _contracts(_TICKERS, _EXPIRIES),
        settings.contracts_cache_path("ES"),
        product_code="ES",
        last_fetched_at=(datetime.now(UTC) - timedelta(days=age_days)).isoformat(),
    )


@pytest.fixture
def es_data(tmp_settings, es_instrument, monkeypatch):
    """Roll ESU6 → ESZ6 (sept. 2026) aux bornes réelles du fetch, prix variant à la minute."""
    from myquantstore.contracts.rollover import RolloverChain

    rchain = RolloverChain("ES", _contracts(_TICKERS, _EXPIRIES), days_before_expiry=7)
    for seg in rchain.segments:
        gte, lte = _determine_segment_range(
            seg, date(2026, 9, 2), date(2026, 9, 18), None, None, tmp_settings
        )
        assert gte is not None and lte is not None
        bars = _cme_session_bars(seg.ticker, date.fromisoformat(gte), date.fromisoformat(lte), 0.0)
        n = bars.height
        step = pl.Series([6000.0 + (i % 97) * 0.25 for i in range(n)])
        bars = bars.with_columns(
            step.alias("open"),
            (step + 1).alias("high"),
            (step - 1).alias("low"),
            (step + 0.5).alias("close"),
            step.alias("settlement_price"),
            pl.Series([1 + i % 13 for i in range(n)]).alias("volume"),
        )
        save_raw_dump(bars, es_instrument, seg.ticker, "20260918T230000", tmp_settings)
    aggregate(es_instrument, tmp_settings)
    _write_contracts(tmp_settings)
    tmp_settings.chart_timezone = TZ
    monkeypatch.setattr("myquantstore.cli.load_settings", lambda *a, **k: tmp_settings)
    return tmp_settings, es_instrument, rchain


@pytest.fixture
def shuffled_polars(monkeypatch):
    """Mélange la sortie des ``join`` / ``unique`` dont l'ordre n'est pas garanti."""
    seeds = itertools.count(1)
    original_join, original_unique = pl.DataFrame.join, pl.DataFrame.unique

    def join(self, *args, **kwargs):
        out = original_join(self, *args, **kwargs)
        if kwargs.get("maintain_order") not in (None, "none"):
            return out
        return out.sample(fraction=1.0, shuffle=True, seed=next(seeds))

    def unique(self, *args, **kwargs):
        out = original_unique(self, *args, **kwargs)
        if kwargs.get("maintain_order"):
            return out
        return out.sample(fraction=1.0, shuffle=True, seed=next(seeds))

    def activate() -> None:
        monkeypatch.setattr(pl.DataFrame, "join", join)
        monkeypatch.setattr(pl.DataFrame, "unique", unique)

    return activate


_VARIANTS = {
    "1min": {},
    "1min_no_dedup": {"dedup_timestamps": False},
    "5min": {"k_minutes": 5},
    "1hour": {"k_minutes": 60},
    "5min_no_dedup": {"k_minutes": 5, "dedup_timestamps": False},
    "5min_forward_fill": {"k_minutes": 5, "forward_fill": True},
    "1min_forward_fill": {"forward_fill": True},
    "intraday_15min": {
        "k_minutes": 15,
        "intraday_begin": time(8, 30),
        "intraday_end": time(15, 15),
    },
    "intraday_15min_forward_fill": {
        "k_minutes": 15,
        "intraday_begin": time(8, 30),
        "intraday_end": time(15, 15),
        "forward_fill": True,
    },
}
_CHAIN_ONLY = {
    "adjust_5min": {"adjust_rollover": True, "k_minutes": 5},
    "normalize_tick_size_5min": {"normalize_tick_size": True, "k_minutes": 5},
}


class TestQueryIgnoresPolarsRowOrder:
    @pytest.mark.parametrize("with_chain", [True, False], ids=["chain", "no_chain"])
    @pytest.mark.parametrize("kwargs", _VARIANTS.values(), ids=_VARIANTS.keys())
    def test_variant(self, es_data, shuffled_polars, with_chain, kwargs):
        settings, es, chain = es_data
        used_chain = chain if with_chain else None
        reference = query(es, settings, chain=used_chain, timezone=TZ, **kwargs)
        assert reference.height > 0
        shuffled_polars()
        for _ in range(3):
            _assert_identical(
                reference, query(es, settings, chain=used_chain, timezone=TZ, **kwargs)
            )

    @pytest.mark.parametrize("kwargs", _CHAIN_ONLY.values(), ids=_CHAIN_ONLY.keys())
    def test_chain_only_variant(self, es_data, shuffled_polars, kwargs):
        settings, es, chain = es_data
        reference = query(es, settings, chain=chain, timezone=TZ, **kwargs)
        shuffled_polars()
        for _ in range(3):
            _assert_identical(reference, query(es, settings, chain=chain, timezone=TZ, **kwargs))

    def test_split_adjustment_keeps_row_order(self, shuffled_polars):
        ts = [datetime(2026, 9, d, 14, 30) for d in range(1, 11)]
        df = pl.DataFrame({"window_start": ts, "open": [float(i) for i in range(10)]})
        splits = pl.DataFrame(
            {"execution_date": [date(2026, 9, 5)], "historical_adjustment_factor": [0.5]}
        )
        reference = apply_split_adjustment(df, splits)
        shuffled_polars()
        _assert_identical(reference, apply_split_adjustment(df, splits))


def _serve_query(settings, params: dict[str, object]) -> pl.DataFrame:
    resp = TestClient(create_serve_app(settings)).get("/v1/query", params=params)
    assert resp.status_code == 200, resp.text
    return pl.read_parquet(BytesIO(resp.content))


_PARITY_CASES = {
    "1min": ({}, []),
    "5min": ({"timescale_nb": 5}, ["--timescale-nb", "5"]),
    "1hour": ({"timescale_unit": "hour", "timescale_nb": 1}, ["--timescale-unit", "hour"]),
    "no_dedup": ({"dedup_timestamps": False}, ["--no-dedup-timestamps"]),
    "intraday_forward_fill": (
        {
            "timescale_nb": 15,
            "intraday_begin": "08:30",
            "intraday_end": "15:15",
            "forward_fill": True,
        },
        [
            "--timescale-nb",
            "15",
            "--intraday-begin",
            "08:30",
            "--intraday-end",
            "15:15",
            "--forward-fill",
        ],
    ),
    "window_utc": (
        {"start": "2026-09-10", "end": "2026-09-15", "timezone": "UTC"},
        ["--start", "2026-09-10", "--end", "2026-09-15", "--timezone", "UTC"],
    ),
}


class TestServeCliParity:
    @pytest.mark.parametrize(("params", "flags"), _PARITY_CASES.values(), ids=_PARITY_CASES.keys())
    def test_serve_equals_query_and_cli(self, es_data, tmp_path, params, flags):
        settings, es, _ = es_data
        served = _serve_query(settings, {"instrument": "ES", **params})
        # query() direct, même chaîne locale que serve
        from myquantstore.cli import _timescale_to_query_params
        from myquantstore.query.reader import parse_query_datetime

        unit = str(params.get("timescale_unit", "min"))
        resolution, k_minutes, k_days = _timescale_to_query_params(
            unit, int(params.get("timescale_nb", 1))
        )
        direct = query(
            es,
            settings,
            build_local_chain(es, settings),
            start=parse_query_datetime(str(params["start"])) if "start" in params else None,
            end=parse_query_datetime(str(params["end"]), is_end=True) if "end" in params else None,
            k_minutes=k_minutes,
            k_days=k_days,
            resolution=resolution,
            intraday_begin=time.fromisoformat(str(params["intraday_begin"]))
            if "intraday_begin" in params
            else None,
            intraday_end=time.fromisoformat(str(params["intraday_end"]))
            if "intraday_end" in params
            else None,
            timezone=params.get("timezone"),  # type: ignore[arg-type]
            dedup_timestamps=bool(params.get("dedup_timestamps", True)),
            forward_fill=bool(params.get("forward_fill", False)),
        )
        _assert_identical(direct, served)
        output = tmp_path / "cli.parquet"
        assert main(["query", "ES", "--no-cascade", "--output", str(output), *flags]) == 0
        _assert_identical(served, pl.read_parquet(output))

    def test_cli_no_cascade_reads_stale_contracts_cache_like_serve(self, es_data, tmp_path):
        """Régression : ``query --no-cascade`` plantait sur un cache contrats périmé."""
        settings, _, _ = es_data
        _write_contracts(settings, age_days=400)
        served = _serve_query(settings, {"instrument": "ES", "timescale_nb": 5})
        output = tmp_path / "cli.parquet"
        assert (
            main(["query", "ES", "--no-cascade", "--timescale-nb", "5", "--output", str(output)])
            == 0
        )
        _assert_identical(served, pl.read_parquet(output))

    def test_cli_no_cascade_without_contracts_cache(self, es_data, tmp_path, capsys):
        settings, _, _ = es_data
        settings.contracts_cache_path("ES").unlink()
        output = tmp_path / "cli.parquet"
        assert main(["query", "ES", "--no-cascade", "--output", str(output)]) == 0
        assert "pas de cache contrats local" in capsys.readouterr().out
        _assert_identical(_serve_query(settings, {"instrument": "ES"}), pl.read_parquet(output))
