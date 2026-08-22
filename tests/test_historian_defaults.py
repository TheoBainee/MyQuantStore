"""Tests défauts historian / run_fetch."""

from __future__ import annotations

from myquantstore.instruments import RESOLUTION_1DAY, RESOLUTION_1MIN
from myquantstore.pipeline.historian import resolve_fetch_resolutions, run_fetch


def test_resolve_all_is_dual(tmp_settings):
    assert resolve_fetch_resolutions(tmp_settings, "all") == [RESOLUTION_1MIN, RESOLUTION_1DAY]


def test_run_fetch_default_resolutions_dual(tmp_settings, monkeypatch):
    """resolutions=None → 1min + 1day (aligné CLI all)."""
    seen: list[str] = []

    def fake_one(instrument, settings, client, resolution, *, force, dry_run):
        seen.append(resolution)
        return {"status": "skipped", "instrument": str(instrument), "resolution": resolution}

    monkeypatch.setattr("myquantstore.pipeline.historian._fetch_one", fake_one)

    class _C:
        pass

    from myquantstore.instruments import Instrument, InstrumentType

    inst = Instrument(InstrumentType.STOCKS, "AAPL")
    run_fetch(tmp_settings, _C(), instruments=[inst], dry_run=True)  # type: ignore[arg-type]
    assert seen == [RESOLUTION_1MIN, RESOLUTION_1DAY]
