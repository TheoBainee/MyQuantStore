"""Tests du module query/timezone.py (résolution centralisée)."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from myquantstore.instruments import Instrument, InstrumentType
from myquantstore.query.timezone import (
    DEFAULT_TIMEZONE,
    ensure_window_start_utc,
    localize_window_start,
    resolve_timezone,
    validate_iana,
    window_start_to_utc_naive,
)


class TestValidateIana:
    def test_valid(self):
        assert validate_iana("America/Chicago") == "America/Chicago"
        assert validate_iana("UTC") == "UTC"

    def test_empty_defaults_utc(self):
        assert validate_iana("") == DEFAULT_TIMEZONE
        assert validate_iana("  ") == DEFAULT_TIMEZONE

    def test_invalid(self):
        with pytest.raises(ValueError, match="IANA invalide"):
            validate_iana("Not/A_Zone")


class TestResolveTimezone:
    def test_override_wins(self, tmp_settings):
        tmp_settings.chart_timezone = "Europe/Paris"
        assert (
            resolve_timezone(tmp_settings, override="America/Chicago") == "America/Chicago"
        )

    def test_conf_when_no_override(self, tmp_settings):
        tmp_settings.chart_timezone = "Europe/Paris"
        assert resolve_timezone(tmp_settings) == "Europe/Paris"

    def test_default_utc(self, tmp_settings):
        tmp_settings.chart_timezone = "UTC"
        assert resolve_timezone(tmp_settings, override=None) == "UTC"

    def test_instrument_hook_noop_for_now(self, tmp_settings):
        """Stub : instrument ignoré tant que pas de TZ par instrument."""
        tmp_settings.chart_timezone = "UTC"
        inst = Instrument(type=InstrumentType.FUTURES, symbol="ES")
        assert resolve_timezone(tmp_settings, inst) == "UTC"

    def test_override_invalid_raises(self, tmp_settings):
        with pytest.raises(ValueError, match="IANA invalide"):
            resolve_timezone(tmp_settings, override="Fake/Zone")


class TestEnsureAndLocalize:
    def test_naive_becomes_utc(self):
        df = pl.DataFrame(
            {"window_start": [datetime(2024, 7, 15, 14, 30)]}
        ).with_columns(pl.col("window_start").cast(pl.Datetime("ns")))
        out = ensure_window_start_utc(df)
        assert out.schema["window_start"].time_zone == "UTC"

    def test_localize_1min_chicago(self):
        df = pl.DataFrame(
            {"window_start": [datetime(2024, 7, 15, 14, 30, tzinfo=UTC)]}
        ).with_columns(pl.col("window_start").cast(pl.Datetime("ns", time_zone="UTC")))
        out = localize_window_start(df, "America/Chicago", is_extraday=False)
        assert out.schema["window_start"].time_zone == "America/Chicago"
        # 14:30 UTC = 09:30 CDT
        assert out["window_start"][0].hour == 9
        assert out["window_start"][0].minute == 30

    def test_localize_1day_stays_utc(self):
        df = pl.DataFrame(
            {"window_start": [datetime(2024, 1, 2, 0, 0, tzinfo=UTC)]}
        ).with_columns(pl.col("window_start").cast(pl.Datetime("ns", time_zone="UTC")))
        out = localize_window_start(df, "America/Chicago", is_extraday=True)
        assert out.schema["window_start"].time_zone == "UTC"
        assert out["window_start"][0].hour == 0

    def test_window_start_to_utc_naive(self):
        df = pl.DataFrame(
            {"window_start": [datetime(2024, 7, 15, 9, 30)]}
        ).with_columns(
            pl.col("window_start")
            .cast(pl.Datetime("ns"))
            .dt.replace_time_zone("America/Chicago")
        )
        out = window_start_to_utc_naive(df)
        assert out.schema["window_start"].time_zone is None
        # 09:30 CDT = 14:30 UTC
        assert out["window_start"][0].hour == 14
