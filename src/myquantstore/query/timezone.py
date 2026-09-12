"""Résolution et localisation timezone pour query / chart / serve.

**Source de vérité unique** : toutes les surfaces (CLI, serve, chart, analytics)
passent par :func:`resolve_timezone` et :func:`localize_window_start`. Aucune
commande ne doit faire ``or settings.chart_timezone or "UTC"`` en propre.

**Precedence** de :func:`resolve_timezone` :

1. ``override`` explicite (CLI ``--timezone``, serve ``?timezone=``)
2. TZ instrument — *stub* pour l'instant (extension future)
3. ``settings.chart_timezone``
4. :data:`DEFAULT_TIMEZONE` (``UTC``)

``intraday_begin`` / ``intraday_end`` sont toujours des heures murales dans le
fuseau résolu. Plus tard, une TZ par instrument s'insère à l'étape 2 sans
retoucher CLI/serve/chart.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import polars as pl

if TYPE_CHECKING:
    from myquantstore.config import Settings
    from myquantstore.instruments import Instrument

DEFAULT_TIMEZONE = "UTC"


def validate_iana(name: str) -> str:
    """Valide un nom IANA ; lève ``ValueError`` si invalide."""
    cleaned = str(name or "").strip() or DEFAULT_TIMEZONE
    try:
        ZoneInfo(cleaned)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(
            f"timezone IANA invalide: {cleaned!r} "
            f"(ex: UTC, America/Chicago, Europe/Paris)"
        ) from exc
    return cleaned


def resolve_timezone(
    settings: Settings,
    instrument: Instrument | None = None,
    *,
    override: str | None = None,
) -> str:
    """Résout le fuseau IANA applicable (override → instrument → conf → UTC).

    :param settings: Configuration (``chart_timezone`` = défaut global actuel).
    :param instrument: Instrument cible (hook futur TZ par instrument).
    :param override: Fuseau forcé par l'appelant (CLI/serve), ou ``None``.
    """
    if override is not None and str(override).strip():
        return validate_iana(str(override).strip())

    # Extension future : TZ par instrument (config / modèle).
    # if instrument is not None:
    #     inst_tz = ...
    #     if inst_tz:
    #         return validate_iana(inst_tz)
    _ = instrument

    conf = getattr(settings, "chart_timezone", None)
    if conf is not None and str(conf).strip():
        return validate_iana(str(conf).strip())
    return DEFAULT_TIMEZONE


def _ws_time_zone(df: pl.DataFrame) -> str | None:
    dtype = df.schema.get("window_start")
    return getattr(dtype, "time_zone", None) if dtype is not None else None


def ensure_window_start_utc(df: pl.DataFrame) -> pl.DataFrame:
    """Normalise ``window_start`` en ``Datetime[..., UTC]`` (naive = UTC)."""
    if df.is_empty() or "window_start" not in df.columns:
        return df
    tz = _ws_time_zone(df)
    if tz is None:
        return df.with_columns(
            pl.col("window_start").dt.replace_time_zone(DEFAULT_TIMEZONE).alias("window_start")
        )
    if tz == DEFAULT_TIMEZONE:
        return df
    return df.with_columns(
        pl.col("window_start").dt.convert_time_zone(DEFAULT_TIMEZONE).alias("window_start")
    )


def localize_window_start(
    df: pl.DataFrame,
    timezone: str,
    *,
    is_extraday: bool,
) -> pl.DataFrame:
    """Localise ``window_start`` pour la sortie de ``query()``.

    - Track **1day** (extraday) : toujours ``Datetime[..., UTC]`` (minuit séance).
    - Track **1min** : convert vers ``timezone`` (même fuseau que intraday).
    """
    if df.is_empty() or "window_start" not in df.columns:
        return df

    target = DEFAULT_TIMEZONE if is_extraday else validate_iana(timezone)
    df = ensure_window_start_utc(df)
    if target == DEFAULT_TIMEZONE:
        return df
    return df.with_columns(
        pl.col("window_start").dt.convert_time_zone(target).alias("window_start")
    )


def window_start_to_utc_naive(df: pl.DataFrame) -> pl.DataFrame:
    """``window_start`` → naive UTC (joins analytics / export interne)."""
    if df.is_empty() or "window_start" not in df.columns:
        return df
    df = ensure_window_start_utc(df)
    return df.with_columns(
        pl.col("window_start").dt.replace_time_zone(None).alias("window_start")
    )
