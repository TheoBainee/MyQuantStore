"""Fetcher daily multi-type via Yahoo Finance (résolution ``1day``).

Historise les chandeliers journaliers pour stocks, forex, indices et futures
continus (``=F``).

**Stocks** : le chart Yahoo livre des OHLC déjà split-adjusted → désajustement
à l'ingest (``reverse_split_adjustment``) + peuplement ``yahoo_actions``.

**Forex / indices / futures** : dump OHLC chart tel quel (pas de corporate
actions equity). Futures = série continue Yahoo par root (``ES=F``), distincte
du track 1min multi-contrats Massive.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl

from myquantstore.api.client import MassiveClient
from myquantstore.api.yahoo import (
    YahooError,
    compute_dividend_adjustment_factors,
    compute_split_adjustment_factors,
    fetch_chart_bundle,
)
from myquantstore.config import Settings, generate_run_ts
from myquantstore.instruments import RESOLUTION_1DAY, Instrument, InstrumentType
from myquantstore.logging_setup import get_logger
from myquantstore.pipeline.aggregator import aggregate
from myquantstore.pipeline.fetchers.base import InstrumentFetcher
from myquantstore.query.adjust import reverse_split_adjustment
from myquantstore.storage.coverage import attach_coverage_fields, get_aggregate_date_range
from myquantstore.storage.raw_dumps import has_run_today, raw_dumps_exist, save_raw_dump
from myquantstore.tickers.yahoo_map import (
    YAHOO_DAILY_TYPES,
    UnmappableTickerError,
    to_yahoo_ticker,
)
from myquantstore.yahoo_actions.cache import YahooActionsCache

logger = get_logger("fetch.yahoo_daily")


class YahooDailyFetcher(InstrumentFetcher):
    """Fetcher daily Yahoo multi-type — ignore le client Massive."""

    def fetch(
        self,
        instrument: Instrument,
        settings: Settings,
        client: MassiveClient,
        force: bool = False,
        dry_run: bool = False,
    ) -> dict[str, object]:
        del client  # Yahoo only
        symbol = instrument.symbol
        resolution = RESOLUTION_1DAY
        result: dict[str, object] = {
            "status": "ok",
            "instrument": str(instrument),
            "symbol": symbol,
            "resolution": resolution,
            "source": "yahoo",
            "candles": 0,
        }

        if instrument.type not in YAHOO_DAILY_TYPES:
            result["status"] = "skipped"
            result["error"] = f"Yahoo daily non supporté pour {instrument.type.value}"
            return result

        try:
            y_ticker = to_yahoo_ticker(instrument, settings.yahoo_ticker_overrides)
        except UnmappableTickerError as exc:
            logger.warning(f"Skip Yahoo daily {instrument.key}: {exc}")
            result["status"] = "skipped"
            result["error"] = str(exc)
            return result

        result["yahoo_ticker"] = y_ticker
        logger.info(f"=== yahoo daily {instrument.key} ({y_ticker}) ===")

        if not force and not dry_run:
            already, run_ts = has_run_today(instrument, settings, resolution=resolution)
            if already:
                logger.warning(
                    f"Daily Yahoo déjà fait aujourd'hui pour {instrument.key} "
                    f"(run_ts={run_ts}) — skip"
                )
                result["status"] = "skipped"
                result["existing_run_ts"] = run_ts
                attach_coverage_fields(result, instrument, settings, resolution)
                return result

        today = datetime.now(UTC).date()
        has_existing = raw_dumps_exist(instrument, settings, resolution=resolution)
        oldest_date, latest_date = (
            get_aggregate_date_range(instrument, settings, resolution)
            if has_existing
            else (None, None)
        )

        # Horizon = max Yahoo au premier run ; incrémental ensuite
        if oldest_date is None:
            cover_start: date | None = None  # period=max
            cover_end = today
            use_max = True
        else:
            buffer = settings.yahoo_overlap_buffer_days
            cover_start = (
                latest_date - timedelta(days=buffer) if latest_date else today - timedelta(days=buffer)
            )
            cover_end = today
            use_max = False

        if not use_max and cover_start is not None and cover_start > cover_end:
            logger.warning(f"Rien à fetcher daily pour {instrument.key}")
            result["status"] = "no_range"
            attach_coverage_fields(result, instrument, settings, resolution, today=today)
            return result

        logger.info(
            f"{instrument.key} daily: "
            + ("period=max" if use_max else f"range=[{cover_start}, {cover_end}]")
            + f", existant={'oui' if has_existing else 'non'}"
        )

        if dry_run:
            result["status"] = "dry_run"
            result["yahoo_ticker"] = y_ticker
            attach_coverage_fields(result, instrument, settings, resolution, today=today)
            return result

        run_ts = generate_run_ts()
        try:
            if use_max:
                df, splits_raw, divs_raw = fetch_chart_bundle(
                    y_ticker,
                    settings,
                    period="max",
                    internal_symbol=symbol,
                )
            else:
                assert cover_start is not None
                # Events du range partiel ignorés (ne doivent pas écraser yahoo_actions).
                df, _splits_partial, _divs_partial = fetch_chart_bundle(
                    y_ticker,
                    settings,
                    date_from=cover_start,
                    date_to=cover_end,
                    internal_symbol=symbol,
                )
                splits_raw, divs_raw = None, None
                del _splits_partial, _divs_partial
        except YahooError as exc:
            logger.error(f"Yahoo daily KO {instrument.key}: {exc}")
            result["status"] = "error"
            result["error"] = str(exc)
            attach_coverage_fields(result, instrument, settings, resolution, today=today)
            return result

        apply_ca = instrument.type == InstrumentType.STOCKS
        splits_df = pl.DataFrame(
            schema={
                "execution_date": pl.Date,
                "split_ratio": pl.Float64,
                "historical_adjustment_factor": pl.Float64,
            }
        )

        if apply_ca:
            # yahoo_actions : historique complet uniquement (jamais le range incrémental).
            try:
                splits_df = self._refresh_yahoo_actions(
                    symbol=symbol,
                    y_ticker=y_ticker,
                    settings=settings,
                    use_max=use_max,
                    ohlcv_adj=df,
                    splits_raw=splits_raw,
                    divs_raw=divs_raw,
                )
            except Exception as exc:
                logger.warning(f"yahoo_actions refresh KO {symbol}: {exc}")
                try:
                    splits_df = YahooActionsCache(symbol, "splits", settings).get(
                        yahoo_ticker=y_ticker
                    )
                except Exception:
                    pass

        if df.is_empty():
            result["status"] = "no_candles"
            result["run_ts"] = run_ts
            attach_coverage_fields(result, instrument, settings, resolution, today=today)
            return result

        if apply_ca:
            # Chart Yahoo = OHLC déjà split-adjusted → convertir en bruts avant dump.
            n_before = df.height
            df = reverse_split_adjustment(df, splits_df)
            logger.info(
                f"{symbol} daily: OHLC désajustés splits "
                f"({splits_df.height} split(s), {n_before} barres) → stockage brut"
            )

        df = df.with_columns(
            [
                pl.lit(symbol).alias("symbol"),
                pl.lit(instrument.type.value).alias("instrument_type"),
                pl.lit(symbol).alias("product_code"),
                pl.lit(run_ts).alias("run_id"),
            ]
        )

        save_raw_dump(
            df,
            instrument,
            symbol,
            run_ts,
            settings,
            source_url=f"yahoo-chart://{y_ticker}",
            page_count=1,
            resolution=resolution,
            source="yahoo",
        )

        result["candles"] = df.height
        result["run_ts"] = run_ts

        if df.height > 0 or has_existing:
            aggregate(instrument, settings, resolution=resolution)

        attach_coverage_fields(result, instrument, settings, resolution, today=today)
        logger.info(f"{instrument.key} daily: terminé ({df.height} barres)")
        return result

    @staticmethod
    def _refresh_yahoo_actions(
        *,
        symbol: str,
        y_ticker: str,
        settings: Settings,
        use_max: bool,
        ohlcv_adj: pl.DataFrame,
        splits_raw: pl.DataFrame | None,
        divs_raw: pl.DataFrame | None,
    ) -> pl.DataFrame:
        """Peuple ``yahoo_actions`` et retourne les splits (facteurs inclus).

        - Premier run (``period=max``) : réutilise les events du chart (0 extra call).
        - Incrémental : refresh dédié ``period=max`` si cache stale/absent, sinon lit
          le cache (ne jamais écrire les events d'une fenêtre partielle).
        """
        splits_cache = YahooActionsCache(symbol, "splits", settings)
        divs_cache = YahooActionsCache(symbol, "dividends", settings)

        if use_max and splits_raw is not None and divs_raw is not None:
            splits_df = compute_split_adjustment_factors(splits_raw)
            # Montants + closes encore dans l'espace split-adjusted Yahoo.
            divs_df = compute_dividend_adjustment_factors(divs_raw, ohlcv_adj)
            splits_cache._write(splits_df, y_ticker)
            divs_cache._write(divs_df, y_ticker)
            return splits_df

        # Incrémental : historique actions complet si besoin (TTL).
        if not splits_cache._is_fresh():
            splits_df = splits_cache.refresh(y_ticker, ohlcv=None)
        else:
            splits_df = splits_cache.get(yahoo_ticker=y_ticker)

        if not divs_cache._is_fresh():
            # ohlcv partiel insuffisant pour les old ex-dates → refresh fetch max.
            divs_cache.refresh(y_ticker, ohlcv=None)
        else:
            divs_cache.get(yahoo_ticker=y_ticker, ohlcv=None)

        return splits_df


# Alias rétrocompat tests / imports externes.
YahooStocksDailyFetcher = YahooDailyFetcher
