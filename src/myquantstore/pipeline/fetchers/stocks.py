"""Fetcher stocks — endpoint v2 ``/v2/aggs/ticker/{t}/range/...`` + corporate actions.

Logique d'historisation des stocks :

1. Vérifier "déjà fait aujourd'hui" (skip si un run daté d'aujourd'hui existe).
2. Rafraîchir les caches corporate actions (**splits** + **dividends**) —
   nécessaires pour l'ajustement (toggle ``--no-split`` / ``--adjust``).
3. Déterminer la plage à fetcher (premier run vs incrémental).
4. Fetch via ``/v2/aggs/ticker/{api_ticker}/range/...`` avec ``adjusted=false``
   (prix bruts — l'ajustement split se fait à la query).
5. Sauvegarder le dump pseudo-brut (ticker = symbole, données normalisées).
6. Agréger les dumps pseudo-bruts.

Pas de RolloverChain (symbole unique, pas d'expiration) — la chaîne est une
:class:`myquantstore.chains.SingleSymbolChain` construite à la query.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from myquantstore.api.aggs_v2 import fetch_aggs_v2
from myquantstore.api.client import MassiveClient
from myquantstore.config import Settings, generate_run_ts
from myquantstore.corporate_actions.cache import CorporateActionsCache
from myquantstore.instruments import RESOLUTION_1MIN, Instrument
from myquantstore.logging_setup import get_logger
from myquantstore.pipeline.aggregator import aggregate
from myquantstore.pipeline.fetchers.base import InstrumentFetcher
from myquantstore.storage.coverage import attach_coverage_fields, get_aggregate_date_range
from myquantstore.storage.raw_dumps import has_run_today, raw_dumps_exist, save_raw_dump

logger = get_logger("fetch.stocks")


class StocksFetcher(InstrumentFetcher):
    """Fetcher pour les stocks (symbole unique + ajustement split à la query)."""

    def fetch(
        self,
        instrument: Instrument,
        settings: Settings,
        client: MassiveClient,
        force: bool = False,
        dry_run: bool = False,
    ) -> dict[str, object]:
        """Historise un stock via l'endpoint v2 (prix bruts adjusted=false)."""
        symbol = instrument.symbol
        logger.info(f"=== stocks:{symbol} ===")
        result: dict[str, object] = {
            "status": "ok",
            "instrument": str(instrument),
            "symbol": symbol,
            "candles": 0,
        }

        resolution = RESOLUTION_1MIN

        # 1. Vérifier "déjà fait aujourd'hui"
        if not force and not dry_run:
            already_done, existing_run_ts = has_run_today(
                instrument, settings, resolution=resolution
            )
            if already_done:
                logger.warning(
                    f"Historisation déjà effectuée aujourd'hui pour {symbol} "
                    f"(run_ts={existing_run_ts}) — skip. Utilisez --force pour relancer."
                )
                result["status"] = "skipped"
                result["existing_run_ts"] = existing_run_ts
                attach_coverage_fields(result, instrument, settings, resolution)
                return result

        # 2. Rafraîchir les caches corporate actions (splits + dividends pour --adjust)
        splits_cache = CorporateActionsCache(symbol, "splits", settings)
        if not dry_run:
            splits_cache.get(client, force_refresh=force)

        dividends_cache = CorporateActionsCache(symbol, "dividends", settings)
        if not dry_run:
            dividends_cache.get(client, force_refresh=force)

        # 3. Déterminer la plage à fetcher
        today = datetime.now(UTC).date()
        target_start = today - timedelta(days=settings.history_months_for(instrument.type) * 30)

        has_existing = raw_dumps_exist(instrument, settings, resolution=resolution)
        oldest_date, latest_date = (
            get_aggregate_date_range(instrument, settings, resolution)
            if has_existing
            else (None, None)
        )

        # Premier run : [target_start, today] ; incrémental : [latest - buffer, today]
        if oldest_date is None:
            cover_start = target_start
        else:
            cover_start = (
                latest_date - timedelta(days=settings.overlap_buffer_days)
                if latest_date
                else target_start
            )
        cover_end = today

        if cover_start > cover_end:
            logger.warning(f"Rien à fetcher pour {symbol} (cover_start > cover_end)")
            result["status"] = "no_range"
            attach_coverage_fields(result, instrument, settings, resolution, today=today)
            return result

        logger.info(
            f"{symbol}: range=[{cover_start}, {cover_end}], historique existant: "
            f"{'oui' if has_existing else 'non'}"
            + (f" (oldest={oldest_date}, latest={latest_date})" if has_existing else "")
        )

        if dry_run:
            logger.info(f"[dry-run] Plan de fetch pour {symbol}: range=[{cover_start}, {cover_end}]")
            result["status"] = "dry_run"
            result["segments"] = [{"ticker": symbol}]
            attach_coverage_fields(result, instrument, settings, resolution, today=today)
            return result

        # 4. Fetch via l'endpoint v2 (prix bruts)
        run_ts = generate_run_ts()
        df = fetch_aggs_v2(
            client,
            instrument,
            settings,
            date_from=cover_start.isoformat(),
            date_to=cover_end.isoformat(),
        )

        if df.is_empty():
            logger.warning(f"Aucun chandelier pour {symbol} sur [{cover_start}, {cover_end}]")
            result["status"] = "no_candles"
            result["run_ts"] = run_ts
            attach_coverage_fields(result, instrument, settings, resolution, today=today)
            return result

        # Stamp colonnes identité
        df = df.with_columns(pl.lit(symbol).alias("symbol"))
        df = df.with_columns(pl.lit(instrument.type.value).alias("instrument_type"))
        df = df.with_columns(pl.lit(symbol).alias("product_code"))  # compat agrégateur
        df = df.with_columns(pl.lit(run_ts).alias("run_id"))

        multiplier, timespan = _timeframe_parts("1min")
        source_url = (
            f"/v2/aggs/ticker/{instrument.api_ticker}/range/{multiplier}/{timespan}/"
            f"{cover_start.isoformat()}/{cover_end.isoformat()}?adjusted=false"
        )
        save_raw_dump(
            df,
            instrument,
            symbol,  # ticker = symbole (pas de sous-niveau contrat)
            run_ts,
            settings,
            source_url=source_url,
            page_count=client.page_count,
            resolution=resolution,
            source="massive",
        )

        total_candles = df.height
        result["candles"] = total_candles
        result["run_ts"] = run_ts
        logger.info(f"  {symbol}: {total_candles} chandeliers récupérés")

        # 5. Agréger
        if total_candles > 0 or has_existing:
            logger.info(f"Agrégation de {symbol}...")
            aggregate(instrument, settings, resolution=resolution)

        attach_coverage_fields(result, instrument, settings, resolution, today=today)
        logger.info(f"{symbol}: terminé ({total_candles} chandeliers)")
        return result


def _timeframe_parts(timeframe: str) -> tuple[int, str]:
    """Réutilise parse_timeframe pour le logging du source_url."""
    from myquantstore.instruments import parse_timeframe

    return parse_timeframe(timeframe)
