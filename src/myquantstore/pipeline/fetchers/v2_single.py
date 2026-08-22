"""Fetcher générique pour instruments v2 mono-symbole (forex, indices).

Miroir de :class:`StocksFetcher` **sans** corporate actions (pas de splits /
dividends). Utilise l'endpoint partagé ``/v2/aggs/ticker/{api_ticker}/range/...``
via :func:`myquantstore.api.aggs_v2.fetch_aggs_v2`.

Types couverts :
- **forex** — ticker préfixé ``C:`` (ex: ``C:EURUSD``), volume présent.
- **indices** — ticker préfixé ``I:`` (ex: ``I:NDX``), volume souvent absent.

Logique d'historisation (identique stocks sans splits) :

1. Vérifier "déjà fait aujourd'hui" (skip si un run daté d'aujourd'hui existe).
2. Déterminer la plage à fetcher (premier run vs incrémental).
3. Fetch via ``/v2/aggs/ticker/{api_ticker}/range/...`` (pas de ``adjusted``).
4. Sauvegarder le dump pseudo-brut (ticker = symbole nu, données normalisées).
5. Agréger les dumps pseudo-bruts.

Pas de RolloverChain — la chaîne est une
:class:`myquantstore.chains.SingleSymbolChain` construite à la query.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from myquantstore.api.aggs_v2 import fetch_aggs_v2
from myquantstore.api.client import MassiveClient
from myquantstore.config import Settings, generate_run_ts
from myquantstore.instruments import RESOLUTION_1MIN, Instrument, parse_timeframe
from myquantstore.logging_setup import get_logger
from myquantstore.pipeline.aggregator import aggregate
from myquantstore.pipeline.fetchers.base import InstrumentFetcher
from myquantstore.storage.coverage import attach_coverage_fields, get_aggregate_date_range
from myquantstore.storage.raw_dumps import has_run_today, raw_dumps_exist, save_raw_dump

logger = get_logger("fetch.v2_single")


class V2SingleSymbolFetcher(InstrumentFetcher):
    """Fetcher v2 pour forex / indices (symbole unique, pas de corporate actions)."""

    def fetch(
        self,
        instrument: Instrument,
        settings: Settings,
        client: MassiveClient,
        force: bool = False,
        dry_run: bool = False,
    ) -> dict[str, object]:
        """Historise un instrument forex ou indices via l'endpoint v2."""
        symbol = instrument.symbol
        type_label = instrument.type.value
        logger.info(f"=== {type_label}:{symbol} ===")
        result: dict[str, object] = {
            "status": "ok",
            "instrument": str(instrument),
            "symbol": symbol,
            "candles": 0,
        }

        resolution = RESOLUTION_1MIN

        # 1. Vérifier "déjà fait aujourd'hui"
        if not force and not dry_run:
            already_done, existing_run_ts = has_run_today(instrument, settings)
            if already_done:
                logger.warning(
                    f"Historisation déjà effectuée aujourd'hui pour {instrument.key} "
                    f"(run_ts={existing_run_ts}) — skip. Utilisez --force pour relancer."
                )
                result["status"] = "skipped"
                result["existing_run_ts"] = existing_run_ts
                attach_coverage_fields(result, instrument, settings, resolution)
                return result

        # 2. Déterminer la plage à fetcher
        today = datetime.now(UTC).date()
        target_start = today - timedelta(days=settings.history_months_for(instrument.type) * 30)

        has_existing = raw_dumps_exist(instrument, settings)
        oldest_date, latest_date = (
            get_aggregate_date_range(instrument, settings, resolution)
            if has_existing
            else (None, None)
        )

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
            logger.warning(
                f"Rien à fetcher pour {instrument.key} (cover_start > cover_end)"
            )
            result["status"] = "no_range"
            attach_coverage_fields(result, instrument, settings, resolution, today=today)
            return result

        logger.info(
            f"{instrument.key}: range=[{cover_start}, {cover_end}], historique existant: "
            f"{'oui' if has_existing else 'non'}"
            + (f" (oldest={oldest_date}, latest={latest_date})" if has_existing else "")
        )

        if dry_run:
            logger.info(
                f"[dry-run] Plan de fetch pour {instrument.key}: "
                f"range=[{cover_start}, {cover_end}]"
            )
            result["status"] = "dry_run"
            result["segments"] = [{"ticker": symbol}]
            attach_coverage_fields(result, instrument, settings, resolution, today=today)
            return result

        # 3. Fetch via l'endpoint v2
        run_ts = generate_run_ts()
        df = fetch_aggs_v2(
            client,
            instrument,
            settings,
            date_from=cover_start.isoformat(),
            date_to=cover_end.isoformat(),
        )

        if df.is_empty():
            logger.warning(
                f"Aucun chandelier pour {instrument.key} sur [{cover_start}, {cover_end}]"
            )
            result["status"] = "no_candles"
            result["run_ts"] = run_ts
            attach_coverage_fields(result, instrument, settings, resolution, today=today)
            return result

        # Stamp colonnes identité
        df = df.with_columns(pl.lit(symbol).alias("symbol"))
        df = df.with_columns(pl.lit(type_label).alias("instrument_type"))
        df = df.with_columns(pl.lit(symbol).alias("product_code"))  # compat agrégateur
        df = df.with_columns(pl.lit(run_ts).alias("run_id"))

        multiplier, timespan = parse_timeframe(settings.timeframe)
        source_url = (
            f"/v2/aggs/ticker/{instrument.api_ticker}/range/{multiplier}/{timespan}/"
            f"{cover_start.isoformat()}/{cover_end.isoformat()}"
        )
        save_raw_dump(
            df,
            instrument,
            symbol,  # ticker = symbole nu (pas de sous-niveau contrat)
            run_ts,
            settings,
            source_url=source_url,
            page_count=client.page_count,
        )

        total_candles = df.height
        result["candles"] = total_candles
        result["run_ts"] = run_ts
        logger.info(f"  {instrument.key}: {total_candles} chandeliers récupérés")

        # 4. Agréger
        if total_candles > 0 or has_existing:
            logger.info(f"Agrégation de {instrument.key}...")
            aggregate(instrument, settings)

        attach_coverage_fields(result, instrument, settings, resolution, today=today)
        logger.info(f"{instrument.key}: terminé ({total_candles} chandeliers)")
        return result
