"""Fetcher futures — RolloverChain + endpoint ``/futures/v1/aggs/{ticker}``.

Logique d'historisation spécifique aux futures :

1. Vérifier "déjà fait aujourd'hui" (skip si un run daté d'aujourd'hui existe).
2. Récupérer le cache contrats (:class:`ContractsCache`).
3. Construire la :class:`RolloverChain` à partir des contrats.
4. Pour chaque contrat actif sur la période cible :
   - Déterminer le range (premier run vs incrémental vs extension arrière).
   - Fetch via ``/futures/v1/aggs/{ticker}``.
   - Sauvegarder le dump pseudo-brut (1 fichier par contrat et par run, données normalisées).
5. Agréger les dumps pseudo-bruts en cache agrégé.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl

from myquantstore.api.aggs_futures import fetch_aggs_futures
from myquantstore.api.client import MassiveClient
from myquantstore.config import Settings, generate_run_ts
from myquantstore.contracts.cache import ContractsCache
from myquantstore.contracts.rollover import RolloverChain
from myquantstore.instruments import RESOLUTION_1MIN, Instrument
from myquantstore.logging_setup import get_logger
from myquantstore.pipeline.aggregator import aggregate
from myquantstore.pipeline.fetchers.base import InstrumentFetcher
from myquantstore.storage.coverage import attach_coverage_fields, get_aggregate_date_range
from myquantstore.storage.raw_dumps import has_run_today, raw_dumps_exist

logger = get_logger("fetch.futures")


class FuturesFetcher(InstrumentFetcher):
    """Fetcher pour les instruments futures (contrats expirants + rollover)."""

    def fetch(
        self,
        instrument: Instrument,
        settings: Settings,
        client: MassiveClient,
        force: bool = False,
        dry_run: bool = False,
    ) -> dict[str, object]:
        """Historise un produit futures via sa chaîne de contrats."""
        product_code = instrument.symbol
        logger.info(f"=== futures:{product_code} ===")
        result: dict[str, object] = {
            "status": "ok",
            "instrument": str(instrument),
            "product_code": product_code,
            "candles": 0,
        }

        resolution = RESOLUTION_1MIN

        # 1. Vérifier "déjà fait aujourd'hui"
        if not force and not dry_run:
            already_done, existing_run_ts = has_run_today(instrument, settings)
            if already_done:
                logger.warning(
                    f"Historisation déjà effectuée aujourd'hui pour {product_code} "
                    f"(run_ts={existing_run_ts}) — skip. Utilisez --force pour relancer."
                )
                result["status"] = "skipped"
                result["existing_run_ts"] = existing_run_ts
                attach_coverage_fields(result, instrument, settings, resolution)
                return result

        # 2. Récupérer le cache contrats
        cache = ContractsCache(product_code, settings)
        contracts_df = cache.get(client)
        if contracts_df.is_empty():
            logger.error(f"Aucun contrat disponible pour {product_code} — skip")
            result["status"] = "error"
            result["error"] = "no_contracts"
            attach_coverage_fields(result, instrument, settings, resolution)
            return result

        # 3. Construire la RolloverChain
        chain = RolloverChain(product_code, contracts_df, settings.days_before_expiry)
        if len(chain) == 0:
            logger.error(f"Chaîne de rollover vide pour {product_code} — skip")
            result["status"] = "error"
            result["error"] = "empty_rollover_chain"
            attach_coverage_fields(result, instrument, settings, resolution)
            return result

        result["contracts"] = len(chain)

        # 4. Déterminer le range global à couvrir
        today = datetime.now(UTC).date()
        target_start = today - timedelta(days=settings.history_months_for(instrument.type) * 30)

        # Déterminer la date la plus ancienne/récente déjà historisée
        has_existing = raw_dumps_exist(instrument, settings)
        oldest_date, latest_date = (
            get_aggregate_date_range(instrument, settings, resolution)
            if has_existing
            else (None, None)
        )

        # 5. Segments à fetcher
        segments = chain.continuous_segments(target_start, today)
        if not segments:
            logger.warning(
                f"Aucun segment actif sur [{target_start}, {today}] pour {product_code}"
            )
            result["status"] = "no_segments"
            attach_coverage_fields(result, instrument, settings, resolution, today=today)
            return result

        logger.info(
            f"{product_code}: {len(segments)} segment(s) à couvrir sur "
            f"[{target_start}, {today}], historique existant: "
            f"{'oui' if has_existing else 'non'}"
            + (f" (oldest={oldest_date}, latest={latest_date})" if has_existing else "")
        )

        if dry_run:
            logger.info(f"[dry-run] Plan de fetch pour {product_code}:")
            for seg in segments:
                seg_start, seg_end = _determine_segment_range(
                    seg, target_start, today, oldest_date, latest_date, settings
                )
                logger.info(f"  {seg.ticker}: range=[{seg_start}, {seg_end}]")
            result["status"] = "dry_run"
            result["segments"] = [{"ticker": s.ticker} for s in segments]
            attach_coverage_fields(result, instrument, settings, resolution, today=today)
            return result

        # 6. Fetcher chaque segment
        run_ts = generate_run_ts()
        total_candles = 0

        for seg in segments:
            seg_start, seg_end = _determine_segment_range(
                seg, target_start, today, oldest_date, latest_date, settings
            )

            if seg_start is None or seg_end is None:
                logger.debug(f"  Skip {seg.ticker}: pas de range à fetcher")
                continue

            logger.info(f"  Fetch {seg.ticker}: range=[{seg_start}, {seg_end}]")

            df = fetch_aggs_futures(
                client,
                seg.ticker,
                settings,
                window_start_gte=seg_start,
                window_start_lte=seg_end,
            )

            if df.is_empty():
                logger.warning(f"  Aucun chandelier pour {seg.ticker} sur [{seg_start}, {seg_end}]")
                continue

            # Stamp colonnes identité (product_code pour compat, symbol + instrument_type)
            df = df.with_columns(pl.lit(product_code).alias("product_code"))
            df = df.with_columns(pl.lit(product_code).alias("symbol"))
            df = df.with_columns(pl.lit(instrument.type.value).alias("instrument_type"))
            df = df.with_columns(pl.lit(run_ts).alias("run_id"))

            source_url = f"/futures/v1/aggs/{seg.ticker}?resolution={settings.timeframe}"
            from myquantstore.storage.raw_dumps import save_raw_dump

            save_raw_dump(
                df,
                instrument,
                seg.ticker,
                run_ts,
                settings,
                source_url=source_url,
                page_count=client.page_count,
            )

            total_candles += df.height
            logger.info(f"  {seg.ticker}: {df.height} chandeliers récupérés")

        result["candles"] = total_candles
        result["run_ts"] = run_ts

        # 7. Agréger
        if total_candles > 0 or has_existing:
            logger.info(f"Agrégation de {product_code}...")
            aggregate(instrument, settings)

        attach_coverage_fields(result, instrument, settings, resolution, today=today)
        logger.info(f"{product_code}: terminé ({total_candles} chandeliers)")
        return result


def _determine_segment_range(
    seg: object,
    target_start: date,
    today: date,
    oldest_date: date | None,
    latest_date: date | None,
    settings: Settings,
) -> tuple[str | None, str | None]:
    """Détermine le range (gte, lte) à fetcher pour un segment de rollover.

    Trois cas :
    1. **Premier run** (pas d'historique) : range = [max(target_start, active_from), min(today, active_until)].
    2. **Run incrémental** (historique existant) : range = [latest_date - buffer, today] ∩ [active_from, active_until].
    3. **Extension** (history_months augmenté) : si target_start < oldest_date, backfill arrière.

    :return: Tuple (window_start_gte, window_start_lte) au format YYYY-MM-DD, ou (None, None).
    """
    seg_active_start = seg.active_from  # type: ignore[attr-defined]
    seg_active_end = seg.active_until  # type: ignore[attr-defined]

    if oldest_date is None:
        cover_start = target_start
        cover_end = today
    else:
        cover_start = (
            min(target_start, latest_date - timedelta(days=settings.overlap_buffer_days))
            if latest_date
            else target_start
        )
        cover_end = today

    range_start = max(cover_start, seg_active_start)
    range_end = min(cover_end, seg_active_end)

    if range_start > range_end:
        return None, None

    return range_start.isoformat(), range_end.isoformat()
