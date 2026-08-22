"""Orchestration de l'historisation (commande ``fetch``).

:func:`run_fetch` orchestre l'historisation des chandeliers OHLCV pour une
liste d'instruments et une ou plusieurs résolutions de stockage :

- ``1min`` (Massive) — tous types implémentés via :func:`get_fetcher`
- ``1day`` (Yahoo) — stocks / forex / indices / futures continu via
  :class:`YahooDailyFetcher`

Le retour est un dict de résultats par clé ``"{instrument_key}[{resolution}]"``.
"""

from __future__ import annotations

from typing import cast

from myquantstore.api.client import MassiveClient
from myquantstore.config import Settings
from myquantstore.instruments import (
    RESOLUTION_1DAY,
    RESOLUTION_1MIN,
    Instrument,
)
from myquantstore.logging_setup import get_logger
from myquantstore.pipeline.fetchers import get_fetcher
from myquantstore.pipeline.fetchers.yahoo_daily import YahooDailyFetcher
from myquantstore.tickers.yahoo_map import YAHOO_DAILY_TYPES

logger = get_logger("fetch")


def resolve_fetch_resolutions(
    settings: Settings,
    timeframe_arg: str | None,
) -> list[str]:
    """Interprète ``--timeframe`` CLI → liste de résolutions à fetcher.

    - ``None`` / ``all`` : ``[1min, 1day]``
    - ``1min`` / ``1day`` : singleton
    """
    del settings  # réservé pour futurs defaults par type
    if timeframe_arg is None or timeframe_arg.strip().lower() in ("all", ""):
        return [RESOLUTION_1MIN, RESOLUTION_1DAY]
    tf = timeframe_arg.strip().lower()
    if tf in (RESOLUTION_1MIN, "min", "1m"):
        return [RESOLUTION_1MIN]
    if tf in (RESOLUTION_1DAY, "day", "1d", "daily"):
        return [RESOLUTION_1DAY]
    raise ValueError(
        f"timeframe '{timeframe_arg}' non supporté pour fetch. "
        "Utilisez: 1min | 1day | all"
    )


def run_fetch(
    settings: Settings,
    client: MassiveClient,
    instruments: list[Instrument] | None = None,
    force: bool = False,
    dry_run: bool = False,
    resolutions: list[str] | None = None,
) -> dict[str, dict[str, object]]:
    """Lance l'historisation pour un ou plusieurs instruments × résolutions.

    :param settings: Configuration.
    :param client: Client Massive authentifié (ignoré pour Yahoo daily).
    :param instruments: Liste des instruments. Si None → tous configurés.
    :param force: Si True, relance même si déjà fait aujourd'hui.
    :param dry_run: Si True, calcule le plan sans appeler l'API ni écrire.
    :param resolutions: Résolutions à fetcher (défaut ``[1min, 1day]``, aligné
        CLI ``--timeframe all``). Cascade passe toujours une liste explicite.
    :return: Dictionnaire des résultats par clé ``instrument[resolution]``.
    """
    if instruments is None:
        instruments = settings.all_instruments()

    if resolutions is None:
        resolutions = [RESOLUTION_1MIN, RESOLUTION_1DAY]

    logger.info(
        f"Début historisation {len(instruments)} instrument(s) × "
        f"resolutions={resolutions}: {[str(i) for i in instruments]}"
    )

    results: dict[str, dict[str, object]] = {}

    for instrument in instruments:
        for resolution in resolutions:
            key = f"{instrument.key}[{resolution}]"
            try:
                result = _fetch_one(
                    instrument, settings, client, resolution, force=force, dry_run=dry_run
                )
            except NotImplementedError as e:
                logger.warning(f"{key} non implémenté: {e}")
                result = {
                    "status": "not_implemented",
                    "instrument": str(instrument),
                    "resolution": resolution,
                    "error": str(e),
                }
            except Exception as e:
                logger.error(f"Erreur fetch {key}: {e}")
                result = {
                    "status": "error",
                    "instrument": str(instrument),
                    "resolution": resolution,
                    "error": str(e),
                }
            results[key] = result

    total_candles = sum(cast("int", r.get("candles", 0)) for r in results.values())
    total_skipped = sum(1 for r in results.values() if r.get("status") == "skipped")
    logger.info(
        f"Historisation terminée: {total_candles} chandeliers, "
        f"{total_skipped} skip(s), {len(results)} job(s)"
    )
    return results


def _fetch_one(
    instrument: Instrument,
    settings: Settings,
    client: MassiveClient,
    resolution: str,
    *,
    force: bool,
    dry_run: bool,
) -> dict[str, object]:
    if resolution == RESOLUTION_1DAY:
        if instrument.type not in YAHOO_DAILY_TYPES:
            return {
                "status": "skipped",
                "instrument": str(instrument),
                "resolution": resolution,
                "error": f"1day Yahoo non supporté pour {instrument.type.value}",
            }
        return YahooDailyFetcher().fetch(
            instrument, settings, client, force=force, dry_run=dry_run
        )

    if resolution != RESOLUTION_1MIN:
        raise ValueError(f"Résolution de fetch inconnue: {resolution}")

    fetcher = get_fetcher(instrument)
    return fetcher.fetch(instrument, settings, client, force=force, dry_run=dry_run)
