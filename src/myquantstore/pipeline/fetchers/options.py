"""Fetcher options — scaffold (``NotImplementedError``).

Les options ont une logique de fetch complexe (chaînes par strike, call/put,
expiration). Implémentation planifiée — toute opération lève
:class:`NotImplementedError`.

Le scaffold existe pour valider l'architecture multi-type (factory
:func:`myquantstore.pipeline.fetchers.get_fetcher` dispatche bien vers ce fetcher
pour le type ``options``) sans implémenter la logique.
"""

from __future__ import annotations

from datetime import date

from myquantstore.api.client import MassiveClient
from myquantstore.config import Settings
from myquantstore.instruments import Instrument
from myquantstore.logging_setup import get_logger
from myquantstore.pipeline.fetchers.base import InstrumentFetcher

logger = get_logger("fetch.options")


class OptionsFetcher(InstrumentFetcher):
    """Fetcher pour les options — scaffold (``NotImplementedError``)."""

    def fetch(
        self,
        instrument: Instrument,
        settings: Settings,
        client: MassiveClient,
        force: bool = False,
        dry_run: bool = False,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, object]:
        raise NotImplementedError(
            f"Fetch des options non implémenté (scaffold). Instrument: {instrument.key}. "
            "Les options requièrent une logique de chaîne par strike/call/put "
            "non encore développée."
        )
