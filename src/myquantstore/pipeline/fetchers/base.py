"""Classe de base abstraite des fetchers d'instruments.

Un :class:`InstrumentFetcher` encapsule la logique de récupération et
d'historisation des chandeliers OHLCV pour un type d'instrument donné :

- Déterminer la plage à fetcher (premier run vs incrémental vs extension).
- Appeler l'endpoint API adapté (futures ``/v1`` ou v2 ``/v2``).
- Sauvegarder les dumps pseudo-bruts (1 fichier Parquet par run, données normalisées au format interne).
- Déclencher l'agrégation après le fetch.

Le retour est un dict de résultat ``{status, candles, ...}`` homogène entre types,
exploitable par la commande ``myquantstore fetch``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from myquantstore.api.client import MassiveClient
from myquantstore.config import Settings
from myquantstore.instruments import Instrument


class InstrumentFetcher(ABC):
    """Interface abstraite des fetchers multi-type."""

    @abstractmethod
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
        """Historise un instrument.

        :param instrument: Instrument à historiser.
        :param settings: Configuration.
        :param client: Client Massive authentifié.
        :param force: Si True, relance même si déjà fait aujourd'hui.
        :param dry_run: Si True, calcule le plan sans appeler l'API ni écrire.
        :param start_date: Plage explicite (``--start-date``, inclusive). Remplace
            le calcul automatique (premier run / incrémental) et contourne le
            skip « dump du jour ». ``None`` = comportement automatique.
        :param end_date: Borne de fin explicite (``--end-date``, inclusive).
            Utilisée seulement avec ``start_date`` ; ``None`` = aujourd'hui.
        :return: Dict de résultat ``{status, candles, ...}`` (homogène entre types).
        """
        ...
