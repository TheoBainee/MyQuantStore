"""Chaînes d'instruments — abstraction multi-type sur la RolloverChain.

Le système multi-type distingue deux familles d'instruments :

- **Instruments à contrats expirants** (futures, options) : l'historique continu
  est construit en enchaînant les contrats successifs selon une règle de
  rollover. Implémenté par :class:`myquantstore.contracts.rollover.RolloverChain`
  (futures). Options = scaffold (``NotImplementedError``).

- **Instruments à symbole unique** (forex, stocks, indices) : pas d'expiration,
  pas de rollover — un seul segment couvrant toute la période. Implémenté par
  :class:`SingleSymbolChain`.

Le :class:`InstrumentChain` est un **protocole** commun qui permet à ``query``
et ``chart`` de traiter tous les types de manière uniforme : récupérer le
contrat/symbole actif, la tick size d'un ticker, les segments sur une période.

Rendre ``chain`` optionnel dans ``query()`` permet aussi de se passer totalement
de chaîne quand aucune normalisation tick_size ni ajustement n'est demandé.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Protocol, runtime_checkable

import polars as pl

from myquantstore.instruments import Instrument


@runtime_checkable
class InstrumentChain(Protocol):
    """Protocole commun pour les chaînes d'instruments (futures + single-symbol).

    Implémenté par :class:`myquantstore.contracts.rollover.RolloverChain` (futures)
    et :class:`SingleSymbolChain` (forex/stocks/indices).

    NB : les types de retour utilisent ``Any`` pour les segments (la variance
    des ``list`` rendrait ``list[RolloverSegment]`` incompatible avec
    ``list[object]``). La structure concrète est documentée par chaque impl.
    """

    def active_contract(self, d: date) -> str | None:
        """Retourne le ticker du contrat/symbole actif à la date ``d``."""
        ...

    def segment_for_ticker(self, ticker: str) -> Any:
        """Retourne le segment associé à un ticker (ou None)."""
        ...

    def continuous_segments(self, start: date, end: date) -> list[Any]:
        """Retourne les segments couvrant la période [start, end]."""
        ...

    def tick_size_for_ticker(self, ticker: str) -> float:
        """Retourne la taille du tick pour un ticker (0.0 si non applicable)."""
        ...

    def to_table(self) -> pl.DataFrame:
        """Retourne un DataFrame plat des segments (pour ``status``)."""
        ...

    def __len__(self) -> int:
        """Nombre de segments dans la chaîne."""
        ...


class SingleSymbolChain:
    """Chaîne à symbole unique pour les instruments sans expiration (forex/stocks/indices).

    Modélise un instrument à symbole unique comme un unique segment couvrant
    toute la période d'historique. ``tick_size_for_ticker`` retourne 0.0 (pas
    de normalisation tick_size pour ces types — la notion de tick size contractuelle
    n'existe pas pour forex/stocks/indices).

    Utilisé par ``query``/``chart`` pour rester polymorphe avec la
    :class:`RolloverChain` futures.
    """

    def __init__(self, instrument: Instrument):
        self.instrument = instrument
        # Un seul segment : du 1er janvier 1900 à +100 ans (couvre tout l'historique).
        self._far_past = date(1900, 1, 1)
        self._far_future = _far_future_date()

    def active_contract(self, d: date) -> str | None:
        """Le symbole est toujours le contrat actif (pas de rollover)."""
        return self.instrument.symbol

    def segment_for_ticker(self, ticker: str) -> dict[str, object] | None:
        """Retourne un dict-segment si le ticker correspond au symbole."""
        if ticker == self.instrument.symbol:
            return {
                "ticker": self.instrument.symbol,
                "active_from": self._far_past,
                "active_until": self._far_future,
            }
        return None

    def continuous_segments(self, start: date, end: date) -> list[dict[str, object]]:
        """Un seul segment couvrant toute la période."""
        return [
            {
                "ticker": self.instrument.symbol,
                "active_from": self._far_past,
                "active_until": self._far_future,
            }
        ]

    def tick_size_for_ticker(self, ticker: str) -> float:
        """Pas de tick size contractuelle pour les symboles uniques → 0.0.

        Conséquence : ``--normalize-tick-size`` est un no-op pour ces types
        (la normalisation divise par tick ; tick=0 est skippé par le reader).
        """
        return 0.0

    def to_table(self) -> pl.DataFrame:
        """Table à une ligne représentant le symbole unique (pour ``status``)."""
        return pl.DataFrame(
            {
                "ticker": [self.instrument.symbol],
                "active_from": [self._far_past],
                "active_until": [self._far_future],
                "trade_tick_size": [0.0],
            }
        )

    def __len__(self) -> int:
        return 1

    def __repr__(self) -> str:
        return f"SingleSymbolChain({self.instrument.key})"


class OptionsChain:
    """Chaîne pour les options — scaffold (``NotImplementedError``).

    Les options ont une logique de rollover complexe (chaînes par strike,
    call/put, expiration). Implémentation planifiée — toute méthode lève
    :class:`NotImplementedError`.
    """

    def __init__(self, instrument: Instrument):
        if instrument.type.value != "options":
            raise ValueError("OptionsChain est réservée au type 'options'")
        self.instrument = instrument

    def _not_implemented(self) -> NotImplementedError:
        return NotImplementedError(
            "Gestion des options non implémentée (scaffold). "
            "Les options requièrent une logique de chaîne par strike/call/put "
            "non encore développée."
        )

    def active_contract(self, d: date) -> str | None:
        raise self._not_implemented()

    def segment_for_ticker(self, ticker: str) -> object | None:
        raise self._not_implemented()

    def continuous_segments(self, start: date, end: date) -> list[object]:
        raise self._not_implemented()

    def tick_size_for_ticker(self, ticker: str) -> float:
        raise self._not_implemented()

    def to_table(self) -> pl.DataFrame:
        raise self._not_implemented()

    def __len__(self) -> int:
        return 0


def build_chain(instrument: Instrument, contracts_df: pl.DataFrame | None = None, **kwargs: object) -> InstrumentChain:
    """Fabrique une chaîne adaptée au type d'instrument.

    :param instrument: Instrument cible.
    :param contracts_df: DataFrame des contrats (requis pour futures/options).
    :param kwargs: Arguments spécifiques (ex: ``days_before_expiry`` pour futures).
    :return: Une :class:`InstrumentChain` (RolloverChain, SingleSymbolChain ou OptionsChain).
    """
    if instrument.type.value == "futures":
        from myquantstore.contracts.rollover import RolloverChain

        if contracts_df is None:
            raise ValueError("contracts_df est requis pour construire une RolloverChain futures")
        days_before_raw = kwargs.get("days_before_expiry", 7)
        days_before = int(days_before_raw) if isinstance(days_before_raw, int | float) else 7
        return RolloverChain(instrument.symbol, contracts_df, days_before_expiry=days_before)
    elif instrument.type.value == "options":
        return OptionsChain(instrument)
    else:
        # forex, stocks, indices → symbole unique
        return SingleSymbolChain(instrument)


# Réexport pratique de la période "far future" pour d'autres modules
def _far_future_date() -> date:
    return date.today() + timedelta(days=365 * 100)
