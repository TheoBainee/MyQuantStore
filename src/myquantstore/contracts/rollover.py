"""Gestion du rollover des contrats futures.

**Règle de rollover** : on passe au contrat suivant **N jours avant
l'expiration** (défaut 7 jours). Le ``rollover_date`` (= ``last_trade_date - N``)
est le **dernier jour** du contrat courant ; le contrat suivant devient actif
au **jour ouvré suivant** (lundi–vendredi, pas de calendrier de jours fériés).
Exemple : un contrat expirant le vendredi 18 a son ``rollover_date`` au
vendredi 11 (= 18 - 7) — le vendredi 11 appartient encore à ce contrat, le
contrat suivant est actif à partir du lundi 14.

**L'objet :class:`RolloverChain`** modélise la chaîne continue des contrats
d'un produit. À partir du DataFrame des contrats (issu du cache ``/contracts``)
et de ``days_before_expiry``, il calcule les segments actifs et expose des
méthodes pour déterminer le contrat actif à une date donnée, récupérer le
``trade_tick_size`` d'un contrat, etc.

La chaîne est affichable via :meth:`RolloverChain.to_table` (utilisé par
la commande ``myquantstore status``).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import polars as pl

from myquantstore.logging_setup import get_logger

logger = get_logger("rollover")


@dataclass
class RolloverSegment:
    """Un segment de la chaîne de rollover.

    Un segment représente une période pendant laquelle un contrat donné est
    le contrat actif (front-month). Il est délimité par ``active_from``
    (inclus) et ``active_until`` (exclus) — à ``active_until``, on bascule
    au contrat suivant.
    """

    ticker: str
    """Ticker du contrat (ex: "ESM5")."""

    first_trade_date: date
    """Premier jour de trading du contrat."""

    last_trade_date: date
    """Dernier jour de trading (expiration)."""

    settlement_date: date
    """Date de settlement du contrat."""

    rollover_date: date
    """Dernier jour actif du contrat = ``last_trade_date - days_before_expiry``.
    Le contrat suivant devient actif au jour ouvré suivant (``active_until``)."""

    active_from: date
    """Date (inclusive) à partir de laquelle ce contrat devient le front-month.
    = ``active_until`` du contrat précédent (jour ouvré suivant son ``rollover_date``),
    ou ``first_trade_date`` pour le premier."""

    active_until: date
    """Date (exclusive) où on bascule au contrat suivant.
    = jour ouvré suivant le ``rollover_date`` de ce contrat."""

    trade_tick_size: float
    """Taille du tick pour ce contrat (depuis /contracts). Utilisé pour la normalisation."""

    product_code: str
    """Code produit (ex: "ES")."""

    name: str = ""
    """Nom complet du contrat (ex: "E-mini S&P 500 Jun 2025")."""

    def contains(self, d: date) -> bool:
        """Vérifie si une date est dans la période active de ce segment.

        :param d: Date à tester.
        :return: True si ``active_from <= d < active_until``.
        """
        return self.active_from <= d < self.active_until


class RolloverChain:
    """Chaîne continue des contrats d'un produit.

    Construite à partir du DataFrame des contrats (issu du cache ``/contracts``)
    et de ``days_before_expiry``. Les contrats sont triés par ``last_trade_date``
    et enchaînés : le jour ouvré suivant le ``rollover_date`` d'un contrat
    devient l'``active_from`` du suivant.

    Usage typique :

    .. code-block:: python

        chain = RolloverChain("ES", contracts_df, days_before_expiry=7)
        active_ticker = chain.active_contract(date(2025, 6, 1))  # "ESM5"
        tick = chain.tick_size_for_ticker("ESM5")  # 0.25
        table = chain.to_table()  # pour affichage dans status
    """

    def __init__(
        self,
        product_code: str,
        contracts: pl.DataFrame,
        days_before_expiry: int = 7,
    ):
        self.product_code = product_code
        self.days_before_expiry = days_before_expiry
        self.contracts = contracts
        self.segments: list[RolloverSegment] = []
        self._build_segments()

    def _build_segments(self) -> None:
        """Construit la liste ordonnée des segments à partir des contrats.

        Étapes :
        1. Trier les contrats par ``first_trade_date`` ascendant.
        2. Filtrer les contrats de type ``single`` (ignorer les ``combo``).
        3. Pour chaque contrat : calculer ``rollover_date = last_trade_date - days_before_expiry``
           (dernier jour actif) et ``active_until`` = jour ouvré suivant.
        4. Enchaîner : ``active_from`` du segment N+1 = ``active_until`` du segment N.
        """
        if self.contracts.is_empty():
            logger.warning(f"Aucun contrat pour construire la chaîne de {self.product_code}")
            return

        df = self.contracts

        # Filtrer les contrats de type "single" (ignorer les "combo").
        # Le champ "type" peut être null pour les contrats avant 2025-03-12.
        if "type" in df.columns:
            df = df.filter(
                (pl.col("type") == "single") | pl.col("type").is_null()
            )

        # Exclure les combos / spreads : les tickers de combos contiennent un "-"
        # (ex: "ESH6-ESM6" est un spread, pas un contrat single).
        #
        # NOTE — Pourquoi on ne peut pas filtrer via l'API avec type=single :
        #   1. L'API tagge aussi les combos (spreads) avec type="single" — on a vérifié
        #      que /contracts?type=single&date=2025-06-20 renvoie les 24 combos d'ES.
        #   2. type=single exclut les contrats historiques antérieurs au 2025-03-12
        #      (type=null à cette époque) → on perdrait tout l'historique pré-2025.
        #   Seul critère fiable : le ticker. Les combos contiennent un "-"
        #   (ex: "ESH6-ESM6"). On filtre donc client-side.
        if "ticker" in df.columns:
            df = df.filter(~pl.col("ticker").str.contains("-"))

        # Trier par last_trade_date (date d'expiration) pour obtenir l'ordre
        # chronologique correct du front-month. On NE trie PAS par first_trade_date
        # car les contrats sont listés longtemps avant leur expiration (ex: ESZ4
        # est listé en 2021 mais expire en déc 2024).
        if "last_trade_date" in df.columns:
            df = df.sort("last_trade_date")

        segments: list[RolloverSegment] = []
        prev_active_until: date | None = None

        for row in df.iter_rows(named=True):
            # Extraction des champs (avec valeurs par défaut pour les champs optionnels)
            ticker = row.get("ticker", "")
            first_trade = row.get("first_trade_date")
            last_trade = row.get("last_trade_date")
            settlement = row.get("settlement_date", last_trade)
            tick_size = row.get("trade_tick_size", 0.0)
            name = row.get("name", "")

            # Skip si champs essentiels manquants
            if not ticker or first_trade is None or last_trade is None:
                continue

            # Calcul du rollover_date = last_trade_date - days_before_expiry
            # (dernier jour où ce contrat est le front-month)
            rollover_date = last_trade - timedelta(days=self.days_before_expiry)

            # active_from : active_until du contrat précédent, ou first_trade_date pour le premier
            active_from = prev_active_until if prev_active_until is not None else first_trade

            # active_until : jour ouvré suivant le rollover_date (exclus)
            active_until = _next_business_day(rollover_date)

            segment = RolloverSegment(
                ticker=ticker,
                first_trade_date=first_trade,
                last_trade_date=last_trade,
                settlement_date=settlement if settlement is not None else last_trade,
                rollover_date=rollover_date,
                active_from=active_from,
                active_until=active_until,
                trade_tick_size=float(tick_size) if tick_size is not None else 0.0,
                product_code=self.product_code,
                name=name,
            )
            segments.append(segment)

            # Le prochain contrat commence au jour ouvré suivant le rollover_date de celui-ci
            prev_active_until = active_until

        self.segments = segments
        logger.debug(
            f"Chaîne de rollover {self.product_code}: {len(segments)} segment(s) construits"
        )

    def active_contract(self, d: date) -> str | None:
        """Retourne le ticker du contrat actif à une date donnée.

        Le contrat actif = le segment dont ``active_from <= d < active_until``.
        Si aucun segment ne contient la date (ex: trou dans la chaîne),
        on retourne le dernier segment dont ``active_from <= d``.

        :param d: Date à tester.
        :return: Ticker du contrat actif, ou None si la chaîne est vide.
        """
        if not self.segments:
            return None

        # Recherche du segment contenant la date
        for seg in self.segments:
            if seg.contains(d):
                return seg.ticker

        # Si aucun segment ne contient la date, on prend le dernier dont active_from <= d
        # (utile si on est entre le rollover_date et le first_trade_date du suivant)
        for seg in reversed(self.segments):
            if d >= seg.active_from:
                return seg.ticker

        # Avant le premier segment
        return self.segments[0].ticker

    def segment_for_ticker(self, ticker: str) -> RolloverSegment | None:
        """Retourne le segment correspondant à un ticker.

        :param ticker: Ticker du contrat (ex: "ESM5").
        :return: Le segment, ou None si le ticker n'est pas dans la chaîne.
        """
        for seg in self.segments:
            if seg.ticker == ticker:
                return seg
        return None

    def continuous_segments(self, start: date, end: date) -> list[RolloverSegment]:
        """Retourne les segments couvrant la période [start, end].

        :param start: Date de début (inclusive).
        :param end: Date de fin (inclusive).
        :return: Liste des segments dont la période active chevauche [start, end].
        """
        result: list[RolloverSegment] = []
        for seg in self.segments:
            # Un segment chevauche [start, end] si active_from <= end ET active_until > start
            if seg.active_from <= end and seg.active_until > start:
                result.append(seg)
        return result

    def tick_size_for_ticker(self, ticker: str) -> float:
        """Retourne le ``trade_tick_size`` d'un contrat.

        :param ticker: Ticker du contrat.
        :return: La taille du tick (ex: 0.25 pour ES), ou 0.0 si introuvable.
        """
        seg = self.segment_for_ticker(ticker)
        if seg is not None:
            return seg.trade_tick_size
        logger.warning(f"trade_tick_size introuvable pour {ticker}")
        return 0.0

    def to_table(self) -> pl.DataFrame:
        """Retourne un DataFrame Polars plat des segments (pour ``status``).

        :return: DataFrame avec colonnes : ticker, first_trade_date, last_trade_date,
            rollover_date, active_from, active_until, trade_tick_size, name.
        """
        if not self.segments:
            return pl.DataFrame()

        rows = [
            {
                "ticker": seg.ticker,
                "first_trade_date": seg.first_trade_date,
                "last_trade_date": seg.last_trade_date,
                "rollover_date": seg.rollover_date,
                "active_from": seg.active_from,
                "active_until": seg.active_until,
                "trade_tick_size": seg.trade_tick_size,
                "name": seg.name,
            }
            for seg in self.segments
        ]
        return pl.DataFrame(rows)

    def __repr__(self) -> str:
        if not self.segments:
            return f"RolloverChain({self.product_code}: 0 segment)"
        lines = [f"RolloverChain({self.product_code}: {len(self.segments)} segments)"]
        for seg in self.segments:
            lines.append(
                f"  {seg.ticker}  active={seg.active_from}..{seg.active_until}  "
                f"rollover={seg.rollover_date}  tick={seg.trade_tick_size}"
            )
        return "\n".join(lines)

    def __len__(self) -> int:
        return len(self.segments)


def _next_business_day(d: date) -> date:
    """Jour ouvré (lundi–vendredi) strictement après ``d``.

    Pas de calendrier de jours fériés : un férié ne porte simplement pas de données.

    :param d: Date de référence.
    :return: Le lendemain de ``d``, ou le lundi suivant si ``d`` est un vendredi/samedi.
    """
    nxt = d + timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return nxt
