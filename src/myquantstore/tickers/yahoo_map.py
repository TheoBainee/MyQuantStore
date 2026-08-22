"""Mapping symbole interne Massive → ticker Yahoo Finance.

Règles multi-type :
1. Override explicite ``settings.yahoo_ticker_overrides[symbol]``
   (ou clé ``type:symbol``).
2. Par type :
   - **stocks** : skip warrants/units ; ``.`` → ``-`` ; sinon identité
   - **forex** : ``{PAIR}=X`` (ex: ``EURUSD`` → ``EURUSD=X``)
   - **indices** : ``^{SYMBOL}`` (ex: ``NDX`` → ``^NDX``)
   - **futures** : ``{ROOT}=F`` continu Yahoo (ex: ``ES`` → ``ES=F``)
3. Options / types non supportés → ``UnmappableTickerError``.
"""

from __future__ import annotations

import re

from myquantstore.instruments import Instrument, InstrumentType

# Suffixes Massive typiques des warrants / units / rights — hors scope stocks.
_SKIP_RE = re.compile(
    r"(\.WS|\.W$|\.U$|\.UN$|\.R$|\.RT$|\.RIGHTS$|\.PW$|-WS$|-W$|-U$|-UN$|-R$|-RT$)$",
    re.IGNORECASE,
)

# Types supportés pour le track Yahoo 1day.
YAHOO_DAILY_TYPES: frozenset[InstrumentType] = frozenset(
    {
        InstrumentType.STOCKS,
        InstrumentType.FOREX,
        InstrumentType.INDICES,
        InstrumentType.FUTURES,
    }
)


class UnmappableTickerError(ValueError):
    """Symbole non mappable vers Yahoo (skip stocks ou type non supporté)."""


def is_skipped_stock_symbol(symbol: str) -> bool:
    """True si le symbole stock est hors scope (warrants/units…)."""
    s = symbol.strip().upper()
    if _SKIP_RE.search(s):
        return True
    # Preferred / class shares purement suffixés .P
    return bool(s.endswith(".P") or s.endswith("-P"))


def to_yahoo_ticker(
    instrument: Instrument,
    overrides: dict[str, str] | None = None,
) -> str:
    """Convertit un instrument interne vers un ticker Yahoo.

    :param instrument: Instrument (symbole nu Massive).
    :param overrides: Table ``{symbol: yahoo_ticker}`` (config ``[yahoo]``).
    :return: Ticker Yahoo (ex: ``BRK-A``, ``EURUSD=X``, ``^NDX``, ``ES=F``).
    :raises UnmappableTickerError: Type non supporté ou symbole skippé stocks.
    """
    overrides = overrides or {}
    symbol = instrument.symbol.strip()

    if symbol in overrides:
        return overrides[symbol]
    # Clé type:symbol aussi acceptée
    if instrument.key in overrides:
        return overrides[instrument.key]

    itype = instrument.type
    if itype not in YAHOO_DAILY_TYPES:
        raise UnmappableTickerError(
            f"Mapping Yahoo daily non supporté pour {instrument.key}"
        )

    if itype == InstrumentType.STOCKS:
        if is_skipped_stock_symbol(symbol):
            raise UnmappableTickerError(
                f"Symbole '{symbol}' skippé (warrant/unit/preferred). "
                "Ajoutez un override dans [yahoo] ticker_overrides si besoin."
            )
        # Massive class shares : BRK.A → BRK-A
        return symbol.replace(".", "-")

    if itype == InstrumentType.FOREX:
        # EURUSD → EURUSD=X
        upper = symbol.upper()
        if upper.endswith("=X"):
            return upper
        return f"{upper}=X"

    if itype == InstrumentType.INDICES:
        # NDX → ^NDX
        upper = symbol.upper()
        if upper.startswith("^"):
            return upper
        return f"^{upper}"

    if itype == InstrumentType.FUTURES:
        # ES → ES=F (continu Yahoo front-month)
        upper = symbol.upper()
        if upper.endswith("=F"):
            return upper
        return f"{upper}=F"

    raise UnmappableTickerError(
        f"Mapping Yahoo daily non supporté pour {instrument.key}"
    )
