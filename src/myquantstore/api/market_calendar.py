"""Fetch des calendriers de marché Massive (fériés actions, séances futures).

Deux endpoints, deux formes de données :

- ``/v1/marketstatus/upcoming`` : fériés et clôtures anticipées des bourses
  actions US (NYSE, NASDAQ). C'est le **même endpoint** pour stocks, forex et
  indices (la doc Massive le liste sous chaque type). Il ne renvoie que les
  fériés **à venir** (environ un an), en un seul appel : la réponse est un
  tableau JSON, sans enveloppe ``results`` ni pagination.
- ``/futures/v1/schedules`` : séances de trading par produit futures, une ligne
  par événement (``pre_open``, ``open``, ``close``) horodaté en UTC et rattaché
  à un ``session_end_date`` (trade date ; une session finit à 17:00 CT).
  Historique + séances publiées à l'avance (~20 mois), paginé (1000 max).

Particularités constatées sur l'API réelle (sonde du 2026-09-27) :

- ``product_code=ES`` renvoie le produit outright **et** « ES Equity Calendar
  Spread » (``YM`` : aussi « YM Butterfly »), avec des horaires identiques :
  chaque événement apparaît deux ou trois fois. Les dumps gardent tout
  (fidélité à l'API), la déduplication se fait à l'agrégation
  (:mod:`myquantstore.market_calendar.store`).
- Un férié futures n'est pas nommé : c'est un ``session_end_date`` absent (jour
  ouvré sans séance) et/ou une séance raccourcie (``close`` avancé) ou
  interrompue (``pre_open`` en cours de séance = arrêt, puis ``open`` = reprise).
- L'historique réel des schedules commence mi-mars 2025, avec quelques
  événements orphelins avant (séances incomplètes).
"""

from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from myquantstore.api.client import MassiveClient
from myquantstore.logging_setup import get_logger

logger = get_logger("market_calendar")

MARKET_HOLIDAYS_PATH = "/v1/marketstatus/upcoming"
FUTURES_SCHEDULES_PATH = "/futures/v1/schedules"
SCHEDULES_PAGE_LIMIT = 1000  # max API pour /futures/v1/schedules

MARKET_HOLIDAYS_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Date(),
    "exchange": pl.Utf8(),
    "name": pl.Utf8(),
    "status": pl.Utf8(),  # closed | early-close
    "open": pl.Datetime("ns", "UTC"),  # renseigné pour early-close uniquement
    "close": pl.Datetime("ns", "UTC"),
}

FUTURES_SCHEDULES_SCHEMA: dict[str, pl.DataType] = {
    "product_code": pl.Utf8(),
    "product_name": pl.Utf8(),
    "trading_venue": pl.Utf8(),
    "session_end_date": pl.Date(),
    "event": pl.Utf8(),  # pre_open | open | close
    "timestamp": pl.Datetime("ns", "UTC"),
}


def fetch_market_holidays(client: MassiveClient) -> pl.DataFrame:
    """Snapshot des fériés à venir (``/v1/marketstatus/upcoming``), normalisé."""
    df = normalize_market_holidays(client.get_list(MARKET_HOLIDAYS_PATH))
    logger.info(f"Fériés à venir: {df.height} ligne(s) ({MARKET_HOLIDAYS_PATH})")
    return df


def fetch_futures_schedules(
    client: MassiveClient,
    product_code: str,
    start: date | None = None,
) -> pl.DataFrame:
    """Événements de séance d'un produit futures depuis ``start`` (inclus), normalisés.

    Sans borne haute : l'API renvoie aussi toutes les séances déjà publiées.
    Tri ``session_end_date.asc`` pour une pagination chronologique stable.
    """
    params: dict[str, Any] = {
        "product_code": product_code,
        "limit": SCHEDULES_PAGE_LIMIT,
        "sort": "session_end_date.asc",
    }
    if start is not None:
        params["session_end_date.gte"] = start.isoformat()
    df = normalize_futures_schedules(client.get_paginated(FUTURES_SCHEDULES_PATH, **params))
    logger.info(f"Schedules {product_code}: {df.height} événement(s) depuis {start or 'le début'}")
    return df


def futures_schedules_url(product_code: str, start: date | None) -> str:
    """URL relative de la requête schedules (audit des dumps, ``--dry-run``)."""
    url = f"{FUTURES_SCHEDULES_PATH}?product_code={product_code}"
    return url + (f"&session_end_date.gte={start.isoformat()}" if start else "")


def normalize_market_holidays(rows: list[dict[str, Any]]) -> pl.DataFrame:
    """Réponse ``/v1/marketstatus/upcoming`` → schéma :data:`MARKET_HOLIDAYS_SCHEMA`."""
    return _normalize(
        rows,
        MARKET_HOLIDAYS_SCHEMA,
        dates=("date",),
        timestamps=("open", "close"),
    ).sort("date", "exchange")


def normalize_futures_schedules(rows: list[dict[str, Any]]) -> pl.DataFrame:
    """Résultats ``/futures/v1/schedules`` → schéma :data:`FUTURES_SCHEDULES_SCHEMA`."""
    return _normalize(
        rows,
        FUTURES_SCHEDULES_SCHEMA,
        dates=("session_end_date",),
        timestamps=("timestamp",),
    ).sort("session_end_date", "timestamp", "event")


def _normalize(
    rows: list[dict[str, Any]],
    schema: dict[str, pl.DataType],
    *,
    dates: tuple[str, ...],
    timestamps: tuple[str, ...],
) -> pl.DataFrame:
    """Colonnes du schéma (absentes → null), dates ``YYYY-MM-DD``, instants ISO → UTC ns."""
    if not rows:
        return pl.DataFrame(schema=schema)
    raw = pl.DataFrame(rows, infer_schema_length=None)
    exprs: list[pl.Expr] = []
    for col in schema:
        value = pl.col(col).cast(pl.Utf8) if col in raw.columns else pl.lit(None, pl.Utf8)
        if col in dates:
            value = value.str.to_date("%Y-%m-%d")
        elif col in timestamps:
            value = value.str.to_datetime(time_unit="ns", time_zone="UTC")
        exprs.append(value.alias(col))
    return raw.select(exprs)
