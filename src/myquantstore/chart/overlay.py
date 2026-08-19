"""Lecture des overlays backtest ({overlay_dir}/Backtests/)."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

_STEM_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def backtests_dir(overlay_dir: str | Path | None) -> Path | None:
    if not overlay_dir:
        return None
    root = Path(overlay_dir).expanduser()
    if not root.is_dir():
        return None
    return root / "Backtests"


def _safe_stem(stem: str) -> str:
    if not _STEM_RE.fullmatch(stem) or ".." in stem:
        raise ValueError(f"stem overlay invalide: {stem}")
    return stem


def _iso(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat() + "Z"
        return value.isoformat()
    return str(value)


def list_overlays(overlay_dir: str | Path | None, product: str) -> list[dict[str, Any]]:
    bdir = backtests_dir(overlay_dir)
    if bdir is None or not bdir.is_dir():
        return []
    out: list[dict[str, Any]] = []
    for meta_path in sorted(bdir.glob("*.meta.json")):
        stem = meta_path.name.removesuffix(".meta.json")
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(meta, dict) or not meta:
            ids = [stem]
            inst = product
        else:
            ids = list(meta.keys())
            inst = next(
                (str(v.get("instrument", "")) for v in meta.values() if isinstance(v, dict)),
                "",
            )
        if inst and inst != product:
            continue
        out.append({"stem": stem, "ids": ids, "instrument": inst or product})
    return out


def load_overlay(
    overlay_dir: str | Path | None,
    stem: str,
    backtest_id: str | None = None,
) -> dict[str, Any]:
    bdir = backtests_dir(overlay_dir)
    if bdir is None or not bdir.is_dir():
        raise FileNotFoundError("overlay_dir / Backtests introuvable")
    stem = _safe_stem(stem)
    meta_path = bdir / f"{stem}.meta.json"
    tx_path = bdir / f"{stem}.transactions.parquet"
    or_path = bdir / f"{stem}.orders.parquet"
    if not meta_path.is_file():
        raise FileNotFoundError(f"overlay inconnu: {stem}")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if not isinstance(meta, dict):
        meta = {}
    if not meta:
        meta = {stem: {}}
    chosen_id = backtest_id or next(iter(meta))
    if chosen_id not in meta:
        raise KeyError(chosen_id)

    transactions: list[dict[str, Any]] = []
    if tx_path.is_file():
        tx = pl.read_parquet(tx_path)
        if "backtest_id" in tx.columns:
            tx = tx.filter(pl.col("backtest_id") == chosen_id)
        for row in tx.iter_rows(named=True):
            transactions.append(
                {
                    "backtest_id": row.get("backtest_id", chosen_id),
                    "time": _iso(row.get("time")),
                    "price": float(row["price"]) if row.get("price") is not None else None,
                    "side": row.get("side"),
                    "kind": row.get("kind"),
                }
            )

    orders: list[dict[str, Any]] = []
    if or_path.is_file():
        ords = pl.read_parquet(or_path)
        if "backtest_id" in ords.columns:
            ords = ords.filter(pl.col("backtest_id") == chosen_id)
        for row in ords.iter_rows(named=True):
            orders.append(
                {
                    "backtest_id": row.get("backtest_id", chosen_id),
                    "time_from": _iso(row.get("time_from")),
                    "time_to": _iso(row.get("time_to")),
                    "price": float(row["price"]) if row.get("price") is not None else None,
                    "side": row.get("side"),
                    "order_type": row.get("order_type"),
                }
            )

    return {
        "stem": stem,
        "id": chosen_id,
        "meta": meta[chosen_id],
        "ids": list(meta.keys()),
        "transactions": transactions,
        "orders": orders,
    }
