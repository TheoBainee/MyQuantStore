"""Lecture des overlays backtest ({overlay_dir}/Backtests/), format v2.

Contrat du format meta.json : ``docs/OVERLAYS.md``. Le parseur est strict —
un fichier non conforme est ignoré et listé dans ``skipped``, il ne fait pas
tomber le catalogue entier.
"""

from __future__ import annotations

import json
import os
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

_STEM_RE = re.compile(r"^[A-Za-z0-9_.-]+$")

#: Version de schéma acceptée dans la clé ``mqs_overlay``.
OVERLAY_SCHEMA_VERSION = 2

#: Unités d'UT reconnues → minutes. Même vocabulaire que le sélecteur d'UT du chart.
_UNIT_MINUTES: dict[str, int] = {"min": 1, "hour": 60, "day": 1440, "week": 10080}

#: Suffixe court par unité, pour les labels.
_UNIT_SUFFIX: dict[str, str] = {"min": "min", "hour": "h", "day": "d", "week": "w"}

#: Plafond de valeurs distinctes exposées par clé dans les facettes.
_MAX_FACET_VALUES = 50


class OverlayFormatError(ValueError):
    """Fichier meta.json non conforme au contrat v2."""


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


# --- UT (timeframe) ---------------------------------------------------------


def _timeframe(raw: object, *, where: str) -> dict[str, Any]:
    """Valide ``{unit, nb}`` et le canonicalise en ``{unit, nb, minutes}``."""
    if not isinstance(raw, dict):
        raise OverlayFormatError(f"{where}: objet {{unit, nb}} attendu")
    unit = raw.get("unit")
    nb = raw.get("nb")
    if not isinstance(unit, str) or unit not in _UNIT_MINUTES:
        raise OverlayFormatError(
            f"{where}.unit invalide ({unit!r}), attendu l'un de {sorted(_UNIT_MINUTES)}"
        )
    if isinstance(nb, bool) or not isinstance(nb, int) or nb <= 0:
        raise OverlayFormatError(f"{where}.nb doit être un entier > 0 (trouvé {nb!r})")
    return {"unit": unit, "nb": nb, "minutes": nb * _UNIT_MINUTES[unit]}


def timeframe_label(tf: dict[str, Any]) -> str:
    """``{unit: min, nb: 5}`` → ``5min``."""
    return f"{tf['nb']}{_UNIT_SUFFIX[str(tf['unit'])]}"


# --- Params ----------------------------------------------------------------


def _flatten(obj: Any, prefix: str = "") -> dict[str, Any]:
    """Aplatit les objets imbriqués en chemins pointés (``session.tz``).

    Les scalaires (bool / int / float / str / None) sont conservés tels quels.
    Les listes et types exotiques sont stringifiés : consultables au tooltip,
    hors comparaison numérique.
    """
    out: dict[str, Any] = {}
    if not isinstance(obj, dict):
        return out
    for key, value in obj.items():
        path = f"{prefix}{key}"
        if isinstance(value, dict):
            out.update(_flatten(value, f"{path}."))
        elif value is None or isinstance(value, (bool, int, float, str)):
            out[path] = value
        else:
            out[path] = json.dumps(value, ensure_ascii=False)
    return out


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return f"{value:g}"
    return str(value)


def _format_param(key: str, value: Any) -> str:
    """Rend un param saillant pour le label.

    Les booléens sortent en nom nu (préfixe ``is_`` retiré) : ``short`` si vrai,
    ``¬short`` si faux. Les autres en ``clé=valeur``.
    """
    if isinstance(value, bool):
        name = key.split(".")[-1].removeprefix("is_")
        return name if value else f"¬{name}"
    return f"{key}={_format_value(value)}"


# --- Parsing ---------------------------------------------------------------


def _parse_meta(stem: str, raw: object) -> list[dict[str, Any]]:
    """Valide un meta.json v2 → une entrée par backtest.

    Lève :class:`OverlayFormatError` au premier manquement au contrat.
    """
    if not isinstance(raw, dict):
        raise OverlayFormatError("racine JSON: objet attendu")

    version = raw.get("mqs_overlay")
    if version != OVERLAY_SCHEMA_VERSION:
        raise OverlayFormatError(
            f"mqs_overlay absent ou != {OVERLAY_SCHEMA_VERSION} (trouvé {version!r})"
        )

    backtest_type = raw.get("backtest_type")
    if not isinstance(backtest_type, str) or not backtest_type.strip():
        raise OverlayFormatError("backtest_type manquant ou vide")

    instrument = raw.get("instrument")
    if not isinstance(instrument, str) or not instrument.strip():
        raise OverlayFormatError("instrument manquant ou vide")

    if "timeframe" not in raw:
        raise OverlayFormatError("timeframe manquant (UT de calcul obligatoire)")
    timeframe = _timeframe(raw["timeframe"], where="timeframe")

    backtests = raw.get("backtests")
    if not isinstance(backtests, dict) or not backtests:
        raise OverlayFormatError("backtests manquant ou vide")

    shared_raw = raw.get("shared", {})
    if not isinstance(shared_raw, dict):
        raise OverlayFormatError("shared doit être un objet")
    shared = _flatten(shared_raw)

    label_params: list[str] | None = None
    label_params_raw = raw.get("label_params")
    if label_params_raw is not None:
        if not isinstance(label_params_raw, list) or not all(
            isinstance(k, str) for k in label_params_raw
        ):
            raise OverlayFormatError("label_params doit être une liste de chaînes")
        label_params = list(label_params_raw)

    entries: list[dict[str, Any]] = []
    for bid, body in backtests.items():
        if not isinstance(bid, str) or not bid:
            raise OverlayFormatError("clé de backtest vide")
        if body is None:
            body = {}
        if not isinstance(body, dict):
            raise OverlayFormatError(f"backtests[{bid}]: objet attendu")

        params_raw = body.get("params", {})
        if not isinstance(params_raw, dict):
            raise OverlayFormatError(f"backtests[{bid}].params doit être un objet")
        params = dict(shared)
        params.update(_flatten(params_raw))

        entry_type = body.get("backtest_type", backtest_type)
        if not isinstance(entry_type, str) or not entry_type.strip():
            raise OverlayFormatError(f"backtests[{bid}].backtest_type vide")

        entry_instrument = body.get("instrument", instrument)
        if not isinstance(entry_instrument, str) or not entry_instrument.strip():
            raise OverlayFormatError(f"backtests[{bid}].instrument vide")

        entry_tf = (
            _timeframe(body["timeframe"], where=f"backtests[{bid}].timeframe")
            if "timeframe" in body
            else timeframe
        )

        entries.append(
            {
                "key": f"{stem}|{bid}",
                "stem": stem,
                "id": bid,
                "backtest_type": entry_type,
                "instrument": entry_instrument,
                "timeframe": entry_tf,
                "params": params,
                "_label_params": label_params,
            }
        )
    return entries


# --- Scan + cache ----------------------------------------------------------

_Fingerprint = tuple[tuple[str, int, int], ...]

#: Cache de parsing par dossier Backtests, invalidé par (nom, mtime_ns, taille).
_PARSE_CACHE: dict[str, tuple[_Fingerprint, list[dict[str, Any]], list[dict[str, str]]]] = {}
_CACHE_LOCK = threading.Lock()


def _fingerprint(bdir: Path) -> _Fingerprint:
    """Empreinte du dossier : (nom, mtime_ns, taille) des ``*.meta.json``, triée."""
    items: list[tuple[str, int, int]] = []
    with os.scandir(bdir) as entries:
        for dirent in entries:
            if not dirent.name.endswith(".meta.json"):
                continue
            try:
                if not dirent.is_file():
                    continue
                stat = dirent.stat()
            except OSError:
                continue
            items.append((dirent.name, stat.st_mtime_ns, stat.st_size))
    return tuple(sorted(items))


def _scan(bdir: Path) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Parse tous les meta.json du dossier (avec cache mtime/taille)."""
    fingerprint = _fingerprint(bdir)
    cache_key = str(bdir)
    with _CACHE_LOCK:
        cached = _PARSE_CACHE.get(cache_key)
        if cached is not None and cached[0] == fingerprint:
            return cached[1], cached[2]

    entries: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    for name, _mtime, _size in fingerprint:
        stem = name.removesuffix(".meta.json")
        try:
            _safe_stem(stem)
            entries.extend(_parse_meta(stem, json.loads((bdir / name).read_text("utf-8"))))
        except json.JSONDecodeError as exc:
            skipped.append({"file": name, "reason": f"JSON invalide: {exc}"})
        except (OSError, ValueError) as exc:
            skipped.append({"file": name, "reason": str(exc)})

    with _CACHE_LOCK:
        _PARSE_CACHE[cache_key] = (fingerprint, entries, skipped)
    return entries, skipped


def clear_catalog_cache() -> None:
    """Vide le cache de parsing (tests, ou rescan forcé)."""
    with _CACHE_LOCK:
        _PARSE_CACHE.clear()


# --- Salience + labels -----------------------------------------------------


def _salient_keys(entries: list[dict[str, Any]]) -> list[str]:
    """Clés discriminantes d'un groupe : >= 2 valeurs distinctes, ou absentes ailleurs.

    Les params invariants d'un même ``backtest_type`` (ticksize, cth_open…) sont
    du bruit : ils restent consultables au tooltip mais ne polluent pas le label.
    """
    order: list[str] = []
    values: dict[str, set[Any]] = {}
    counts: dict[str, int] = {}
    for entry in entries:
        for key, value in entry["params"].items():
            if key not in values:
                values[key] = set()
                counts[key] = 0
                order.append(key)
            values[key].add(value)
            counts[key] += 1
    total = len(entries)
    return [key for key in order if len(values[key]) >= 2 or counts[key] < total]


def _label(entry: dict[str, Any], keys: list[str]) -> str:
    """``type · UT · params saillants``, avec repli sur l'id si aucun saillant."""
    params = entry["params"]
    rendered = [_format_param(key, params[key]) for key in keys if key in params]
    parts = [str(entry["backtest_type"]), timeframe_label(entry["timeframe"])]
    parts.extend(rendered or [str(entry["id"])])
    return " · ".join(parts)


def _facets(
    entries: list[dict[str, Any]], by_type: dict[str, list[dict[str, Any]]]
) -> dict[str, Any]:
    """Facettes de navigation : types, UT, clés de params observées."""
    types = [{"name": name, "count": len(group)} for name, group in sorted(by_type.items())]

    timeframes_by_min: dict[int, dict[str, Any]] = {}
    for entry in entries:
        timeframes_by_min[int(entry["timeframe"]["minutes"])] = entry["timeframe"]
    timeframes = [timeframes_by_min[key] for key in sorted(timeframes_by_min)]

    seen: dict[str, set[Any]] = {}
    ordered_values: dict[str, list[Any]] = {}
    for entry in entries:
        for key, value in entry["params"].items():
            bucket = seen.setdefault(key, set())
            if value not in bucket:
                bucket.add(value)
                ordered_values.setdefault(key, []).append(value)

    param_keys: dict[str, dict[str, Any]] = {}
    for key in sorted(ordered_values):
        values = ordered_values[key]
        if all(isinstance(v, bool) for v in values):
            kind = "bool"
        elif all(not isinstance(v, bool) and isinstance(v, (int, float)) for v in values):
            kind = "number"
        else:
            kind = "string"
        try:
            shown: list[Any] = sorted(values, key=lambda v: (v is None, v))
        except TypeError:
            shown = values
        param_keys[key] = {
            "kind": kind,
            "values": shown[:_MAX_FACET_VALUES],
            "truncated": len(shown) > _MAX_FACET_VALUES,
        }

    return {"types": types, "timeframes": timeframes, "param_keys": param_keys}


def build_catalog(overlay_dir: str | Path | None, product: str) -> dict[str, Any]:
    """Catalogue overlay d'un produit : une ligne par backtest, + facettes.

    La salience est calculée par ``backtest_type`` sur l'ensemble du produit,
    tous stems confondus — d'où un calcul côté serveur plutôt que côté client.
    """
    empty: dict[str, Any] = {
        "overlays": [],
        "facets": {"types": [], "timeframes": [], "param_keys": {}},
        "skipped": [],
    }
    bdir = backtests_dir(overlay_dir)
    if bdir is None or not bdir.is_dir():
        return empty

    all_entries, skipped = _scan(bdir)
    entries = [entry for entry in all_entries if entry["instrument"] == product]
    entries.sort(
        key=lambda e: (
            str(e["backtest_type"]),
            int(e["timeframe"]["minutes"]),
            str(e["stem"]),
            str(e["id"]),
        )
    )

    by_type: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        by_type.setdefault(str(entry["backtest_type"]), []).append(entry)
    salient_by_type = {name: _salient_keys(group) for name, group in by_type.items()}

    overlays: list[dict[str, Any]] = []
    for entry in entries:
        auto = salient_by_type[str(entry["backtest_type"])]
        keys = entry["_label_params"] if entry["_label_params"] is not None else auto
        present = [key for key in keys if key in entry["params"]]
        row = {name: value for name, value in entry.items() if not name.startswith("_")}
        row["salient"] = present
        row["label"] = _label(entry, present)
        overlays.append(row)

    return {"overlays": overlays, "facets": _facets(entries, by_type), "skipped": skipped}


def scan_report(overlay_dir: str | Path | None) -> dict[str, Any]:
    """Rapport de validation tous produits confondus (``doctor overlays``)."""
    bdir = backtests_dir(overlay_dir)
    if bdir is None or not bdir.is_dir():
        return {"dir": str(bdir) if bdir else None, "exists": False, "products": [], "skipped": []}

    all_entries, skipped = _scan(bdir)
    products = sorted({str(entry["instrument"]) for entry in all_entries})
    return {
        "dir": str(bdir),
        "exists": True,
        "products": [
            {"product": product, **build_catalog(overlay_dir, product)} for product in products
        ],
        "skipped": skipped,
    }


# --- Chargement d'un overlay ----------------------------------------------


def load_overlay(
    overlay_dir: str | Path | None,
    stem: str,
    backtest_id: str | None = None,
) -> dict[str, Any]:
    """Payload d'un backtest : métadonnées + transactions + ordres."""
    bdir = backtests_dir(overlay_dir)
    if bdir is None or not bdir.is_dir():
        raise FileNotFoundError("overlay_dir / Backtests introuvable")
    stem = _safe_stem(stem)
    meta_path = bdir / f"{stem}.meta.json"
    tx_path = bdir / f"{stem}.transactions.parquet"
    or_path = bdir / f"{stem}.orders.parquet"
    if not meta_path.is_file():
        raise FileNotFoundError(f"overlay inconnu: {stem}")

    try:
        entries = _parse_meta(stem, json.loads(meta_path.read_text("utf-8")))
    except json.JSONDecodeError as exc:
        raise OverlayFormatError(f"JSON invalide: {exc}") from exc

    by_id = {str(entry["id"]): entry for entry in entries}
    chosen_id = backtest_id or next(iter(by_id))
    if chosen_id not in by_id:
        raise KeyError(chosen_id)
    entry = by_id[chosen_id]

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
        "key": f"{stem}|{chosen_id}",
        "backtest_type": entry["backtest_type"],
        "instrument": entry["instrument"],
        "timeframe": entry["timeframe"],
        "params": entry["params"],
        "ids": list(by_id),
        "transactions": transactions,
        "orders": orders,
    }
