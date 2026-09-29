"""Stabilité aggregate → query sur les données réelles (``doctor stability``).

Reconstruit l'agrégat d'un instrument × résolution ``repeats`` fois depuis les
dumps réels, dans un **dossier temporaire** (``aggregate_subdir`` redirigé), puis
compare strictement (valeurs, dtypes, ordre des lignes) l'agrégat et un jeu fixe
de variantes ``query()`` d'une reconstruction à l'autre.

**Lecture seule** : ``data/raw`` est lu, ``data/aggregate`` n'est jamais écrit ;
les caches (contrats, corporate actions) sont lus sans réseau. Les variantes
stocks passent ``no_split=True`` (le cache Yahoo se rafraîchirait sinon).

L'agrégat sur disque est aussi comparé à la reconstruction : un écart signale un
agrégat **périmé** (dumps plus récents, ou écrit par une version antérieure) —
information, pas une instabilité.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
from polars.testing import assert_frame_equal

from myquantstore.chains import InstrumentChain
from myquantstore.config import Settings
from myquantstore.instruments import RESOLUTION_1DAY, Instrument, InstrumentType
from myquantstore.pipeline.aggregator import aggregate
from myquantstore.query.reader import query
from myquantstore.storage.aggregate_cache import aggregate_exists, read_aggregate

DISK_IDENTICAL = "identique"
DISK_STALE = "périmé"
DISK_MISSING = "absent"


@dataclass
class Divergence:
    """Écart entre la 1re reconstruction et une suivante."""

    step: str
    """``agrégat`` ou nom de la variante query."""
    rebuild: int
    """Numéro de la reconstruction divergente (2..repeats)."""
    detail: str


@dataclass
class StabilityReport:
    """Résultat pour un instrument × résolution."""

    instrument: Instrument
    resolution: str
    repeats: int
    rows: int = 0
    variants: list[str] = field(default_factory=list)
    divergences: list[Divergence] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)
    disk_status: str = DISK_MISSING
    disk_detail: str = ""
    """Écart orienté disque → reconstruction (vide si identique)."""

    @property
    def aggregate_stable(self) -> bool:
        return not any(d.step == "agrégat" for d in self.divergences)

    @property
    def unstable_variants(self) -> list[str]:
        return sorted({d.step for d in self.divergences if d.step != "agrégat"})

    @property
    def ok(self) -> bool:
        return not self.divergences and not self.errors


def query_variants(
    instrument: Instrument,
    resolution: str,
    chain: InstrumentChain | None,
) -> dict[str, tuple[InstrumentChain | None, dict[str, Any]]]:
    """Variantes ``query()`` rejouées : ``{nom: (chaîne, kwargs)}``."""
    if resolution == RESOLUTION_1DAY:
        base: dict[str, dict[str, Any]] = {
            "1day": {},
            "5 jours": {"k_days": 5},
            "semaine": {"k_days": 7, "week_aligned": True},
        }
    else:
        base = {
            "1min": {},
            "1min sans dédup": {"dedup_timestamps": False},
            "5min": {"k_minutes": 5},
            "1h": {"k_minutes": 60},
            "5min forward fill": {"k_minutes": 5, "forward_fill": True},
        }
    if instrument.type == InstrumentType.STOCKS:
        base = {name: {**kw, "no_split": True} for name, kw in base.items()}
    if instrument.type != InstrumentType.FUTURES:
        return {name: (chain, kw) for name, kw in base.items()}
    variants: dict[str, tuple[InstrumentChain | None, dict[str, Any]]] = {}
    if chain is not None:
        variants.update({f"chaîne · {n}": (chain, kw) for n, kw in base.items()})
    variants.update({f"sans chaîne · {n}": (None, kw) for n, kw in base.items()})
    return variants


def _as_comparable(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [pl.col(c).cast(pl.Utf8) for c, t in df.schema.items() if t == pl.Categorical]
    )


def describe_difference(reference: pl.DataFrame, current: pl.DataFrame) -> str | None:
    """``None`` si strictement identiques, sinon un résumé du premier écart."""
    try:
        assert_frame_equal(reference, current, check_exact=True, categorical_as_str=True)
        return None
    except AssertionError:
        pass
    ref, cur = _as_comparable(reference), _as_comparable(current)
    if ref.schema != cur.schema:
        changed = sorted(set(ref.schema.items()).symmetric_difference(cur.schema.items()), key=str)
        return f"schéma différent : {', '.join(f'{c} ({t})' for c, t in changed)}"
    if ref.height != cur.height:
        return f"{ref.height} → {cur.height} lignes"
    columns = [c for c in ref.columns if not ref[c].equals(cur[c])]
    if not columns:
        return "écart non localisé"
    mask = pl.Series([False] * ref.height)
    for c in columns:
        mask = mask | ref[c].ne_missing(cur[c]).fill_null(True)
    first = int(mask.arg_true()[0])
    where = f"ligne {first}"
    if "window_start" in ref.columns:
        where = f"1re à {ref['window_start'][first]}"
    return f"{int(mask.sum())} ligne(s) diffèrent ({where}) ; colonnes : {', '.join(columns)}"


def check_stability(
    instrument: Instrument,
    settings: Settings,
    resolution: str,
    chain: InstrumentChain | None,
    *,
    repeats: int = 3,
    start: datetime | None = None,
    end: datetime | None = None,
    timezone: str | None = None,
) -> StabilityReport:
    """Reconstruit ``repeats`` fois dans un dossier temporaire et compare.

    :param chain: Chaîne locale (futures : cache contrats), ``None`` sinon.
    :param start: Borne basse des variantes query (la reconstruction couvre tout).
    :param end: Borne haute des variantes query.
    :param timezone: Override du fuseau (sinon ``resolve_timezone``).
    """
    if repeats < 2:
        raise ValueError(f"repeats doit être >= 2 (reçu: {repeats})")
    report = StabilityReport(instrument=instrument, resolution=resolution, repeats=repeats)
    variants = query_variants(instrument, resolution, chain)
    report.variants = list(variants)

    reference_aggregate: pl.DataFrame | None = None
    reference_queries: dict[str, pl.DataFrame] = {}
    with tempfile.TemporaryDirectory(prefix="mqs-stability-") as tmp:
        work = settings.model_copy(update={"aggregate_subdir": tmp})
        if work.aggregate_dir() != Path(tmp):  # garde-fou : jamais data/aggregate
            raise RuntimeError(f"dossier de reconstruction inattendu : {work.aggregate_dir()}")
        for rebuild in range(1, repeats + 1):
            aggregate(instrument, work, resolution=resolution)
            current_aggregate = read_aggregate(instrument, work, resolution=resolution)
            if reference_aggregate is None:
                reference_aggregate = current_aggregate
                report.rows = current_aggregate.height
            else:
                diff = describe_difference(reference_aggregate, current_aggregate)
                if diff:
                    report.divergences.append(Divergence("agrégat", rebuild, diff))
            for name, (variant_chain, kwargs) in variants.items():
                if name in report.errors:
                    continue
                try:
                    result = query(
                        instrument,
                        work,
                        chain=variant_chain,
                        start=start,
                        end=end,
                        timezone=timezone,
                        resolution=resolution,
                        **kwargs,
                    )
                except Exception as exc:  # une variante en erreur n'arrête pas les autres
                    report.errors[name] = f"{type(exc).__name__}: {exc}"
                    continue
                if name not in reference_queries:
                    reference_queries[name] = result
                    continue
                diff = describe_difference(reference_queries[name], result)
                if diff:
                    report.divergences.append(Divergence(name, rebuild, diff))

    if reference_aggregate is not None and aggregate_exists(
        instrument, settings, resolution=resolution
    ):
        on_disk = read_aggregate(instrument, settings, resolution=resolution)
        diff = describe_difference(on_disk, reference_aggregate)  # disque → reconstruit
        report.disk_status = DISK_STALE if diff else DISK_IDENTICAL
        report.disk_detail = diff or ""
    return report
