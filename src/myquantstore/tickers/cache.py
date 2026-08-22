"""Cache Parquet du référentiel tickers (+ types), shards market × active.

Layout ::

    {cache_dir}/tickers/
      types.parquet (+ .meta.json)
      stocks/active.parquet (+ .meta.json)
      stocks/inactive.parquet (+ .meta.json)
      fx/active.parquet
      …

TTL commun : ``settings.instrument_cache_ttl_days`` (par shard).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from myquantstore.api.client import MassiveClient
from myquantstore.api.tickers import fetch_all_tickers, fetch_ticker_types
from myquantstore.config import Settings
from myquantstore.logging_setup import get_logger, log_cache_skip
from myquantstore.storage.parquet_io import read_meta, read_parquet, write_parquet

logger = get_logger("tickers.cache")

# Markets supportés pour refresh multi / --markets all
DEFAULT_MARKETS = ("stocks",)
KNOWN_MARKETS = ("stocks", "fx", "indices", "otc", "crypto")

ActiveBucket = str  # "active" | "inactive"


@dataclass(frozen=True)
class TickerShardStatus:
    """État d'un shard tickers ``{market}/{active|inactive}``."""

    market: str
    bucket: ActiveBucket
    path: Path
    exists: bool
    row_count: int | None
    last_fetched_at: str | None
    fresh: bool


def active_to_bucket(active: bool) -> ActiveBucket:
    return "active" if active else "inactive"


def parse_csv_list(
    value: str | Sequence[str] | None,
    *,
    lower: bool = False,
) -> list[str]:
    """Parse une liste CSV (virgules uniquement — pas d'espaces comme séparateur).

    Accepte une string ``"a,b"``, une séquence de tokens (chacun éventuellement
    CSV), ou ``None`` → ``[]``. Déduplique en conservant l'ordre.
    """
    if value is None:
        return []

    parts: list[str]
    if isinstance(value, str):
        parts = [value]
    else:
        parts = [str(v) for v in value]

    seen: set[str] = set()
    out: list[str] = []
    for part in parts:
        for token in part.split(","):
            t = token.strip()
            if lower:
                t = t.lower()
            if not t or t in seen:
                continue
            seen.add(t)
            out.append(t)
    return out


def parse_markets_arg(
    values: str | Sequence[str] | None,
    *,
    default: tuple[str, ...] = DEFAULT_MARKETS,
) -> list[str]:
    """Parse ``--markets stocks,fx`` ou ``all`` (CSV uniquement, pas d'espaces).

    :raises ValueError: si un market n'est pas dans :data:`KNOWN_MARKETS` (hors ``all``).
    :return: Liste de markets dédupliquée (ordre conservé).
    """
    raw = parse_csv_list(values, lower=True)
    if not raw:
        return list(default)

    if any(m == "all" for m in raw):
        return list(KNOWN_MARKETS)

    unknown = [m for m in raw if m not in KNOWN_MARKETS]
    if unknown:
        known = ", ".join(KNOWN_MARKETS)
        bad = ", ".join(unknown)
        raise ValueError(
            f"Market(s) inconnu(s): {bad}. "
            f"Valeurs acceptées: {known}, ou all (CSV: stocks,fx)."
        )
    return raw


def parse_active_buckets(active_flag: str) -> list[bool]:
    """``true`` → [True], ``false`` → [False], ``all`` → [True, False]."""
    if active_flag == "true":
        return [True]
    if active_flag == "false":
        return [False]
    return [True, False]


class TickersCache:
    """Cache multi-shards de l'univers tickers (``/v3/reference/tickers``)."""

    def __init__(self, settings: Settings):
        self._settings = settings

    def shard_path(self, market: str, active: bool) -> Path:
        return self._settings.tickers_shard_path(market, active_to_bucket(active))

    def list_shard_paths(
        self,
        markets: list[str] | None = None,
        active: bool | None = None,
    ) -> list[Path]:
        """Liste les parquets existants correspondant aux filtres.

        :param markets: Si None, tous les markets trouvés sur disque.
        :param active: Si None, active + inactive.
        """
        return [s.path for s in self.inventory(markets=markets, active=active) if s.exists]

    def inventory(
        self,
        markets: list[str] | None = None,
        active: bool | None = None,
        *,
        include_missing: bool = False,
    ) -> list[TickerShardStatus]:
        """Inventaire des shards tickers (présents, et optionnellement manquants).

        :param markets: Markets à inspecter. Si None : markets trouvés sur disque
            (+ KNOWN_MARKETS si ``include_missing``).
        :param active: Filtre active/inactive (None = les deux).
        :param include_missing: Si True, inclut les shards absents pour chaque
            market de ``KNOWN_MARKETS`` (ou ``markets``).
        """
        buckets: list[ActiveBucket]
        if active is True:
            buckets = ["active"]
        elif active is False:
            buckets = ["inactive"]
        else:
            buckets = ["active", "inactive"]

        root = self._settings.tickers_cache_dir()
        if markets is not None:
            market_list = list(markets)
        elif include_missing:
            market_list = list(KNOWN_MARKETS)
        elif root.exists():
            market_list = sorted(
                p.name for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")
            )
        else:
            market_list = []

        # Si on scanne le disque sans include_missing, ne lister que l'existant
        statuses: list[TickerShardStatus] = []
        for m in market_list:
            for b in buckets:
                path = self._settings.tickers_shard_path(m, b)
                exists = path.exists()
                if not exists and not include_missing:
                    continue
                meta = read_meta(path) if exists else None
                row_count: int | None = None
                last: str | None = None
                if meta is not None:
                    rc = meta.get("row_count")
                    row_count = int(rc) if rc is not None else None
                    lf = meta.get("last_fetched_at")
                    last = str(lf) if lf is not None else None
                is_active = b == "active"
                statuses.append(
                    TickerShardStatus(
                        market=m,
                        bucket=b,
                        path=path,
                        exists=exists,
                        row_count=row_count,
                        last_fetched_at=last,
                        fresh=self.is_shard_fresh(m, is_active) if exists else False,
                    )
                )
        return statuses

    @property
    def exists(self) -> bool:
        """True s'il existe au moins un shard (hors types)."""
        return bool(self.list_shard_paths())

    def legacy_all_path(self) -> Path:
        return self._settings.tickers_all_path()

    def warn_legacy_layout(self) -> None:
        legacy = self.legacy_all_path()
        if legacy.exists() and not self.exists:
            logger.warning(
                f"Ancien layout détecté ({legacy}). "
                "Relancez 'myquantstore tickers refresh' pour migrer vers "
                "cache/tickers/{{market}}/{{active|inactive}}.parquet"
            )

    def is_shard_fresh(self, market: str, active: bool) -> bool:
        path = self.shard_path(market, active)
        if not path.exists():
            return False
        meta = read_meta(path)
        if meta is None or "last_fetched_at" not in meta:
            return False
        try:
            last = datetime.fromisoformat(str(meta["last_fetched_at"]))
            if last.tzinfo is None:
                last = last.replace(tzinfo=UTC)
        except (TypeError, ValueError):
            return False
        age = datetime.now(UTC) - last
        return age < timedelta(days=self._settings.instrument_cache_ttl_days)

    def get_shard_last_fetched(self, market: str, active: bool) -> str | None:
        meta = read_meta(self.shard_path(market, active))
        if meta is None:
            return None
        val = meta.get("last_fetched_at")
        return str(val) if val is not None else None

    def refresh_shard(
        self,
        client: MassiveClient,
        market: str,
        active: bool,
        *,
        force: bool = False,
    ) -> pl.DataFrame:
        """Fetch + écrit un shard ``(market, active)`` si absent/périmé."""
        if not force and self.is_shard_fresh(market, active):
            last = self.get_shard_last_fetched(market, active) or "inconnu"
            log_cache_skip(logger, "tickers", f"{market}/{active_to_bucket(active)}", last)
            return read_parquet(self.shard_path(market, active))

        logger.info(
            f"Cache miss/périmé: fetch /v3/reference/tickers "
            f"market={market} active={active}"
        )
        df = fetch_all_tickers(
            client,
            self._settings,
            market=market,
            active=active,
        )
        self._write_shard(df, market=market, active=active)
        return df

    def refresh(
        self,
        client: MassiveClient,
        *,
        markets: list[str],
        active_flags: list[bool],
        force: bool = False,
    ) -> pl.DataFrame:
        """Refresh plusieurs shards et retourne le concat des shards touchés."""
        frames: list[pl.DataFrame] = []
        for market in markets:
            for active in active_flags:
                df = self.refresh_shard(client, market, active, force=force)
                if not df.is_empty():
                    frames.append(df)
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames, how="diagonal_relaxed")

    def read_concat(
        self,
        markets: list[str] | None = None,
        active: bool | None = None,
    ) -> pl.DataFrame:
        """Concatène les shards existants (lecture pure, pas d'API)."""
        self.warn_legacy_layout()
        paths = self.list_shard_paths(markets=markets, active=active)
        if not paths:
            # fallback legacy all.parquet
            legacy = self.legacy_all_path()
            if legacy.exists():
                logger.warning(f"Lecture legacy {legacy} — migrez avec tickers refresh")
                return read_parquet(legacy)
            raise FileNotFoundError(
                f"Aucun shard tickers trouvé sous {self._settings.tickers_cache_dir()}. "
                "Exécutez 'myquantstore tickers refresh'."
            )
        frames = [read_parquet(p) for p in paths]
        if len(frames) == 1:
            return frames[0]
        return pl.concat(frames, how="diagonal_relaxed")

    def ensure(
        self,
        client: MassiveClient | None,
        *,
        markets: list[str] | None = None,
        active: bool | None = True,
        force: bool = False,
        no_cascade: bool = False,
    ) -> pl.DataFrame:
        """Assure des shards frais puis retourne le concat.

        Défaut cascade : markets=['stocks'], active=True.
        """
        mkts = markets if markets else list(DEFAULT_MARKETS)
        active_flags = [True, False] if active is None else [active]

        if no_cascade:
            return self.read_concat(markets=mkts, active=active)

        need_fetch = force or any(
            not self.is_shard_fresh(m, a) for m in mkts for a in active_flags
        )
        if not need_fetch:
            return self.read_concat(markets=mkts, active=active)

        if client is None:
            # tenter lecture partielle
            try:
                return self.read_concat(markets=mkts, active=active)
            except FileNotFoundError:
                raise ValueError(
                    "Cache tickers absent/périmé et aucun client API. "
                    "Exécutez 'myquantstore setup-key' puis 'tickers refresh'."
                ) from None

        return self.refresh(client, markets=mkts, active_flags=active_flags, force=force)

    def _write_shard(self, df: pl.DataFrame, *, market: str, active: bool) -> None:
        path = self.shard_path(market, active)
        now = datetime.now(UTC).isoformat()
        write_parquet(
            df,
            path,
            source_url="/v3/reference/tickers",
            last_fetched_at=now,
            market_filter=market,
            active_filter=active,
            shard=f"{market}/{active_to_bucket(active)}",
        )
        logger.info(f"Cache tickers écrit : {path} ({df.height} lignes)")


class TickerTypesCache:
    """Cache des types de tickers (``/v3/reference/tickers/types``)."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._parquet_path = settings.tickers_types_path()

    @property
    def parquet_path(self) -> Path:
        return self._parquet_path

    @property
    def exists(self) -> bool:
        return self._parquet_path.exists()

    def get_last_fetched(self) -> str | None:
        meta = read_meta(self._parquet_path)
        if meta is None:
            return None
        val = meta.get("last_fetched_at")
        return str(val) if val is not None else None

    def is_fresh(self) -> bool:
        """True si le cache types existe et est dans le TTL."""
        return self._is_fresh()

    def get(
        self,
        client: MassiveClient | None = None,
        force_refresh: bool = False,
    ) -> pl.DataFrame:
        if not force_refresh and self.is_fresh():
            last = self.get_last_fetched() or "inconnu"
            log_cache_skip(logger, "ticker_types", "all", last)
            return read_parquet(self._parquet_path)

        if client is None:
            raise ValueError(
                "Cache ticker types absent/périmé et aucun client API fourni. "
                "Exécutez 'myquantstore tickers types --force' ou 'tickers refresh'."
            )

        logger.info("Cache miss/périmé: fetch /v3/reference/tickers/types")
        df = fetch_ticker_types(client)
        self._write(df)
        return df

    def read(self) -> pl.DataFrame:
        if not self.exists:
            raise FileNotFoundError(
                f"Cache ticker types introuvable : {self._parquet_path}. "
                "Exécutez 'myquantstore tickers refresh'."
            )
        return read_parquet(self._parquet_path)

    def _is_fresh(self) -> bool:
        if not self.exists:
            return False
        meta = read_meta(self._parquet_path)
        if meta is None or "last_fetched_at" not in meta:
            return False
        try:
            last = datetime.fromisoformat(str(meta["last_fetched_at"]))
            if last.tzinfo is None:
                last = last.replace(tzinfo=UTC)
        except (TypeError, ValueError):
            return False
        age = datetime.now(UTC) - last
        return age < timedelta(days=self._settings.instrument_cache_ttl_days)

    def _write(self, df: pl.DataFrame) -> None:
        now = datetime.now(UTC).isoformat()
        write_parquet(
            df,
            self._parquet_path,
            source_url="/v3/reference/tickers/types",
            last_fetched_at=now,
        )
        logger.info(f"Cache ticker types écrit : {self._parquet_path} ({df.height} lignes)")
