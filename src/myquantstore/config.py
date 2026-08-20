"""Configuration de MyQuantStore.

Ce module charge la configuration depuis deux sources distinctes :
- ``.env`` : les secrets (clé API, URL de base) via ``pydantic-settings``.
  Jamais committé, chargé depuis ``~/.config/myquantstore/.env``.
- ``config.toml`` : les paramètres métier (instruments, fetch, stockage, rollover…).
  Emplacement principal : ``~/.config/myquantstore/config.toml`` (config utilisateur).
  Fallback : ``config.toml`` dans le répertoire courant (pour dev/repo).

Les deux sources sont fusionnées dans une unique classe :class:`Settings` qui
expose tous les paramètres de manière typée et validée.

**Structure multi-type** : les instruments sont déclarés par type dans la section
``[instruments]`` (``futures``, ``forex``, ``stocks``, ``indices``, ``options``).
Les paramètres spécifiques à un type vivent dans leur propre section
(``[futures]``, ``[stocks]``) pour éviter de polluer la config générique.
"""

from __future__ import annotations

import tomllib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from myquantstore.instruments import DEFAULT_RESOLUTION, Instrument, InstrumentType

# Couleurs chart par défaut (hex #RRGGBB)
_DEFAULT_CANDLE_UP = "#26a69a"
_DEFAULT_CANDLE_DOWN = "#ef5350"
_DEFAULT_TX_BUY = "#2196F3"
_DEFAULT_TX_SELL = "#FF9800"
_DEFAULT_ORDER_BUY = "#2196F3"
_DEFAULT_ORDER_SELL = "#FF9800"


def normalize_hex_color(value: str, *, field_name: str = "color") -> str:
    """Normalise une couleur hex (avec/sans ``#``, 3 ou 6 digits) → ``#RRGGBB``."""
    raw = str(value).strip()
    if raw.startswith("#"):
        raw = raw[1:]
    if len(raw) == 3 and all(c in "0123456789abcdefABCDEF" for c in raw):
        raw = "".join(c * 2 for c in raw)
    if len(raw) != 6 or any(c not in "0123456789abcdefABCDEF" for c in raw):
        raise ValueError(
            f"{field_name} doit être un hex 3/6 digits (ex: 26a69a ou #FF9800), reçu: {value!r}"
        )
    return f"#{raw.upper()}"


def get_user_config_dir() -> Path:
    """Répertoire de configuration utilisateur (XDG Base Directory).

    Retourne ``~/.config/myquantstore``.
    """
    return Path.home() / ".config" / "myquantstore"


def get_user_config_path() -> Path:
    """Chemin du fichier de configuration utilisateur principal.

    Retourne ``~/.config/myquantstore/config.toml``.
    """
    return get_user_config_dir() / "config.toml"


def get_user_env_path() -> Path:
    """Chemin du fichier .env utilisateur principal.

    Retourne ``~/.config/myquantstore/.env``.
    """
    return get_user_config_dir() / ".env"


def get_repo_config_path() -> Path:
    """Chemin du fichier de configuration dans le repo (fallback dev).

    Retourne ``config.toml`` dans le répertoire courant.
    """
    return Path("config.toml")


def resolve_config_path(config_path: str | Path | None = None) -> Path:
    """Chemin du ``config.toml`` effectivement chargé (XDG puis fallback repo).

    :param config_path: Chemin imposé. Si None, résout comme :func:`load_settings`.
    :return: Chemin absolu du fichier existant.
    :raises FileNotFoundError: Si aucun ``config.toml`` n'est trouvé.
    """
    if config_path is None:
        path = get_user_config_path()
    else:
        path = Path(config_path)
    if path.exists():
        return path.resolve()
    fallback = get_repo_config_path()
    if fallback.exists():
        return fallback.resolve()
    raise FileNotFoundError(
        f"Fichier de configuration introuvable : {get_user_config_path()}. "
        "Lancez `myquantstore init` (recommandé), ou copiez "
        f"config.toml.example vers {get_user_config_path()}, "
        "ou placez un config.toml dans le répertoire courant."
    )


def get_repo_env_path() -> Path:
    """Chemin du fichier .env dans le repo (fallback dev).

    Retourne ``.env`` dans le répertoire courant.
    """
    return Path(".env")


class Settings(BaseSettings):
    """Configuration globale de MyQuantStore.

    Les attributs préfixés par ``MASSIVE_`` sont chargés depuis ``.env``
    (``~/.config/myquantstore/.env`` en priorité).
    Les autres attributs sont hydratés depuis ``config.toml`` par la fonction
    :func:`load_settings`.
    """

    model_config = SettingsConfigDict(
        env_prefix="MASSIVE_",
        # env_file résolu dynamiquement dans load_settings (XDG puis repo).
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Secrets (chargés depuis .env) ---
    api_key: str = ""
    base_url: str = "https://api.massive.com"

    # --- Instruments (config.toml: [instruments]) ---
    # Listes compactes par type. Les symboles sont nus (ex: "ES", "AAPL", "EURUSD").
    futures: list[str] = ["NQ", "ES", "RTY", "YM"]
    forex: list[str] = []
    stocks: list[str] = []
    indices: list[str] = []
    options: list[str] = []

    # --- Fetch (config.toml: [fetch]) — générique, commun aux aggs de tous les types ---
    timeframe: str = "1min"
    overlap_buffer_days: int = 1
    # Historique ciblé (mois) par type d'instrument. Défaut: 24 partout sauf indices=60.
    # TOML: [fetch.history_months] futures=24 indices=60 …
    history_months: dict[str, int] = {
        "futures": 24,
        "forex": 24,
        "stocks": 24,
        "indices": 60,
        "options": 24,
    }
    requests_per_minute: int = 6
    page_limit: int = 50000  # max 50000 pour les aggs (futures /v1 et v2)
    max_retries: int = 6

    # --- Storage (config.toml: [storage]) ---
    # Chemins par défaut XDG data ; ~ est expansé dans load_settings.
    data_dir: str = "~/.local/share/myquantstore/data"
    raw_dumps_subdir: str = "raw"
    aggregate_subdir: str = "aggregate"
    # Racine des caches de listing / métadonnées (contrats, corporate actions…)
    cache_dir: str = "~/.local/share/myquantstore/cache"
    contracts_cache_subdir: str = "contracts"  # cache contrats futures
    corporate_actions_cache_subdir: str = "corporate_actions"  # cache splits/dividends stocks (Massive)
    yahoo_actions_cache_subdir: str = "yahoo_actions"  # cache splits/dividends Yahoo (daily)
    tickers_cache_subdir: str = "tickers"  # cache référentiel /v3/reference/tickers
    log_dir: str = "~/.local/share/myquantstore/logs"

    # --- Yahoo Finance (config.toml: [yahoo]) — track extraday 1day ---
    yahoo_requests_per_minute: int = 12
    yahoo_overlap_buffer_days: int = 5
    yahoo_ticker_overrides: dict[str, str] = {}

    # --- Cache instruments (config.toml: [instrument_cache]) — TTL commun à tous les caches ---
    instrument_cache_ttl_days: int = 30

    # --- Health / fraîcheur OHLCV (config.toml: [health]) — jours calendaires ---
    # Agrégé considéré STALE si (today - max(window_start)).days > seuil.
    health_stale_lag_days_1min: int = 3
    health_stale_lag_days_1day: int = 5
    # Warn si |lag_1min - lag_1day| > seuil (dual-source).
    health_cross_resolution_lag_days: int = 7

    # --- Futures (config.toml: [futures]) — spécifique au type futures ---
    days_before_expiry: int = 7
    contracts_page_limit: int = 1000  # max API = 1000 pour /futures/v1/contracts
    # Intervalle (en mois) entre snapshots pour récupérer les contrats expirés.
    contracts_snapshot_interval_months: int = 1

    # --- Stocks (config.toml: [stocks]) — spécifique au type stocks ---
    splits_page_limit: int = 5000  # max API = 5000 pour /stocks/v1/splits
    dividends_page_limit: int = 5000  # max API = 5000 pour /stocks/v1/dividends

    # --- Tickers reference (config.toml: [tickers]) ---
    tickers_page_limit: int = 1000  # max API = 1000 pour /v3/reference/tickers

    # --- Tests (config.toml: [tests]) ---
    data_quality_trigger: float = 0.1

    # --- Logging (config.toml: [logging]) ---
    log_level: str = "DEBUG"

    # --- Affichage (config.toml: [display]) ---
    # Limites d'affichage des tableaux Polars dans les commandes CLI
    # (status, contracts, query). Au-delà, le tableau est tronqué.
    display_max_rows: int = 10
    display_max_columns: int = 20

    # --- Chart / Visualisation (config.toml: [chart]) ---
    # Paramètres du serveur de visualisation (commande `myquantstore chart`).
    default_timescale_unit: str = "min"
    default_timescale_nb: int = 5
    default_nb_candle: int = 2000
    max_visible_candles: int = 100000
    buffer_multiplier: int = 3
    fetch_chunk_size: int = 50000
    chart_port: int = 8050
    chart_host: str = "127.0.0.1"
    chart_mdns: bool = False
    # Fenêtre des miniatures dashboard (jours calendaires, track 1day Yahoo).
    thumbnail_lookback_days: int = 90
    # Couleurs chandeliers (hex).
    chart_candle_up: str = _DEFAULT_CANDLE_UP
    chart_candle_down: str = _DEFAULT_CANDLE_DOWN
    # Racine des overlays (sous-dossier Backtests/). Vide = désactivé.
    # TOML: [chart.overlay] overlay_dir (rétrocompat: [chart] overlay_dir).
    overlay_dir: str = ""
    # Couleurs overlay backtest (hex) — [chart.overlay.backtest]
    chart_tx_buy: str = _DEFAULT_TX_BUY
    chart_tx_sell: str = _DEFAULT_TX_SELL
    chart_order_buy: str = _DEFAULT_ORDER_BUY
    chart_order_sell: str = _DEFAULT_ORDER_SELL

    # --- Serve / API query (config.toml: [serve]) ---
    # Bind de ``myquantstore serve`` si --host / --port absents.
    serve_port: int = 8741
    serve_host: str = "127.0.0.1"

    # --- Portfolio / MPT (config.toml: [portfolio]) ---
    portfolio_risk_free_rate: float = 0.04  # annualisé (fallback / source static)
    # "static" = portfolio_risk_free_rate ; "yahoo" = ^IRX (13w T-bill) 1day
    portfolio_rf_source: str = "yahoo"
    portfolio_rf_yahoo_ticker: str = "^IRX"
    portfolio_rf_cache_ttl_days: int = 1
    portfolio_trading_days_per_year: int = 252
    portfolio_min_coverage: float = 0.95  # fraction dates non-null pour garder un titre
    portfolio_frontier_samples: int = 5000
    portfolio_default_lookback_years: int = 5
    portfolio_optim_seed: int = 42
    # Capital par défaut pour ``portfolio allocate`` (override CLI --value).
    portfolio_default_value: float = 20000.0

    # --- Validations ---

    @field_validator(
        "chart_candle_up",
        "chart_candle_down",
        "chart_tx_buy",
        "chart_tx_sell",
        "chart_order_buy",
        "chart_order_sell",
        mode="before",
    )
    @classmethod
    def _chart_hex_colors(cls, v: Any, info: ValidationInfo) -> str:
        return normalize_hex_color(str(v), field_name=info.field_name)

    @field_validator("overlap_buffer_days")
    @classmethod
    def _buffer_non_neg(cls, v: int) -> int:
        if v < 0:
            raise ValueError("overlap_buffer_days doit être >= 0")
        return v

    @field_validator(
        "health_stale_lag_days_1min",
        "health_stale_lag_days_1day",
        "health_cross_resolution_lag_days",
    )
    @classmethod
    def _health_lag_non_neg(cls, v: int) -> int:
        if v < 0:
            raise ValueError("seuils health lag doivent être >= 0")
        return v

    @field_validator("days_before_expiry")
    @classmethod
    def _days_before_non_neg(cls, v: int) -> int:
        if v < 0:
            raise ValueError("days_before_expiry doit être >= 0")
        return v

    @field_validator("history_months", mode="before")
    @classmethod
    def _history_months_normalize(cls, v: Any) -> dict[str, int]:
        """Normalise history_months (int legacy ou dict) et valide >= 1 par type."""
        defaults: dict[str, int] = {
            "futures": 24,
            "forex": 24,
            "stocks": 24,
            "indices": 60,
            "options": 24,
        }
        if isinstance(v, int):
            # Compat: un seul entier s'applique à tous les types
            if v < 1:
                raise ValueError("history_months doit être >= 1")
            return {t.value: v for t in InstrumentType}
        if not isinstance(v, dict):
            raise ValueError(
                "history_months doit être un entier ou une table "
                "{futures, forex, stocks, indices, options}"
            )
        result = dict(defaults)
        for key, val in v.items():
            k = str(key)
            months = int(val)
            if months < 1:
                raise ValueError(f"history_months.{k} doit être >= 1 (reçu: {months})")
            result[k] = months
        return result

    @field_validator("requests_per_minute")
    @classmethod
    def _rpm_non_neg(cls, v: int) -> int:
        if v < 0:
            raise ValueError("requests_per_minute doit être >= 0")
        return v

    @field_validator("max_retries")
    @classmethod
    def _retries_ge_1(cls, v: int) -> int:
        if v < 1:
            raise ValueError("max_retries doit être >= 1")
        return v

    @field_validator("page_limit")
    @classmethod
    def _page_limit_range(cls, v: int) -> int:
        if not 1 <= v <= 50000:
            raise ValueError("page_limit doit être entre 1 et 50000")
        return v

    @field_validator("contracts_page_limit", "tickers_page_limit")
    @classmethod
    def _page_limit_1_1000(cls, v: int) -> int:
        if not 1 <= v <= 1000:
            raise ValueError("contracts_page_limit / tickers_page_limit doit être entre 1 et 1000")
        return v

    @field_validator("splits_page_limit", "dividends_page_limit")
    @classmethod
    def _corp_actions_page_limit_range(cls, v: int) -> int:
        if not 1 <= v <= 5000:
            raise ValueError("splits/dividends_page_limit doit être entre 1 et 5000")
        return v

    @field_validator("data_quality_trigger")
    @classmethod
    def _trigger_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("data_quality_trigger doit être > 0")
        return v

    @field_validator("instrument_cache_ttl_days")
    @classmethod
    def _ttl_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("instrument_cache_ttl_days doit être >= 1")
        return v

    @field_validator("display_max_rows", "display_max_columns")
    @classmethod
    def _display_limits_ge_1(cls, v: int) -> int:
        if v < 1:
            raise ValueError("display_max_rows et display_max_columns doivent être >= 1")
        return v

    @field_validator("default_timescale_unit")
    @classmethod
    def _timescale_unit_valid(cls, v: str) -> str:
        if v not in ("min", "hour", "day", "week"):
            raise ValueError(
                f"default_timescale_unit doit être 'min'|'hour'|'day'|'week' (reçu: {v})."
            )
        return v

    @field_validator(
        "default_timescale_nb",
        "max_visible_candles",
        "buffer_multiplier",
        "fetch_chunk_size",
        "default_nb_candle",
        "thumbnail_lookback_days",
        "portfolio_trading_days_per_year",
        "portfolio_frontier_samples",
        "portfolio_default_lookback_years",
    )
    @classmethod
    def _chart_positive_int(cls, v: int) -> int:
        if v < 1:
            raise ValueError("les paramètres chart/portfolio doivent être >= 1")
        return v

    @field_validator("portfolio_min_coverage")
    @classmethod
    def _portfolio_coverage(cls, v: float) -> float:
        if not 0.0 < v <= 1.0:
            raise ValueError("portfolio_min_coverage doit être dans (0, 1]")
        return v

    @field_validator("portfolio_risk_free_rate")
    @classmethod
    def _portfolio_rf(cls, v: float) -> float:
        if v < -0.5 or v > 1.0:
            raise ValueError("portfolio_risk_free_rate hors plage raisonnable [-0.5, 1]")
        return v

    @field_validator("portfolio_rf_source")
    @classmethod
    def _portfolio_rf_source(cls, v: str) -> str:
        allowed = {"static", "yahoo"}
        s = (v or "static").strip().lower()
        if s not in allowed:
            raise ValueError(f"portfolio_rf_source doit être l'un de {sorted(allowed)}")
        return s

    @field_validator("portfolio_rf_cache_ttl_days")
    @classmethod
    def _portfolio_rf_ttl(cls, v: int) -> int:
        if v < 0:
            raise ValueError("portfolio_rf_cache_ttl_days doit être >= 0")
        return v

    @field_validator("portfolio_default_value")
    @classmethod
    def _portfolio_value(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("portfolio_default_value doit être > 0")
        return v

    @model_validator(mode="after")
    def _check_at_least_one_instrument(self) -> Settings:
        """Vérifie qu'au moins un instrument est configuré (tous types confondus)."""
        all_lists = (self.futures, self.forex, self.stocks, self.indices, self.options)
        if all(len(lst) == 0 for lst in all_lists):
            raise ValueError(
                "Aucun instrument configuré. Déclarez au moins un symbole dans "
                "[instruments] (futures, forex, stocks, indices ou options)."
            )
        return self

    @model_validator(mode="after")
    def _check_api_key(self) -> Settings:
        """Avertit si la clé API est absente — elle est obligatoire pour les appels API."""
        if not self.api_key:
            # On ne lève pas d'erreur ici car certaines commandes (config, setup-key)
            # n'ont pas besoin de clé. L'erreur sera levée au moment de l'appel API.
            pass
        return self

    # --- Helpers de chemins ---

    def raw_dumps_dir(self) -> Path:
        """Chemin complet du répertoire des dumps bruts."""
        return Path(self.data_dir).expanduser() / self.raw_dumps_subdir

    def aggregate_dir(self) -> Path:
        """Chemin complet du répertoire du cache agrégé."""
        return Path(self.data_dir).expanduser() / self.aggregate_subdir

    def aggregate_path(
        self,
        instrument: Instrument,
        resolution: str = DEFAULT_RESOLUTION,
    ) -> Path:
        """Chemin du fichier Parquet agrégé pour un instrument × résolution.

        Layout : ``data/aggregate/{type}/{symbol}/{resolution}.parquet``

        Ex: ``aggregate/stocks/AAPL/1min.parquet``, ``aggregate/stocks/AAPL/1day.parquet``.
        La logique de continu/rollover se fait à la query, pas au stockage.
        """
        return (
            self.aggregate_dir()
            / instrument.path_segment
            / instrument.symbol
            / f"{resolution}.parquet"
        )

    def raw_dump_path(
        self,
        instrument: Instrument,
        ticker: str,
        run_ts: str,
        resolution: str = DEFAULT_RESOLUTION,
    ) -> Path:
        """Chemin complet d'un dump brut.

        Layout : ``data/raw/{type}/{symbol}/{ticker}/{resolution}/{run_ts}.parquet``

        Pour futures, ``ticker`` = le contrat individuel (ex: ``ESM5``).
        Pour les autres types, ``ticker`` = le symbole (pas de sous-niveau
        contrat) — on passe ``ticker=instrument.symbol``.
        """
        return (
            self.raw_dumps_dir()
            / instrument.path_segment
            / instrument.symbol
            / ticker
            / resolution
            / f"{run_ts}.parquet"
        )

    def raw_ticker_dir(self, instrument: Instrument, ticker: str) -> Path:
        """Répertoire parent des résolutions pour un ticker.

        Layout : ``data/raw/{type}/{symbol}/{ticker}/``
        """
        return (
            self.raw_dumps_dir()
            / instrument.path_segment
            / instrument.symbol
            / ticker
        )

    def raw_resolution_dir(
        self,
        instrument: Instrument,
        ticker: str,
        resolution: str = DEFAULT_RESOLUTION,
    ) -> Path:
        """Répertoire des dumps d'un ticker × résolution."""
        return self.raw_ticker_dir(instrument, ticker) / resolution

    def legacy_aggregate_path(self, instrument: Instrument) -> Path:
        """Ancien layout (pré multi-résolution) : ``aggregate/{type}/{symbol}.parquet``."""
        return self.aggregate_dir() / instrument.path_segment / f"{instrument.symbol}.parquet"

    def yahoo_actions_path(self, ticker: str, kind: str) -> Path:
        """Cache corporate actions Yahoo : ``cache/yahoo_actions/{ticker}/{kind}.parquet``."""
        return (
            Path(self.cache_dir).expanduser()
            / self.yahoo_actions_cache_subdir
            / ticker
            / f"{kind}.parquet"
        )

    def yahoo_actions_meta_path(self, ticker: str, kind: str) -> Path:
        """Sidecar meta du cache yahoo_actions."""
        return self.yahoo_actions_path(ticker, kind).with_suffix(".meta.json")

    # --- Caches ---

    def contracts_cache_dir(self) -> Path:
        """Chemin du répertoire du cache contrats futures."""
        return Path(self.cache_dir).expanduser() / self.contracts_cache_subdir

    def contracts_cache_path(self, product_code: str) -> Path:
        """Chemin du fichier Parquet cache contrats pour un produit futures."""
        return self.contracts_cache_dir() / f"{product_code}.parquet"

    def contracts_meta_path(self, product_code: str) -> Path:
        """Chemin du sidecar .meta.json du cache contrats pour un produit futures."""
        return self.contracts_cache_dir() / f"{product_code}.meta.json"

    def corporate_actions_dir(self) -> Path:
        """Chemin du répertoire racine du cache corporate actions (stocks)."""
        return Path(self.cache_dir).expanduser() / self.corporate_actions_cache_subdir

    def corporate_actions_path(self, ticker: str, kind: str) -> Path:
        """Chemin du fichier Parquet corporate actions pour un ticker.

        :param ticker: Symbole nu du stock (ex: ``"AAPL"``).
        :param kind: Type d'action : ``"splits"`` ou ``"dividends"``.
        :return: ``cache/corporate_actions/{ticker}/{kind}.parquet``
        """
        return self.corporate_actions_dir() / ticker / f"{kind}.parquet"

    def corporate_actions_meta_path(self, ticker: str, kind: str) -> Path:
        """Chemin du sidecar .meta.json du cache corporate actions."""
        return self.corporate_actions_dir() / ticker / f"{kind}.meta.json"

    def tickers_cache_dir(self) -> Path:
        """Répertoire du cache référentiel tickers."""
        return Path(self.cache_dir).expanduser() / self.tickers_cache_subdir

    def tickers_all_path(self) -> Path:
        """Ancien path monolithe (legacy) : ``cache/tickers/all.parquet``."""
        return self.tickers_cache_dir() / "all.parquet"

    def tickers_all_meta_path(self) -> Path:
        return self.tickers_cache_dir() / "all.meta.json"

    def tickers_shard_path(self, market: str, active_bucket: str) -> Path:
        """Shard ``cache/tickers/{market}/{active|inactive}.parquet``."""
        return self.tickers_cache_dir() / market.lower() / f"{active_bucket}.parquet"

    def tickers_types_path(self) -> Path:
        """Parquet types de tickers : ``cache/tickers/types.parquet``."""
        return self.tickers_cache_dir() / "types.parquet"

    def tickers_types_meta_path(self) -> Path:
        return self.tickers_cache_dir() / "types.meta.json"

    # --- Helpers d'instruments ---

    def instruments_of_type(self, t: InstrumentType) -> list[Instrument]:
        """Retourne la liste des instruments configurés d'un type donné."""
        symbols = self._symbols_for_type(t)
        return [Instrument(type=t, symbol=s) for s in symbols]

    def all_instruments(self) -> list[Instrument]:
        """Retourne tous les instruments configurés (tous types confondus)."""
        result: list[Instrument] = []
        for t in InstrumentType:
            result.extend(self.instruments_of_type(t))
        return result

    def resolve_instrument(self, symbol: str, t: InstrumentType | None = None) -> Instrument:
        """Résout un symbole en :class:`Instrument` depuis la config.

        :param symbol: Symbole nu (ex: ``"ES"``, ``"AAPL"``).
        :param t: Type imposé. Si None, cherche le symbole parmi tous les types
            configurés et lève une erreur si le symbole est absent ou ambigu
            (présent dans plusieurs types).
        :return: L'instrument résolu.
        :raises ValueError: Si le symbole n'est pas configuré, ou est ambigu sans
            type imposé.
        """
        if t is not None:
            if symbol in self._symbols_for_type(t):
                return Instrument(type=t, symbol=symbol)
            raise ValueError(
                f"Symbole '{symbol}' non trouvé dans les instruments de type '{t.value}'."
            )

        found: list[Instrument] = []
        for tt in InstrumentType:
            if symbol in self._symbols_for_type(tt):
                found.append(Instrument(type=tt, symbol=symbol))
        if not found:
            raise ValueError(
                f"Symbole '{symbol}' non trouvé dans les instruments configurés. "
                f"Instruments: {[str(i) for i in self.all_instruments()]}"
            )
        if len(found) > 1:
            raise ValueError(
                f"Symbole '{symbol}' ambigu — présent dans plusieurs types: "
                f"{[i.type.value for i in found]}. Précisez --type."
            )
        return found[0]

    def _symbols_for_type(self, t: InstrumentType) -> list[str]:
        """Retourne la liste des symboles configurés pour un type."""
        return {
            InstrumentType.FUTURES: self.futures,
            InstrumentType.FOREX: self.forex,
            InstrumentType.STOCKS: self.stocks,
            InstrumentType.INDICES: self.indices,
            InstrumentType.OPTIONS: self.options,
        }[t]

    def stale_lag_days_for(self, resolution: str) -> int:
        """Seuil de lag calendaire (jours) au-delà duquel un agrégé est STALE."""
        if resolution == "1day":
            return self.health_stale_lag_days_1day
        return self.health_stale_lag_days_1min

    def history_months_for(self, instrument_type: InstrumentType | str) -> int:
        """Retourne l'historique ciblé (mois) pour un type d'instrument.

        Défauts : 24 mois pour tous les types sauf ``indices`` (60 mois).
        """
        key = (
            instrument_type.value
            if isinstance(instrument_type, InstrumentType)
            else str(instrument_type)
        )
        if key in self.history_months:
            return self.history_months[key]
        return 60 if key == InstrumentType.INDICES.value else 24


def resolve_env_path() -> Path | None:
    """Résout le chemin du ``.env`` à charger (XDG puis fallback repo).

    :return: Chemin du premier ``.env`` existant, ou ``None`` si aucun.
    """
    user_env = get_user_env_path()
    if user_env.exists():
        return user_env
    repo_env = get_repo_env_path()
    if repo_env.exists():
        return repo_env
    return None


def _expand_path_str(value: str) -> str:
    """Expand ``~`` dans un chemin stocké en string (pour data_dir/cache_dir/log_dir).

    N'altère pas les chemins relatifs (``./data`` reste ``./data``).
    """
    if value.startswith("~"):
        return str(Path(value).expanduser())
    return value


def _merge_history_months(
    raw: Any,
    defaults: dict[str, int],
) -> dict[str, int]:
    """Fusionne ``[fetch.history_months]`` (ou int legacy) avec les défauts Settings."""
    if raw is None:
        return dict(defaults)
    if isinstance(raw, int):
        return {t.value: raw for t in InstrumentType}
    if isinstance(raw, dict):
        merged = dict(defaults)
        merged.update({str(k): int(v) for k, v in raw.items()})
        return merged
    return dict(defaults)


def load_settings(config_path: str | Path | None = None) -> Settings:
    """Charge la configuration depuis ``.env`` + ``config.toml``.

    :param config_path: Chemin du fichier ``config.toml``.
        Par défaut : ``~/.config/myquantstore/config.toml`` (config utilisateur).
        Fallback : ``config.toml`` dans le répertoire courant (dev/repo).
    :return: Instance :class:`Settings` complète.
    :raises FileNotFoundError: Si aucun ``config.toml`` n'est trouvé.
    """
    # 1. Charger les secrets depuis .env (XDG prioritaire, sinon ./.env)
    env_path = resolve_env_path()
    if env_path is not None:
        settings = Settings(_env_file=str(env_path))
    else:
        settings = Settings()

    # 2. Charger config.toml (XDG prioritaire, sinon repo)
    config_path = resolve_config_path(config_path)

    with open(config_path, "rb") as f:
        toml_data: dict[str, Any] = tomllib.load(f)

    # 3. Hydrater settings avec les valeurs de config.toml
    # Le mapping est explicite pour éviter les conflits de noms.
    instruments = toml_data.get("instruments", {})
    futures_cfg = toml_data.get("futures", {})
    stocks_cfg = toml_data.get("stocks", {})
    tickers_cfg = toml_data.get("tickers", {})
    instrument_cache = toml_data.get("instrument_cache", {})
    fetch = toml_data.get("fetch", {})
    storage = toml_data.get("storage", {})
    tests = toml_data.get("tests", {})
    logging_section = toml_data.get("logging", {})
    display = toml_data.get("display", {})
    chart = toml_data.get("chart", {})
    chart_overlay = chart.get("overlay") if isinstance(chart.get("overlay"), dict) else {}
    chart_overlay_bt = (
        chart_overlay.get("backtest")
        if isinstance(chart_overlay.get("backtest"), dict)
        else {}
    )
    serve_cfg = toml_data.get("serve", {})
    portfolio_cfg = toml_data.get("portfolio", {})
    yahoo_cfg = toml_data.get("yahoo", {})
    health_cfg = toml_data.get("health", {})

    # On utilise model_dump + update + reconstruct pour rester typé et validé
    data = settings.model_dump()
    data.update(
        {
            # [instruments] — listes par type
            "futures": instruments.get("futures", data["futures"]),
            "forex": instruments.get("forex", data["forex"]),
            "stocks": instruments.get("stocks", data["stocks"]),
            "indices": instruments.get("indices", data["indices"]),
            "options": instruments.get("options", data["options"]),
            # [futures] — spécifique futures
            "days_before_expiry": futures_cfg.get("days_before_expiry", data["days_before_expiry"]),
            "contracts_page_limit": futures_cfg.get("contracts_page_limit", data["contracts_page_limit"]),
            "contracts_snapshot_interval_months": futures_cfg.get(
                "contracts_snapshot_interval_months", data["contracts_snapshot_interval_months"]
            ),
            # [stocks] — spécifique stocks
            "splits_page_limit": stocks_cfg.get("splits_page_limit", data["splits_page_limit"]),
            "dividends_page_limit": stocks_cfg.get("dividends_page_limit", data["dividends_page_limit"]),
            # [tickers] — référentiel
            "tickers_page_limit": tickers_cfg.get("page_limit", data["tickers_page_limit"]),
            # [instrument_cache] — TTL commun
            "instrument_cache_ttl_days": instrument_cache.get("ttl_days", data["instrument_cache_ttl_days"]),
            # [fetch] — générique
            "timeframe": fetch.get("timeframe", data["timeframe"]),
            "overlap_buffer_days": fetch.get("overlap_buffer_days", data["overlap_buffer_days"]),
            "history_months": _merge_history_months(
                fetch.get("history_months"), data["history_months"]
            ),
            "requests_per_minute": fetch.get("requests_per_minute", data["requests_per_minute"]),
            "page_limit": fetch.get("page_limit", data["page_limit"]),
            "max_retries": fetch.get("max_retries", data["max_retries"]),
            # [storage] — expand ~ pour chemins portables
            "data_dir": _expand_path_str(storage.get("data_dir", data["data_dir"])),
            "raw_dumps_subdir": storage.get("raw_dumps_subdir", data["raw_dumps_subdir"]),
            "aggregate_subdir": storage.get("aggregate_subdir", data["aggregate_subdir"]),
            "cache_dir": _expand_path_str(storage.get("cache_dir", data["cache_dir"])),
            "log_dir": _expand_path_str(storage.get("log_dir", data["log_dir"])),
            # [tests]
            "data_quality_trigger": tests.get("data_quality_trigger", data["data_quality_trigger"]),
            # [logging]
            "log_level": logging_section.get("level", data["log_level"]),
            # [display]
            "display_max_rows": display.get("max_rows", data["display_max_rows"]),
            "display_max_columns": display.get("max_columns", data["display_max_columns"]),
            # [chart]
            "default_timescale_unit": chart.get("default_timescale_unit", data["default_timescale_unit"]),
            "default_timescale_nb": chart.get("default_timescale_nb", data["default_timescale_nb"]),
            "default_nb_candle": chart.get("default_nb_candle", data["default_nb_candle"]),
            "max_visible_candles": chart.get("max_visible_candles", data["max_visible_candles"]),
            "buffer_multiplier": chart.get("buffer_multiplier", data["buffer_multiplier"]),
            "fetch_chunk_size": chart.get("fetch_chunk_size", data["fetch_chunk_size"]),
            "chart_port": chart.get("port", data["chart_port"]),
            "chart_host": chart.get("host", data["chart_host"]),
            "chart_mdns": chart.get("mdns", data["chart_mdns"]),
            "thumbnail_lookback_days": chart.get(
                "thumbnail_lookback_days", data["thumbnail_lookback_days"]
            ),
            "chart_candle_up": chart.get("candle_up", data["chart_candle_up"]),
            "chart_candle_down": chart.get("candle_down", data["chart_candle_down"]),
            # [chart.overlay] (+ rétrocompat [chart].overlay_dir)
            "overlay_dir": _expand_path_str(
                chart_overlay.get("overlay_dir")
                or chart.get("overlay_dir")
                or data["overlay_dir"]
            ),
            "chart_tx_buy": chart_overlay_bt.get("transaction_buy", data["chart_tx_buy"]),
            "chart_tx_sell": chart_overlay_bt.get(
                "transaction_sell", data["chart_tx_sell"]
            ),
            "chart_order_buy": chart_overlay_bt.get("order_buy", data["chart_order_buy"]),
            "chart_order_sell": chart_overlay_bt.get(
                "order_sell", data["chart_order_sell"]
            ),
            # [serve]
            "serve_port": serve_cfg.get("port", data["serve_port"]),
            "serve_host": serve_cfg.get("host", data["serve_host"]),
            # [portfolio]
            "portfolio_risk_free_rate": portfolio_cfg.get(
                "risk_free_rate", data["portfolio_risk_free_rate"]
            ),
            "portfolio_rf_source": portfolio_cfg.get(
                "rf_source", data["portfolio_rf_source"]
            ),
            "portfolio_rf_yahoo_ticker": portfolio_cfg.get(
                "rf_yahoo_ticker", data["portfolio_rf_yahoo_ticker"]
            ),
            "portfolio_rf_cache_ttl_days": portfolio_cfg.get(
                "rf_cache_ttl_days", data["portfolio_rf_cache_ttl_days"]
            ),
            "portfolio_trading_days_per_year": portfolio_cfg.get(
                "trading_days_per_year", data["portfolio_trading_days_per_year"]
            ),
            "portfolio_min_coverage": portfolio_cfg.get(
                "min_coverage", data["portfolio_min_coverage"]
            ),
            "portfolio_frontier_samples": portfolio_cfg.get(
                "frontier_samples", data["portfolio_frontier_samples"]
            ),
            "portfolio_default_lookback_years": portfolio_cfg.get(
                "default_lookback_years", data["portfolio_default_lookback_years"]
            ),
            "portfolio_optim_seed": portfolio_cfg.get(
                "optim_seed", data["portfolio_optim_seed"]
            ),
            "portfolio_default_value": portfolio_cfg.get(
                "default_value", data["portfolio_default_value"]
            ),
            # [yahoo]
            "yahoo_requests_per_minute": yahoo_cfg.get(
                "requests_per_minute", data["yahoo_requests_per_minute"]
            ),
            "yahoo_overlap_buffer_days": yahoo_cfg.get(
                "overlap_buffer_days", data["yahoo_overlap_buffer_days"]
            ),
            "yahoo_ticker_overrides": {
                str(k): str(v)
                for k, v in dict(
                    yahoo_cfg.get("ticker_overrides", data["yahoo_ticker_overrides"]) or {}
                ).items()
            },
            "yahoo_actions_cache_subdir": storage.get(
                "yahoo_actions_cache_subdir", data["yahoo_actions_cache_subdir"]
            ),
            # [health] — fraîcheur OHLCV
            "health_stale_lag_days_1min": health_cfg.get(
                "stale_lag_days_1min", data["health_stale_lag_days_1min"]
            ),
            "health_stale_lag_days_1day": health_cfg.get(
                "stale_lag_days_1day", data["health_stale_lag_days_1day"]
            ),
            "health_cross_resolution_lag_days": health_cfg.get(
                "cross_resolution_lag_days", data["health_cross_resolution_lag_days"]
            ),
        }
    )

    # Reconstruire avec validation complète (re-run des field_validators)
    return Settings(**data)


def generate_run_ts() -> str:
    """Génère un identifiant d'exécution au format ``YYYYMMDDTHHMMSS``.

    Exemple : ``20260711T183000`` pour le 11 juillet 2026 à 18:30:00 UTC.
    Ce format garantit l'unicité d'un run et permet de détecter si une historisation
    a déjà été faite aujourd'hui (en comparant les 8 premiers caractères = date).
    """
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
