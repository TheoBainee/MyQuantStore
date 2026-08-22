"""Configuration du logging pour MyQuantStore.

Le logging utilise deux handlers :
- Console : ``rich.logging.RichHandler`` (couleurs, formatage lisible).
- Fichier : ``{log_dir}/myquantstore.log`` avec rotation (10 MB, 5 fichiers).

Un **seul levier** contrôle tout : le niveau de log (``level`` dans config.toml).
En mode ``DEBUG`` (défaut), tous les helpers (appels API, skips cache, extrait
pagination) se déclenchent via ``isEnabledFor(DEBUG)``.
"""

from __future__ import annotations

import logging
from datetime import UTC
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from rich.logging import RichHandler

# Format des logs : préfixe [module] pour distinguer les étapes du pipeline
_LOG_FORMAT = "%(message)s"


def setup_logging(level: str = "DEBUG", log_dir: str = "./logs") -> logging.Logger:
    """Configure le logger racine de MyQuantStore.

    :param level: Niveau de logging (DEBUG, INFO, WARNING, ERROR).
    :param log_dir: Répertoire des fichiers de log.
    :return: Le logger configuré.
    """
    numeric_level = getattr(logging, level.upper(), logging.DEBUG)

    # Logger racine myquantstore — on évite de configurer le logger global
    logger = logging.getLogger("myquantstore")
    logger.setLevel(numeric_level)
    logger.handlers.clear()  # Évite les doublons si appelé plusieurs fois

    # --- Handler console (rich) ---
    console_handler = RichHandler(
        show_time=True,
        show_level=True,
        show_path=False,
        markup=True,
        rich_tracebacks=True,
    )
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt="[%X]"))
    logger.addHandler(console_handler)

    # --- Handler fichier (rotation 10 MB, 5 fichiers) ---
    log_path = Path(log_dir).expanduser()
    log_path.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        log_path / "myquantstore.log",
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(file_handler)

    # Réduire le bruit de httpx/tenacity en DEBUG (trop verbeux)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("tenacity").setLevel(logging.INFO)

    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    """Retourne un logger enfant du logger racine myquantstore.

    :param name: Nom du logger (ex: "fetch", "contracts"). Sera préfixé par "myquantstore.".
    """
    if name is None:
        return logging.getLogger("myquantstore")
    return logging.getLogger(f"myquantstore.{name}")


# --- Helpers de log DEBUG (ne se déclenchent que si level >= DEBUG) ---


_SECRET_PARAM_MARKERS = ("key", "token")


def _is_secret_param(name: str) -> bool:
    lowered = name.lower()
    return any(marker in lowered for marker in _SECRET_PARAM_MARKERS)


def redact_url(path: str) -> str:
    """Masque apiKey / token dans une URL (ex: ``next_url`` Massive)."""
    if "?" not in path:
        return path
    parts = urlsplit(path)
    if not parts.query:
        return path
    redacted = [
        (k, "****" if _is_secret_param(k) else v)
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
    ]
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(redacted, safe="*"), parts.fragment)
    )


def log_api_call(logger: logging.Logger, method: str, path: str, **params: object) -> None:
    """Log un appel API en DEBUG (endpoint + params, clé masquée).

    :param logger: Logger à utiliser.
    :param method: Méthode HTTP (GET, POST…).
    :param path: Path de l'endpoint (ex: /futures/v1/contracts) ou URL complète.
    :param params: Paramètres de la requête.
    """
    if logger.isEnabledFor(logging.DEBUG):
        safe_params = {
            k: ("****" if _is_secret_param(k) else v)
            for k, v in params.items()
        }
        logger.debug(f"API {method} {redact_url(path)} params={safe_params}")


def log_cache_skip(logger: logging.Logger, cache_name: str, product_code: str, last_fetched: str) -> None:
    """Log un skip de cache en DEBUG (cache encore frais).

    :param logger: Logger à utiliser.
    :param cache_name: Nom du cache (ex: "contrats").
    :param product_code: Code produit (ex: "ES").
    :param last_fetched: Date du dernier fetch (ISO).
    """
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Cache skip: {cache_name} pour {product_code} (last_fetched={last_fetched})")


def log_pagination_excerpt(logger: logging.Logger, page_num: int, results: list[dict[str, Any]]) -> None:
    """Log un extrait de la réponse de pagination en DEBUG.

    Affiche les 5 premières lignes de ``results`` avec conversion du timestamp
    ``window_start`` (nanosecondes) en datetime lisible UTC. **La colonne
    ``window_start`` est systématiquement incluse** dans l'extrait pour repérer
    visuellement une boucle infinie de pagination.

    :param logger: Logger à utiliser.
    :param page_num: Numéro de la page (1-indexed).
    :param results: Liste des résultats de la page (liste de dicts).
    """
    if not logger.isEnabledFor(logging.DEBUG):
        return
    if not results:
        logger.debug(f"[page {page_num}] 0 résultats (page vide)")
        return

    # Copier les 5 premières lignes pour NE PAS modifier les résultats originaux
    # (les dicts sont mutables — results[:5] fait une shallow copy de la liste
    # mais partage les mêmes objets dicts).
    excerpt = []
    for row in results[:5]:
        row_copy = dict(row)
        if "window_start" in row_copy and isinstance(row_copy["window_start"], (int, float)):
            row_copy["window_start"] = _ns_to_iso(int(row_copy["window_start"]))
        excerpt.append(row_copy)

    logger.debug(f"[page {page_num}] 5 premières lignes: {excerpt}")


def _ns_to_iso(ns: int) -> str:
    """Convertit un timestamp nanosecondes en string ISO 8601 UTC lisible.

    :param ns: Timestamp en nanosecondes (depuis epoch).
    :return: String ISO 8601 (ex: "2026-07-11T18:30:00Z").
    """
    from datetime import datetime

    seconds = ns / 1_000_000_000
    return datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
