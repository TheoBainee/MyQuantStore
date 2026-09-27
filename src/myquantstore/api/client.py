"""Client HTTP pour l'API Massive.com.

Ce module implémente :class:`MassiveClient`, le client HTTP central qui gère :
- **Authentification** : header ``Authorization: Bearer <MASSIVE_API_KEY>``.
- **Throttle self-imposé** : délai minimum inter-requête calculé depuis
  ``requests_per_minute`` (ex: 10 req/min = 6s entre appels). Prévient les 429.
- **Retry via Tenacity** : sur 429 (avec header ``Retry-After`` prioritaire)
  et 5xx (exponential backoff), jusqu'à ``max_retries`` tentatives.
- **Pagination** : suit ``next_url`` automatiquement, logge un extrait de
  chaque page en DEBUG (avec ``window_start`` converti en datetime lisible).

Le client est synchrone (httpx.Client) — MyQuantStore n'a pas besoin d'async
pour de l'historisation périodique.
"""

from __future__ import annotations

import time
from typing import Any, cast

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from myquantstore.config import Settings
from myquantstore.logging_setup import get_logger, log_api_call, log_pagination_excerpt


class RateLimitError(Exception):
    """Levée quand le serveur renvoie 429 Too Many Requests et que tous les retries sont épuisés."""

    def __init__(self, url: str, retry_after: float | None = None):
        self.url = url
        self.retry_after = retry_after
        super().__init__(
            f"429 Too Many Requests sur {url}"
            + (f" (Retry-After={retry_after}s)" if retry_after else "")
        )


class ServerError(Exception):
    """Levée quand le serveur renvoie 5xx et que tous les retries sont épuisés."""

    def __init__(self, url: str, status_code: int, body: str = ""):
        self.url = url
        self.status_code = status_code
        self.body = body
        super().__init__(f"{status_code} Server Error sur {url}: {body[:200]}")


class ClientError(Exception):
    """Levée pour les erreurs 4xx non retryables (400, 403, 404…)."""

    def __init__(self, url: str, status_code: int, body: str = ""):
        self.url = url
        self.status_code = status_code
        self.body = body
        super().__init__(f"{status_code} Client Error sur {url}: {body[:200]}")


class _RetryableRateLimitError(Exception):
    """Exception interne levée pour déclencher un retry Tenacity sur 429.

    On utilise une exception intermédiaire (pas RateLimitError directement) car
    Tenacity a besoin de l'attraper pour retry, mais on ne veut pas que
    RateLimitError (l'erreur finale) soit retryable.
    """

    def __init__(self, retry_after: float | None):
        self.retry_after = retry_after
        super().__init__(f"429 — retry après {retry_after}s" if retry_after else "429 — retry (backoff)")


class _RetryableServerError(Exception):
    """Exception interne levée pour déclencher un retry Tenacity sur 5xx."""

    def __init__(self, status_code: int):
        self.status_code = status_code
        super().__init__(f"{status_code} — retry (backoff)")


class MassiveClient:
    """Client HTTP synchrone pour l'API Massive.com.

    Usage typique :

    .. code-block:: python

        with MassiveClient(settings) as client:
            data = client.get("/futures/v1/contracts", product_code="ES", limit=1000)
            # data = {"results": [...], "next_url": "...", "status": "OK"}
    """

    def __init__(self, settings: Settings, timeout: float = 30.0):
        self._settings = settings
        self._logger = get_logger("api")
        self._timeout = timeout

        # --- Throttle self-imposé ---
        # On garde le timestamp de la dernière requête pour imposer un délai minimum.
        # Si requests_per_minute = 6, délai = 60/6 = 10s entre chaque appel.
        self._min_interval: float = (
            60.0 / settings.requests_per_minute if settings.requests_per_minute > 0 else 0.0
        )
        self._last_request_time: float = 0.0

        # Client httpx — on le crée paresselement (lazy) pour faciliter les tests
        self._client: httpx.Client | None = None

        # Compteur de pages pour la pagination (incrémenté dans get_paginated)
        self._page_counter: int = 0

    # --- Gestion du cycle de vie ---

    def __enter__(self) -> MassiveClient:
        self._ensure_client()
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.close()

    def _ensure_client(self) -> httpx.Client:
        """Crée le client httpx si pas déjà fait (lazy init)."""
        if self._client is None:
            self._client = httpx.Client(
                base_url=self._settings.base_url,
                timeout=self._timeout,
                headers=self._auth_headers(),
            )
        return self._client

    def close(self) -> None:
        """Ferme le client httpx et libère les connexions."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def _auth_headers(self) -> dict[str, str]:
        """Construit les headers d'authentification (Bearer token)."""
        return {
            "Authorization": f"Bearer {self._settings.api_key}",
            "Accept": "application/json",
        }

    # --- Throttle ---

    def _throttle(self) -> None:
        """Impose un délai minimum inter-requête si requests_per_minute > 0.

        Calcule le temps écoulé depuis la dernière requête et attend
        le complément pour respecter le délai minimum.
        """
        if self._min_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_request_time
        remaining = self._min_interval - elapsed
        if remaining > 0:
            self._logger.debug(f"Throttle: attente de {remaining:.2f}s (rpm={self._settings.requests_per_minute})")
            time.sleep(remaining)
        self._last_request_time = time.monotonic()

    # --- Requête HTTP de base (avec retry Tenacity) ---

    def _raw_get(self, url: str, params: dict[str, Any] | None = None) -> httpx.Response:
        """Effectue une requête GET brute avec retry Tenacity.

        Cette méthode est décorée par Tenacity pour retry automatique sur
        429 (Retry-After prioritaire) et 5xx (exponential backoff).
        Les erreurs 4xx (sauf 429) lèvent immédiatement ClientError (pas de retry).

        :param url: URL complète ou path relatif (httpx gère base_url).
        :param params: Paramètres de query string.
        :raises RateLimitError: Si 429 persistant après max_retries.
        :raises ServerError: Si 5xx persistant après max_retries.
        :raises ClientError: Si 4xx (sauf 429) — non retryable.
        """
        client = self._ensure_client()
        self._throttle()

        # Log de l'appel en DEBUG (clé masquée par log_api_call)
        log_api_call(self._logger, "GET", url, **(params or {}))

        try:
            response = client.get(url, params=params)
        except httpx.RequestError as e:
            # Erreur réseau (timeout, connexion refusée…) — on retry
            self._logger.warning(f"Erreur réseau sur {url}: {e} — retry")
            raise _RetryableServerError(0) from e

        # 200 = succès
        if response.status_code == 200:
            return response

        # 429 Too Many Requests — on lit Retry-After si présent
        if response.status_code == 429:
            retry_after = self._parse_retry_after(response.headers.get("retry-after"))
            self._logger.warning(
                f"429 sur {url} — retry"
                + (f" (Retry-After={retry_after}s)" if retry_after else " (backoff exponentiel)")
            )
            # Si Retry-After présent, on attend cette durée avant le retry
            if retry_after is not None:
                time.sleep(retry_after)
            raise _RetryableRateLimitError(retry_after)

        # 5xx Server Error — retry avec backoff
        if 500 <= response.status_code < 600:
            self._logger.warning(f"{response.status_code} sur {url} — retry (backoff)")
            raise _RetryableServerError(response.status_code)

        # 4xx (sauf 429) — non retryable
        raise ClientError(url, response.status_code, response.text)

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        """Parse le header ``Retry-After`` (en secondes).

        L'API Massive utilise des secondes (pas de date HTTP).
        :return: Délai en secondes, ou None si absent/invalide.
        """
        if value is None:
            return None
        try:
            return float(value)
        except ValueError:
            return None

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        """Effectue une requête GET sur l'API et retourne le JSON parsé.

        Gère le retry (Tenacity) et le throttle. Contrairement à
        :meth:`get_paginated`, cette méthode ne suit pas ``next_url`` —
        elle retourne uniquement la première page.

        :param path: Path de l'endpoint (ex: "/futures/v1/contracts").
        :param params: Paramètres de query string.
        :return: Dictionnaire JSON de la réponse.
        """
        # On filtre les params None pour ne pas les envoyer
        clean_params = {k: v for k, v in params.items() if v is not None}

        # Décorateur Tenacity recréé à chaque appel pour utiliser max_retries dynamique
        retry_decorator = retry(
            stop=stop_after_attempt(self._settings.max_retries),
            wait=wait_exponential(multiplier=1, min=1, max=60),
            retry=retry_if_exception_type((_RetryableRateLimitError, _RetryableServerError)),
            reraise=True,
        )

        @retry_decorator
        def _do_get() -> dict[str, Any]:
            # On laisse _RetryableRateLimitError / _RetryableServerError remonter
            # pour que tenacity les intercepte et retry. On ne les attrape PAS ici.
            response = self._raw_get(path, params=clean_params if clean_params else None)
            return cast("dict[str, Any]", response.json())

        try:
            return _do_get()
        except _RetryableRateLimitError as e:
            # Tenacity a épuisé les retries -> on lève l'erreur finale
            raise RateLimitError(path) from e
        except _RetryableServerError as e:
            raise ServerError(path, e.status_code) from e

    def get_list(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """GET sur un endpoint qui renvoie un **tableau** JSON (sans enveloppe ``results``).

        Ex. ``/v1/marketstatus/upcoming``. Même retry / throttle que :meth:`get`,
        qui ne transforme pas le JSON (son ``cast`` est purement statique) : un
        tableau traverse donc tel quel et on vérifie ici sa forme.

        :raises ClientError: Si la réponse n'est pas un tableau JSON.
        """
        data: object = self.get(path, **params)
        if not isinstance(data, list):
            raise ClientError(path, 200, f"tableau JSON attendu, reçu {type(data).__name__}")
        return cast("list[dict[str, Any]]", data)

    def get_paginated(self, path: str, **params: Any) -> list[dict[str, Any]]:
        """Effectue une requête GET paginée et retourne tous les résultats concaténés.

        Suit automatiquement ``next_url`` page par page jusqu'à ce qu'il n'y
        ait plus de page suivante. À chaque page, log DEBUG d'un extrait des
        5 premières lignes avec ``window_start`` converti en datetime lisible
        (pour repérer une boucle infinie de pagination).

        :param path: Path de l'endpoint initial (ex: "/futures/v1/contracts").
        :param params: Paramètres de query string (pour la première page uniquement).
        :return: Liste de tous les résultats de toutes les pages.
        """
        self._page_counter = 0
        all_results: list[dict[str, Any]] = []

        # Première requête avec les params fournis
        clean_params = {k: v for k, v in params.items() if v is not None}
        data = self.get(path, **clean_params)

        results = data.get("results", [])
        self._page_counter += 1
        log_pagination_excerpt(self._logger, self._page_counter, results)
        all_results.extend(results)

        # Pagination : suivre next_url tant qu'il est présent
        next_url = data.get("next_url")
        while next_url:
            # next_url est une URL complète — on l'utilise directement (pas de base_url)
            # On passe les params via l'URL elle-même (next_url contient déjà le cursor)
            data = self.get(next_url)
            results = data.get("results", [])
            self._page_counter += 1
            log_pagination_excerpt(self._logger, self._page_counter, results)
            all_results.extend(results)
            next_url = data.get("next_url")

        self._logger.info(
            f"Pagination terminée: {self._page_counter} page(s), {len(all_results)} résultat(s) au total"
        )
        return all_results

    @property
    def page_count(self) -> int:
        """Nombre de pages fetchées lors du dernier appel get_paginated."""
        return self._page_counter
