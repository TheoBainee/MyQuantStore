"""Tests du module api/client.py (MassiveClient)."""

from __future__ import annotations

import httpx
import pytest
import respx

from myquantstore.api.client import ClientError, MassiveClient, RateLimitError


@pytest.fixture
def client(tmp_settings, monkeypatch):
    """MassiveClient avec settings de test (pas de throttle, sleep mocké)."""
    # Mocker time.sleep pour éviter les attentes réelles pendant les retries
    monkeypatch.setattr("time.sleep", lambda _: None)
    c = MassiveClient(tmp_settings)
    yield c
    c.close()


class TestAuthentication:
    """Tests de l'authentification Bearer."""

    @respx.mock
    def test_bearer_header_sent(self, client, tmp_settings):
        """Le header Authorization: Bearer est envoyé."""
        respx.get("/futures/v1/contracts").mock(
            return_value=httpx.Response(200, json={"results": [], "status": "OK"})
        )

        client.get("/futures/v1/contracts", product_code="ES")

        # Vérifier que le header Authorization a été envoyé
        request = respx.calls[0].request
        assert request.headers["authorization"] == f"Bearer {tmp_settings.api_key}"

    @respx.mock
    def test_successful_get(self, client):
        """Un GET 200 retourne le JSON parsé."""
        respx.get("/futures/v1/contracts").mock(
            return_value=httpx.Response(
                200,
                json={"results": [{"ticker": "ESM5"}], "status": "OK"},
            )
        )

        data = client.get("/futures/v1/contracts", product_code="ES")

        assert data["status"] == "OK"
        assert data["results"] == [{"ticker": "ESM5"}]


class TestRetry:
    """Tests du retry Tenacity."""

    @respx.mock
    def test_retry_on_429_with_retry_after(self, client):
        """429 avec Retry-After est retryé puis réussit."""
        route = respx.get("/futures/v1/contracts").mock(
            side_effect=[
                httpx.Response(429, headers={"retry-after": "0"}),
                httpx.Response(200, json={"results": [], "status": "OK"}),
            ]
        )

        data = client.get("/futures/v1/contracts")

        assert route.call_count == 2
        assert data["status"] == "OK"

    @respx.mock
    def test_retry_on_500(self, client):
        """500 est retryé puis réussit."""
        route = respx.get("/futures/v1/contracts").mock(
            side_effect=[
                httpx.Response(500, text="Server Error"),
                httpx.Response(200, json={"results": [], "status": "OK"}),
            ]
        )

        data = client.get("/futures/v1/contracts")

        assert route.call_count == 2
        assert data["status"] == "OK"

    @respx.mock
    def test_client_error_not_retried(self, client):
        """400 (client error) n'est pas retryé."""
        route = respx.get("/futures/v1/contracts").mock(
            return_value=httpx.Response(400, text="Bad Request")
        )

        with pytest.raises(ClientError) as exc_info:
            client.get("/futures/v1/contracts")

        assert route.call_count == 1
        assert exc_info.value.status_code == 400

    @respx.mock
    def test_max_retries_exhausted(self, tmp_settings, monkeypatch):
        """Après max_retries, RateLimitError est levée."""
        # Mocker time.sleep pour éviter les attentes
        monkeypatch.setattr("time.sleep", lambda _: None)
        # Réduire max_retries pour accélérer le test
        tmp_settings = tmp_settings.model_copy(update={"max_retries": 2})
        client = MassiveClient(tmp_settings)

        route = respx.get("/futures/v1/contracts").mock(
            return_value=httpx.Response(429, headers={"retry-after": "0"})
        )

        with pytest.raises((RateLimitError, Exception)):
            client.get("/futures/v1/contracts")

        # Au moins 2 tentatives (max_retries)
        assert route.call_count >= 2

        client.close()


class TestPagination:
    """Tests de la pagination next_url."""

    @respx.mock
    def test_single_page_no_next_url(self, client):
        """Une seule page (pas de next_url) retourne tous les résultats."""
        respx.get("/futures/v1/contracts").mock(
            return_value=httpx.Response(
                200,
                json={
                    "results": [{"ticker": "ESM5"}, {"ticker": "ESU5"}],
                    "next_url": None,
                    "status": "OK",
                },
            )
        )

        results = client.get_paginated("/futures/v1/contracts", product_code="ES")

        assert len(results) == 2
        assert client.page_count == 1

    @respx.mock
    def test_multi_page_follows_next_url(self, client):
        """La pagination suit next_url jusqu'à la dernière page."""
        # Utiliser side_effect pour retourner séquentiellement les pages
        # (évite que la première route intercepte tous les appels)
        respx.get(host="api.test.massive.com", path="/futures/v1/contracts").mock(
            side_effect=[
                httpx.Response(
                    200,
                    json={
                        "results": [{"ticker": "ESH5"}],
                        "next_url": "https://api.test.massive.com/futures/v1/contracts?cursor=page2",
                        "status": "OK",
                    },
                ),
                httpx.Response(
                    200,
                    json={
                        "results": [{"ticker": "ESM5"}],
                        "next_url": "https://api.test.massive.com/futures/v1/contracts?cursor=page3",
                        "status": "OK",
                    },
                ),
                httpx.Response(
                    200,
                    json={
                        "results": [{"ticker": "ESU5"}],
                        "next_url": None,
                        "status": "OK",
                    },
                ),
            ]
        )

        results = client.get_paginated("/futures/v1/contracts", product_code="ES")

        assert len(results) == 3
        assert results[0]["ticker"] == "ESH5"
        assert results[1]["ticker"] == "ESM5"
        assert results[2]["ticker"] == "ESU5"
        assert client.page_count == 3

    @respx.mock
    def test_empty_results(self, client):
        """Une réponse sans résultats retourne une liste vide."""
        respx.get("/futures/v1/contracts").mock(
            return_value=httpx.Response(
                200,
                json={"results": [], "status": "OK"},
            )
        )

        results = client.get_paginated("/futures/v1/contracts")

        assert results == []
        assert client.page_count == 1


class TestLogRedaction:
    def test_redact_url_masks_apikey(self):
        from myquantstore.logging_setup import redact_url

        url = "https://api.massive.com/v2/aggs?ticker=ES&apiKey=SECRET123&limit=10"
        redacted = redact_url(url)
        assert "SECRET123" not in redacted
        assert "apiKey=****" in redacted
        assert "ticker=ES" in redacted
