"""Logs JSON : une requête produit une ligne structurée, sauf /health (healthcheck Docker en boucle)."""

import json

from fastapi.testclient import TestClient

from pool_api.main import app


def _request_logs(out: str) -> list[dict]:
    return [p for p in map(json.loads, out.splitlines()) if p["message"] == "requête traitée"]


def test_request_is_logged_as_json(capsys):
    client = TestClient(app)  # sans `with` : pas de lifespan (pas de base)
    client.get("/unknown-route")

    (payload,) = _request_logs(capsys.readouterr().out)
    assert payload["service"] == "pool-api"
    assert payload["path"] == "/unknown-route"
    assert payload["status"] == 404


def test_health_is_not_logged(capsys):
    # le container n'est pas branché : /health échoue, ce qui n'importe pas ici (seul l'absence de log compte)
    client = TestClient(app, raise_server_exceptions=False)
    client.get("/health")

    assert _request_logs(capsys.readouterr().out) == []
