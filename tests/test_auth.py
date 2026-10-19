import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from pool_api.auth import verify_api_key
from pool_api.security_api import get_token_bearerAuth
from settings import settings


@pytest.fixture(autouse=True)
def api_key(monkeypatch):
    monkeypatch.setattr(settings, "api_key", "secret")


def test_verify_api_key_accepts_the_configured_key():
    assert verify_api_key("secret") is None


def test_verify_api_key_rejects_other_keys():
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc:
        verify_api_key("nope")
    assert exc.value.status_code == 401
    assert exc.value.headers == {"WWW-Authenticate": "Bearer"}


@pytest.fixture
def client():
    app = FastAPI()

    @app.get("/protected")
    def protected(token=Depends(get_token_bearerAuth)):
        return {"sub": token.sub}

    return TestClient(app)


def test_bearer_dependency_accepts_valid_token(client):
    r = client.get("/protected", headers={"Authorization": "Bearer secret"})
    assert r.status_code == 200
    assert r.json() == {"sub": "api-key"}


def test_bearer_dependency_rejects_wrong_token(client):
    assert client.get("/protected", headers={"Authorization": "Bearer wrong"}).status_code == 401


def test_bearer_dependency_requires_a_header(client):
    assert client.get("/protected").status_code in (401, 403)
