"""Smoke tests ensuring the API app imports and serves expected health metadata."""

from fastapi.testclient import TestClient

from llm_tradebot.services.api.main import app


def test_health_endpoint() -> None:
    """Health endpoint should report an OK status."""

    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
