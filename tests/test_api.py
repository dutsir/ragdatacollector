"""Тесты API (FastAPI TestClient)."""
import pytest
from fastapi.testclient import TestClient

from src.app import app


@pytest.fixture
def client():
    return TestClient(app)


def test_health(client: TestClient):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_list_sources(client: TestClient):
    r = client.get("/api/v1/sources")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert "cyberleninka" in data


def test_create_collect_task(client: TestClient):
    r = client.post(
        "/api/v1/collect",
        json={
            "task_type": "science",
            "keywords": ["диссертация климат"],
            "languages": ["ru", "en"],
            "sources": ["cyberleninka"],
            "max_results": 5,
        },
    )
    assert r.status_code == 200
    data = r.json()
    assert "task_id" in data
    assert data["status"] == "pending"


def test_get_task_not_found(client: TestClient):
    r = client.get("/api/v1/tasks/00000000-0000-0000-0000-000000000000")
    assert r.status_code == 404


def test_suggest_improvements(client: TestClient):
    r = client.post(
        "/api/v1/query/suggest",
        json={"keywords": ["ab"], "languages": []},
    )
    assert r.status_code == 200
    data = r.json()
    assert "keywords" in data or "filters" in data or "sources" in data
