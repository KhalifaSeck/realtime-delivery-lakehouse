"""
Tests de l'API FastAPI (endpoints vehicles + health).

Utilise le TestClient de FastAPI (appels en mémoire, sans uvicorn) et
peuple Redis avec des véhicules de test via une fixture.

Prérequis : conteneur Redis démarré (les tests lisent/écrivent Redis).

Lancement : pytest tests/api/test_api.py -v
"""
import pytest
from fastapi.testclient import TestClient

from api.main import app
from scripts.seed_redis import _client, seed_vehicles, clear_test_vehicles


@pytest.fixture(scope="module")
def seeded_redis():
    """
    Peuple Redis avec 3 véhicules de test avant les tests, nettoie après.
    scope='module' : un seul peuplement pour tout le fichier de test.
    """
    client = _client()
    if not client.ping():
        pytest.skip("Redis non disponible (démarre docker compose).")
    clear_test_vehicles(client)
    ids = seed_vehicles(client, n=3)
    yield ids
    clear_test_vehicles(client)


@pytest.fixture
def api_client():
    """Client de test FastAPI (appels en mémoire)."""
    return TestClient(app)


def test_health_ok(api_client, seeded_redis):
    """Le health check renvoie un statut ok quand Redis répond."""
    resp = api_client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["redis_connected"] is True
    assert data["vehicles_tracked"] >= 3  # au moins nos 3 véhicules de test


def test_list_vehicles(api_client, seeded_redis):
    """La liste des véhicules contient les véhicules de test."""
    resp = api_client.get("/vehicles")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] >= 3
    # Nos IDs de test sont présents.
    for vid in seeded_redis:
        assert vid in data["vehicle_ids"]


def test_get_vehicle_existant(api_client, seeded_redis):
    """Récupérer un véhicule existant renvoie son état complet."""
    vid = seeded_redis[0]
    resp = api_client.get(f"/vehicles/{vid}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["vehicle_id"] == vid
    assert isinstance(data["lat"], float)
    assert isinstance(data["lon"], float)
    assert data["driver_id"] is not None


def test_get_vehicle_inexistant(api_client, seeded_redis):
    """Récupérer un véhicule inexistant renvoie un 404."""
    resp = api_client.get("/vehicles/veh-nexiste-pas")
    assert resp.status_code == 404
    assert "introuvable" in resp.json()["detail"].lower()


def test_root(api_client):
    """La racine renvoie les infos du service."""
    resp = api_client.get("/")
    assert resp.status_code == 200
    assert "endpoints" in resp.json()