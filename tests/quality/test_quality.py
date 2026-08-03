"""
Tests unitaires de la couche qualité (Great Expectations).

Vérifie DEUX choses essentielles :
  1. Des données valides passent la validation (test positif).
  2. Des données INVALIDES échouent bien (test négatif) — c'est ce qui
     prouve que les expectations mordent réellement.

Lancement : pytest tests/quality/test_quality.py -v
"""
import pandas as pd
import great_expectations as gx

from quality.runner import validate_gps, validate_delivery, validate_driver


def _context():
    return gx.get_context(mode="ephemeral")


# ------------------------------------------------------------
# Tests POSITIFS : données valides -> succès
# ------------------------------------------------------------

def test_gps_valide_passe():
    """Des positions GPS correctes passent toutes les attentes."""
    df = pd.DataFrame({
        "vehicle_id": ["veh-1", "veh-2"],
        "driver_id": ["drv-1", "drv-2"],
        "lat": [45.5, 45.6],
        "lon": [-73.6, -73.7],
        "speed_kmh": [30.0, 25.0],
    })
    summary = validate_gps(_context(), df)
    assert summary["success"] is True
    assert summary["n_success"] == summary["n_expectations"]


# ------------------------------------------------------------
# Tests NÉGATIFS : données invalides -> échec attendu
# ------------------------------------------------------------

def test_gps_coordonnee_hors_bornes_echoue():
    """Une latitude aberrante (Paris) fait échouer la validation."""
    df = pd.DataFrame({
        "vehicle_id": ["veh-1"],
        "driver_id": ["drv-1"],
        "lat": [48.85],       # Paris : hors bornes Montréal
        "lon": [2.35],
        "speed_kmh": [30.0],
    })
    summary = validate_gps(_context(), df)
    assert summary["success"] is False


def test_gps_vehicle_id_null_echoue():
    """Un vehicle_id nul fait échouer la validation."""
    df = pd.DataFrame({
        "vehicle_id": [None],
        "driver_id": ["drv-1"],
        "lat": [45.5],
        "lon": [-73.6],
        "speed_kmh": [30.0],
    })
    summary = validate_gps(_context(), df)
    assert summary["success"] is False


def test_delivery_statut_invalide_echoue():
    """Un statut de livraison hors de l'ensemble autorisé fait échouer."""
    df = pd.DataFrame({
        "package_id": ["pkg-1"],
        "order_id": ["ord-1"],
        "vehicle_id": ["veh-1"],
        "status": ["livré"],   # français, pas dans l'ensemble autorisé
    })
    summary = validate_delivery(_context(), df)
    assert summary["success"] is False


def test_driver_statut_invalide_echoue():
    """Un statut chauffeur inconnu fait échouer."""
    df = pd.DataFrame({
        "driver_id": ["drv-1"],
        "vehicle_id": ["veh-1"],
        "status": ["en_pause"],   # pas dans {offline, online, on_break, delivering}
    })
    summary = validate_driver(_context(), df)
    assert summary["success"] is False