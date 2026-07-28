"""
Tests unitaires du générateur GPS.
Vérifie la distance de Haversine, le rapprochement vers la destination,
la détection d'arrivée et le calage exact sur la cible.

Ces tests tournent sans Kafka ni Redis : logique pure.
Lancement : pytest tests/simulator/test_gps_generator.py -v
"""
import math

from simulator.models import Driver
from simulator.generators.gps_generator import (
    haversine_km,
    next_position,
    has_arrived,
    _step_towards,
)


def test_haversine_distance_nulle():
    """La distance d'un point à lui-même est nulle."""
    assert haversine_km(45.5, -73.6, 45.5, -73.6) == 0.0


def test_haversine_distance_connue():
    """
    Vérifie Haversine sur une distance connue.
    1 degré de latitude ≈ 111 km. On tolère 1 km d'écart.
    """
    d = haversine_km(45.0, -73.6, 46.0, -73.6)
    assert abs(d - 111.0) < 1.0


def test_haversine_symetrique():
    """La distance A->B égale la distance B->A."""
    d1 = haversine_km(45.50, -73.60, 45.55, -73.65)
    d2 = haversine_km(45.55, -73.65, 45.50, -73.60)
    assert abs(d1 - d2) < 1e-9


def test_next_position_rapproche_de_la_cible():
    """Après un pas, le véhicule est plus proche de la destination qu'avant."""
    driver = Driver.create(lat=45.50, lon=-73.60)
    dest_lat, dest_lon = 45.55, -73.65

    dist_avant = haversine_km(driver.lat, driver.lon, dest_lat, dest_lon)
    next_position(driver, dest_lat, dest_lon)
    dist_apres = haversine_km(driver.lat, driver.lon, dest_lat, dest_lon)

    assert dist_apres < dist_avant


def test_next_position_met_a_jour_le_driver():
    """next_position modifie bien la position du driver en place."""
    driver = Driver.create(lat=45.50, lon=-73.60)
    lat_initiale, lon_initiale = driver.lat, driver.lon

    next_position(driver, 45.55, -73.65)

    # Au moins une des deux coordonnées a changé.
    assert (driver.lat, driver.lon) != (lat_initiale, lon_initiale)


def test_next_position_retourne_event_coherent():
    """L'événement GPS retourné porte le bon véhicule et une vitesse positive."""
    driver = Driver.create(lat=45.50, lon=-73.60)
    event = next_position(driver, 45.55, -73.65)

    assert event.vehicle_id == driver.vehicle_id
    assert event.driver_id == driver.driver_id
    assert event.speed_kmh >= 0.0
    assert event.timestamp  # non vide


def test_step_towards_ne_depasse_pas_la_cible():
    """Si le pas dépasse la distance restante, on se cale exactement sur la cible."""
    # Distance énorme demandée (1000 km) vers une cible toute proche.
    new_lat, new_lon = _step_towards(45.50, -73.60, 45.5001, -73.6001, 1000.0)
    assert new_lat == 45.5001
    assert new_lon == -73.6001


def test_has_arrived_vrai_si_sur_place():
    """has_arrived est vrai quand le véhicule est sur la destination."""
    driver = Driver.create(lat=45.50, lon=-73.60)
    assert has_arrived(driver, 45.50, -73.60) is True


def test_has_arrived_faux_si_loin():
    """has_arrived est faux quand le véhicule est loin de la destination."""
    driver = Driver.create(lat=45.50, lon=-73.60)
    assert has_arrived(driver, 45.70, -73.90) is False